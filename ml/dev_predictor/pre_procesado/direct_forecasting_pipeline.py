"""
Pipeline de Direct Forecasting T+7
====================================
Encapsula el entrenamiento de 7 modelos XGBoost independientes (uno por horizonte)
y la evaluación walk-forward con integridad temporal garantizada.

Estrategia anti-leakage:
  Para el modelo de horizonte h, la última fecha usable en entrenamiento es
  max_train_date - h días. Esto evita que el target de un día en validación
  filtre información al modelo durante la fase de fit.
"""

import pandas as pd
import numpy as np
import xgboost as xgb
import warnings
from sklearn.metrics import (
    roc_auc_score, f1_score, accuracy_score,
    precision_score, recall_score, brier_score_loss,
)
from pre_procesado.non_linear_pipeline import build_tree_pipeline



# =============================================================================
# CONSTANTES INTERNAS
# Definir aquí evita el Training-Serving Skew
# =============================================================================

# Lista de backup por si se necesita
COLUMNAS_BASURA: list[str] = ['month', 
    'is_entire_home', 
    'occ_barrio_ma7',
    'day_of_week', 
    'is_weekend', 
    'is_holiday', 
    'has_reviews',
    'instant_bookable', 
    'precipitation_mm', 
    'num_sports', 
    'num_festivals', 
    'total_attendance', 
    'is_missing_review_scores_rating',
    'occ_barrio_ayer',
    'is_occupied_lag_3', 'is_occupied_lag_4', 'is_occupied_lag_5', 
    'is_occupied_lag_6', 'is_occupied_lag_7', 'is_occupied_lag_8', 
    'is_occupied_lag_9', 'is_occupied_lag_10', 'is_occupied_lag_11', 
    'is_occupied_lag_12', 'is_occupied_lag_13', 'is_occupied_lag_14']

TARGET_COL: str = 'is_occupied'
CAT_COL: str = 'neighbourhood_cleansed'
MAX_HORIZON: int = 7

# Columnas de metadatos que nunca son features
_META_COLS: list[str] = [TARGET_COL, 'listing_id', 'date']


# =============================================================================
# HELPERS INTERNOS
# =============================================================================

def _target_h_col(h: int) -> str:
    return f'{TARGET_COL}_target_h{h}'


def _all_target_h_cols(max_h: int = MAX_HORIZON) -> list[str]:
    return [_target_h_col(h) for h in range(1, max_h + 1)]


def _drop_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return df.drop(columns=[c for c in cols if c in df.columns], errors='ignore')


def _build_feature_matrix(df_subset: pd.DataFrame, pruned_features: list = None) -> pd.DataFrame:
    """Elimina meta-columnas, targets desplazados y columnas_basura."""
    all_targets = _all_target_h_cols()
    X = _drop_cols(df_subset, _META_COLS + all_targets)
    
    # Priorizar la lista inyectada sobre la global
    drop_list = pruned_features if pruned_features is not None else COLUMNAS_BASURA
    X = _drop_cols(X, drop_list)
    return X


def _generate_shifted_targets(df: pd.DataFrame, max_h: int = MAX_HORIZON) -> pd.DataFrame:
    """Añade columnas is_occupied_target_h1 .. hN con shift(-h) por listing_id."""
    df_out = df.copy()
    for h in range(1, max_h + 1):
        col = _target_h_col(h)
        df_out[col] = df_out.groupby('listing_id')[TARGET_COL].shift(-h)
    return df_out


# =============================================================================
# CLASE PRINCIPAL
# =============================================================================

class DirectForecastingWrapper:
    """
    Entrena 7 modelos XGBoost independientes, uno por horizonte h=1..7.

    Atributos públicos (disponibles tras fit_all o evaluate):
        models_    : dict {h: XGBClassifier}  — modelos entrenados
        pipelines_ : dict {h: Pipeline}       — preprocesadores TreePreprocessor por horizonte

    El wrapper también guarda las columnas_basura como constante interna
    para garantizar que producción elimine exactamente las mismas features
    que entrenamiento.
    """

    # Exponemos las constantes como atributos de clase para inspección
    columnas_basura: list[str] = COLUMNAS_BASURA
    target_col: str = TARGET_COL
    cat_col: str = CAT_COL
    max_horizon: int = MAX_HORIZON

    def __init__(
        self,
        n_estimators: int = 300,
        top_k_lags: int = 5,
        mi_sample_frac: float = 0.3,
        random_state: int = 42,
        pruned_features: list = None):
        self.n_estimators = n_estimators
        self.top_k_lags = top_k_lags
        self.mi_sample_frac = mi_sample_frac
        self.random_state = random_state
        self.pruned_features = pruned_features # Guardamos la lista inyectada

        self.models_: dict = {}
        self.pipelines_: dict = {}

    # ------------------------------------------------------------------
    # Entrenamiento de un único horizonte
    # ------------------------------------------------------------------

    def _fit_one_horizon(
        self,
        df_train: pd.DataFrame,
        h: int,
        max_train_date: pd.Timestamp,
        verbose: bool = True) -> None:
        """
        Entrena el modelo para el horizonte h.

        Anti-leakage: solo usa filas hasta max_train_date - h días,
        para que el target (h días en el futuro) nunca caiga en el
        periodo de validación.
        """
        target_h = _target_h_col(h)
        cutoff = max_train_date - pd.Timedelta(days=h)

        df_h = df_train[df_train['date'] <= cutoff].dropna(subset=[target_h])
        y_h = df_h[target_h]
        X_h = _build_feature_matrix(df_h, pruned_features=self.pruned_features)

        pipe = build_tree_pipeline(
            cat_col=CAT_COL,
            target_col=target_h,
            lag_prefix='is_occupied_lag_',
            top_k_lags=self.top_k_lags,
            mi_sample_frac=self.mi_sample_frac,
            verbose=verbose,
        )
        X_h_tree = pipe.fit_transform(X_h, y_h)

        modelo = xgb.XGBClassifier(
            n_estimators=self.n_estimators,
            enable_categorical=True,
            tree_method='hist',
            device='cuda',
            random_state=self.random_state,
            n_jobs=-1,
        )
        modelo.fit(X_h_tree, y_h)

        self.pipelines_[h] = pipe
        self.models_[h] = modelo

    # ------------------------------------------------------------------
    # Evaluación walk-forward
    # ------------------------------------------------------------------

    def evaluate(
        self,
        df: pd.DataFrame,
        folds: list[tuple],
        verbose: bool = True) -> pd.DataFrame:
        """
        Evalúa el modelo en un esquema walk-forward.

        Parámetros
        ----------
        df     : DataFrame procesado por run_global_pipeline (con shifted targets).
        folds  : Lista de tuplas (train_idx, val_idx) de walk_forward_split.

        Devuelve
        --------
        DataFrame con columnas: Fold, Horizonte, AUC, F1, Accuracy, Precision, Recall, Brier.
        """
        if _target_h_col(1) not in df.columns:
            df = _generate_shifted_targets(df)

        resultados = []

        for fold_i, (train_idx, val_idx) in enumerate(folds, 1):
            if verbose:
                print(f"\n{'─'*55}")
                print(f"[Direct Forecasting] Fold {fold_i}/{len(folds)}")

            df_train = df.loc[train_idx]
            max_train_date = df_train['date'].max()

            for h in range(1, self.max_horizon + 1):
                # ── Train: reutiliza _fit_one_horizon (guarda en self.models_/pipelines_) ──
                self._fit_one_horizon(df_train, h, max_train_date, verbose=verbose)

                # ── Validación ──────────────────────────────────────────────────────────
                target_h = _target_h_col(h)
                df_vl = df.loc[val_idx].dropna(subset=[target_h])
                y_vl  = df_vl[target_h]
                X_vl  = self.pipelines_[h].transform(
                    _build_feature_matrix(df_vl, pruned_features=self.pruned_features)
                )
                y_pred = self.models_[h].predict(X_vl)
                y_prob = self.models_[h].predict_proba(X_vl)[:, 1]

                res = {
                    'Fold':      fold_i,
                    'Horizonte': h,
                    'AUC':       roc_auc_score(y_vl, y_prob),
                    'F1':        f1_score(y_vl, y_pred, zero_division=0),
                    'Accuracy':  accuracy_score(y_vl, y_pred),
                    'Precision': precision_score(y_vl, y_pred, zero_division=0),
                    'Recall':    recall_score(y_vl, y_pred, zero_division=0),
                    'Brier':     brier_score_loss(y_vl, y_prob),
                }
                resultados.append(res)

                if verbose:
                    print(f"  h={h} → AUC {res['AUC']:.4f} | F1 {res['F1']:.4f} | Brier {res['Brier']:.4f}")

        return pd.DataFrame(resultados)

    # ------------------------------------------------------------------
    # Entrenamiento final (todos los datos, sin split)
    # ------------------------------------------------------------------

    def fit_all(
        self,
        df: pd.DataFrame,
        last_fold: tuple,
        verbose: bool = True) -> "DirectForecastingWrapper":
        """
        Entrena los 7 modelos usando el conjunto de datos hasta el corte del último fold.
        Guarda los modelos en self.models_ y los pipelines en self.pipelines_.

        Parámetros
        ----------
        df         : DataFrame completo procesado.
        last_fold  : Tupla (train_idx, val_idx) del último fold walk-forward.
                     Se entrena con train_idx para respetar la frontera temporal.
        """
        if _target_h_col(1) not in df.columns:
            df = _generate_shifted_targets(df)

        train_idx, _ = last_fold
        df_train = df.loc[train_idx]
        max_train_date = df_train['date'].max()

        for h in range(1, self.max_horizon + 1):
            if verbose:
                print(f"[fit_all] Entrenando h={h}...")
            self._fit_one_horizon(df_train, h, max_train_date, verbose=verbose)

        if verbose:
            print(f"\n✅ DirectForecastingWrapper entrenado para h=1..{self.max_horizon}")
        return self

    # ------------------------------------------------------------------
    # Inferencia
    # ------------------------------------------------------------------

    def predict_proba(self, X_today: pd.DataFrame) -> dict[int, np.ndarray]:
        """
        Predicción probabilística para los 7 horizontes dados los datos de hoy.

        Parámetros
        ----------
        X_today : DataFrame con las features del día actual (sin columnas target).

        Devuelve
        --------
        dict {h: array de probabilidades de clase 1}
        """
        if not self.models_:
            raise RuntimeError("El wrapper no está entrenado. Llama a fit_all() primero.")

        # Usamos la lista de la instancia
        drop_list = (self.pruned_features if self.pruned_features is not None else COLUMNAS_BASURA)
        X_clean = _drop_cols(X_today, drop_list + [TARGET_COL] + _all_target_h_cols() + ['listing_id', 'date'])
        resultado = {}
        for h in range(1, self.max_horizon + 1):
            X_tree = self.pipelines_[h].transform(X_clean)
            resultado[h] = self.models_[h].predict_proba(X_tree)[:, 1]
        return resultado

    def predict(self, X_today: pd.DataFrame) -> dict[int, np.ndarray]:
        """Predicción binaria para los 7 horizontes."""
        probas = self.predict_proba(X_today)
        return {h: (arr >= 0.5).astype(int) for h, arr in probas.items()}
