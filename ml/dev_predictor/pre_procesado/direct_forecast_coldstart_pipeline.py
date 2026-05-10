"""
Pipeline Exclusivo: Direct Forecasting Cold Start T+7
=====================================================
Archivo de referencia para listings SIN historia previa.
En lugar de usar lags individuales del listing, se apoya en variables de
'Momentum' de barrio calculadas en non_linear_coldstart_pipeline.py.

Decisiones de diseño:
  - COLUMNAS_BASURA interna está vacía: la poda manda desde el .ipynb.
  - top_k_lags=0: no se seleccionan lags individuales del listing.
  - Anti-leakage idéntico al wrapper Hot Start (cutoff por horizonte).
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import (
    roc_auc_score, f1_score, accuracy_score,
    precision_score, recall_score, brier_score_loss,
)

from pre_procesado.non_linear_coldstart_pipeline import create_neighborhood_momentum_features
from pre_procesado.non_linear_pipeline import build_tree_pipeline


# =============================================================================
# CONSTANTES (backup vacío)
# =============================================================================

COLUMNAS_BASURA: list[str] = [
    'day_of_week', 'is_weekend', 'days_to_next_holiday', 'is_holiday',
    'has_reviews', 'instant_bookable', 'temp_mean', 'precipitation_mm',
    'num_sports', 'num_festivals', 'total_attendance',
    'is_missing_review_scores_rating', 'occ_barrio_ma7',
    'occ_barrio_raw_delayed', 'occ_barrio_lag7', 'occ_barrio_lag14', 
    'reviews_barrio_momentum', 'streak_occupied', 'streak_vacant',
    'is_occupied_lag_1', 'is_occupied_lag_2', 'is_occupied_lag_3',
    'is_occupied_lag_4', 'is_occupied_lag_5', 'is_occupied_lag_6',
    'is_occupied_lag_7', 'is_occupied_lag_8', 'is_occupied_lag_9',
    'is_occupied_lag_10', 'is_occupied_lag_11', 'is_occupied_lag_12',
    'is_occupied_lag_13', 'is_occupied_lag_14'
]

TARGET_COL: str  = 'is_occupied'
CAT_COL: str     = 'neighbourhood_cleansed'
MAX_HORIZON: int = 7
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
    """Elimina meta-columnas, targets y aplica la poda inyectada desde el notebook."""
    all_targets = _all_target_h_cols()
    X = _drop_cols(df_subset, _META_COLS + all_targets)
    drop_list = pruned_features if pruned_features is not None else COLUMNAS_BASURA
    X = _drop_cols(X, drop_list)
    return X


def _generate_shifted_targets(df: pd.DataFrame, max_h: int = MAX_HORIZON) -> pd.DataFrame:
    """Añade columnas is_occupied_target_h1..hN desplazadas por listing_id."""
    df_out = df.copy()
    for h in range(1, max_h + 1):
        df_out[_target_h_col(h)] = df_out.groupby('listing_id')[TARGET_COL].shift(-h)
    return df_out


# =============================================================================
# WRAPPER COLD START
# =============================================================================

class DirectForecastingColdStartWrapper:
    """
    Entrena 7 modelos XGBoost para escenario Cold Start (sin historia de listing).

    Atributos públicos (tras evaluate o fit_all):
        models_    : dict {h: XGBClassifier}
        pipelines_ : dict {h: Pipeline}

    Uso típico desde el notebook:
        wrapper = DirectForecastingColdStartWrapper(
            n_estimators=300,
            pruned_features=COLUMNAS_BASURA  # lista definida en el .ipynb
        )
        df_resultados = wrapper.evaluate(df_processed, folds)
    """

    def __init__(
        self,
        n_estimators: int = 300,
        random_state: int = 42,
        pruned_features: list = None,
    ):
        self.n_estimators   = n_estimators
        self.random_state   = random_state
        self.pruned_features = pruned_features  # None → usa COLUMNAS_BASURA (vacía)
        self.max_horizon    = MAX_HORIZON

        self.models_: dict    = {}
        self.pipelines_: dict = {}

    # ------------------------------------------------------------------
    # Preparación de datos
    # ------------------------------------------------------------------

    def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Inyecta Momentum de barrio y targets desplazados si no existen."""
        if 'occ_barrio_raw_delayed' not in df.columns:
            df = create_neighborhood_momentum_features(df)
        if _target_h_col(1) not in df.columns:
            df = _generate_shifted_targets(df)
        return df

    # ------------------------------------------------------------------
    # Entrenamiento de un único horizonte
    # ------------------------------------------------------------------

    def _fit_one_horizon(
        self,
        df_train: pd.DataFrame,
        h: int,
        max_train_date: pd.Timestamp,
        verbose: bool = True,
    ) -> None:
        """Anti-leakage: solo usa filas hasta max_train_date - h días."""
        target_h = _target_h_col(h)
        cutoff   = max_train_date - pd.Timedelta(days=h)

        df_h = df_train[df_train['date'] <= cutoff].dropna(subset=[target_h])
        y_h  = df_h[target_h]
        X_h  = _build_feature_matrix(df_h, pruned_features=self.pruned_features)

        # top_k_lags=0: no seleccionamos lags individuales del listing
        pipe = build_tree_pipeline(
            cat_col=CAT_COL,
            target_col=target_h,
            top_k_lags=0,
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
        self.models_[h]    = modelo

    # ------------------------------------------------------------------
    # Evaluación walk-forward
    # ------------------------------------------------------------------

    def evaluate(
        self,
        df: pd.DataFrame,
        folds: list[tuple],
        verbose: bool = True,
    ) -> pd.DataFrame:
        """
        Evalúa en walk-forward. Tras la última iteración, los modelos del
        último fold quedan disponibles en self.models_ / self.pipelines_.
        """
        df = self._prepare_df(df)
        resultados = []

        for fold_i, (train_idx, val_idx) in enumerate(folds, 1):
            if verbose:
                print(f"\n{'─'*55}")
                print(f"[Cold Start] Fold {fold_i}/{len(folds)}")

            df_train      = df.loc[train_idx]
            max_train_date = df_train['date'].max()

            for h in range(1, self.max_horizon + 1):
                # Train — reutiliza _fit_one_horizon
                self._fit_one_horizon(df_train, h, max_train_date, verbose=verbose)

                # Validación
                target_h = _target_h_col(h)
                df_vl    = df.loc[val_idx].dropna(subset=[target_h])
                y_vl     = df_vl[target_h]
                X_vl     = self.pipelines_[h].transform(
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
    # Entrenamiento final (para serialización con joblib)
    # ------------------------------------------------------------------

    def fit_all(
        self,
        df: pd.DataFrame,
        last_fold: tuple,
        verbose: bool = True,
    ) -> "DirectForecastingColdStartWrapper":
        """
        Entrena los 7 modelos con los datos de train del último fold.
        Usar tras evaluate() para obtener los pesos finales para joblib.
        """
        df = self._prepare_df(df)
        train_idx, _ = last_fold
        df_train      = df.loc[train_idx]
        max_train_date = df_train['date'].max()

        for h in range(1, self.max_horizon + 1):
            if verbose:
                print(f"[fit_all] Entrenando h={h}...")
            self._fit_one_horizon(df_train, h, max_train_date, verbose=verbose)

        if verbose:
            print(f"\n✅ DirectForecastingColdStartWrapper entrenado h=1..{self.max_horizon}")
        return self

    # ------------------------------------------------------------------
    # Inferencia
    # ------------------------------------------------------------------

    def predict_proba(self, X_today: pd.DataFrame) -> dict[int, np.ndarray]:
        """
        Predicción probabilística para los 7 horizontes.
        X_today debe incluir las variables de momentum de barrio.
        """
        if not self.models_:
            raise RuntimeError("El wrapper no está entrenado. Llama a evaluate() o fit_all() primero.")
        X_clean = _build_feature_matrix(X_today, pruned_features=self.pruned_features)
        return {
            h: self.models_[h].predict_proba(self.pipelines_[h].transform(X_clean))[:, 1]
            for h in range(1, self.max_horizon + 1)
        }

    def predict(self, X_today: pd.DataFrame) -> dict[int, np.ndarray]:
        """Predicción binaria (umbral 0.5) para los 7 horizontes."""
        return {h: (arr >= 0.5).astype(int) for h, arr in self.predict_proba(X_today).items()}
