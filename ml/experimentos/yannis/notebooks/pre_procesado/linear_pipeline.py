"""
Pipeline Lineal (Fase 3A)
==========================
Preprocesamiento específico para modelos lineales (Regresión Logística).
Incluye: selección de lags por MI, imputación por mediana contextual,
Target Encoding con smoothing, winsorización y escalado.

IMPORTANTE: Todo se recalcula (fit) desde cero en cada pliegue del Walk-Forward CV.
"""

import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import mutual_info_classif


# =============================================================================
# 1. SELECCIÓN DE LAGS POR MUTUAL INFORMATION
# =============================================================================

def select_lags_by_mi(X_train: pd.DataFrame, y_train: pd.Series,
                      lag_prefix: str = 'is_occupied_lag_',
                      sample_frac: float = 0.3,
                      top_k: int = 5,
                      random_state: int = 42) -> list:
    """
    Selecciona los top_k lags más relevantes usando Mutual Information
    sobre una muestra representativa del set de Train.

    Devuelve la lista de nombres de columnas de los lags seleccionados.
    """
    lag_cols = [c for c in X_train.columns if c.startswith(lag_prefix)]

    if not lag_cols:
        print("[MI Lags] No se encontraron columnas de lag. Saltando selección.")
        return []

    # Muestra representativa para ahorrar cómputo
    n_sample = int(len(X_train) * sample_frac)
    idx = X_train.sample(n=n_sample, random_state=random_state).index

    X_sample = X_train.loc[idx, lag_cols].copy()
    y_sample = y_train.loc[idx]

    # Eliminar nulos para MI
    mask = X_sample.notna().all(axis=1) & y_sample.notna()
    X_clean = X_sample[mask]
    y_clean = y_sample[mask]

    if len(X_clean) == 0:
        print("[MI Lags] Muestra vacía tras eliminar nulos. Devolviendo todos los lags.")
        return lag_cols

    mi_scores = mutual_info_classif(X_clean, y_clean, random_state=random_state)
    mi_series = pd.Series(mi_scores, index=lag_cols).sort_values(ascending=False)

    selected = mi_series.head(top_k).index.tolist()
    print(f"[MI Lags] Top {top_k} lags seleccionados: {selected}")
    print(f"          Scores: {mi_series.head(top_k).round(4).to_dict()}")

    return selected


# =============================================================================
# 2. IMPUTACIÓN POR MEDIANA CONTEXTUAL (BARRIO + INSTANTE DE TIEMPO)
# =============================================================================

class ContextualMedianImputer(BaseEstimator, TransformerMixin):
    """
    Custom Transformer: imputa nulos con la mediana histórica de la variable
    agrupada por barrio.

    NO se agrupa por fecha: las fechas del set de validación nunca existirán
    en el fit de entrenamiento, lo que provocaría un fallback al 100% a la
    mediana global. La información temporal ya la captura occ_barrio_ayer
    generada en el Pipeline Global (Fase 1).

    Si un barrio no aparece en Train (cold start),
    usa la mediana global de la variable en Train.

    fit: calcula las medianas por barrio y la mediana global de Train.
    transform: imputa valores faltantes usando la jerarquía barrio->global.
    """

    def __init__(self, group_cols=None, numeric_cols=None):
        self.group_cols = group_cols or ['neighbourhood_cleansed']
        self.numeric_cols = numeric_cols

    def fit(self, X, y=None):
        if self.numeric_cols is None:
            self.numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()

        # Medianas históricas por barrio (sin fecha)
        self.group_medians_ = X.groupby(self.group_cols)[self.numeric_cols].median()

        # Fallback: mediana global por columna
        self.global_medians_ = X[self.numeric_cols].median()

        return self

    def transform(self, X):
        X_out = X.copy()

        for col in self.numeric_cols:
            if col not in X_out.columns:
                continue

            null_mask = X_out[col].isna()
            if not null_mask.any():
                continue

            # Intentar rellenar con mediana del grupo
            if col in self.group_medians_.columns:
                fill_values = X_out[null_mask].set_index(self.group_cols).index.map(
                    lambda idx: self.group_medians_.loc[idx, col]
                    if idx in self.group_medians_.index else np.nan
                )
                X_out.loc[null_mask, col] = fill_values.values

            # Fallback a mediana global para los que siguen siendo NaN
            still_null = X_out[col].isna()
            if still_null.any():
                X_out.loc[still_null, col] = self.global_medians_[col]

        return X_out


# =============================================================================
# 3. TARGET ENCODING CON SMOOTHING
# =============================================================================

class SmoothTargetEncoder(BaseEstimator, TransformerMixin):
    """
    Custom Transformer: Target Encoding con Smoothing para la variable de barrio.

    Fórmula: encoding = (count * mean_barrio + m * mean_global) / (count + m)
    Donde m (smoothing) controla la regularización:
    - m alto -> más peso a la media global (protege contra barrios con pocas muestras).

    PRECAUCIÓN: Se recalcula desde cero en cada fold del Walk-Forward CV.
    """

    def __init__(self, cat_col: str = 'neighbourhood_cleansed',
                 target_col: str = 'is_occupied', smoothing: float = 10.0):
        self.cat_col = cat_col
        self.target_col = target_col
        self.smoothing = smoothing

    def fit(self, X, y=None):
        # Si y no se pasa, intentamos extraerlo de X
        if y is None:
            y = X[self.target_col]

        df_temp = X[[self.cat_col]].copy()
        df_temp['__target__'] = y.values

        # Estadísticos por categoría
        stats = df_temp.groupby(self.cat_col)['__target__'].agg(['mean', 'count'])
        self.global_mean_ = y.mean()

        # Smoothed encoding
        stats['encoding'] = (
            (stats['count'] * stats['mean'] + self.smoothing * self.global_mean_)
            / (stats['count'] + self.smoothing)
        )

        self.encoding_map_ = stats['encoding'].to_dict()
        return self

    def transform(self, X):
        X_out = X.copy()
        X_out[f'{self.cat_col}_encoded'] = (
            X_out[self.cat_col]
            .map(self.encoding_map_)
            .fillna(self.global_mean_)  # Categorías nuevas -> media global
        )
        # Eliminar la columna categórica original para evitar que el modelo
        # lineal reciba strings (crashearía en la multiplicación de coeficientes)
        X_out.drop(columns=[self.cat_col], inplace=True)
        return X_out


# =============================================================================
# 4. WINSORIZACIÓN POR PERCENTILES
# =============================================================================

class PercentileWinsorizer(BaseEstimator, TransformerMixin):
    """
    Custom Transformer: recorta outliers al percentil inferior y superior.
    fit: calcula los límites sobre Train.
    transform: aplica los límites (clip) en Train/Val/Test.
    """

    def __init__(self, lower_pct: float = 0.01, upper_pct: float = 0.99,
                 numeric_cols: list = None):
        self.lower_pct = lower_pct
        self.upper_pct = upper_pct
        self.numeric_cols = numeric_cols

    def fit(self, X, y=None):
        if self.numeric_cols is None:
            self.numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()

        self.bounds_ = {}
        for col in self.numeric_cols:
            if col in X.columns:
                self.bounds_[col] = (
                    X[col].quantile(self.lower_pct),
                    X[col].quantile(self.upper_pct)
                )
        return self

    def transform(self, X):
        X_out = X.copy()
        for col, (lo, hi) in self.bounds_.items():
            if col in X_out.columns:
                X_out[col] = X_out[col].clip(lower=lo, upper=hi)
        return X_out


# =============================================================================
# 5. CONSTRUCTOR DEL PIPELINE LINEAL
# =============================================================================

def build_linear_pipeline(numeric_cols: list = None,
                          cat_col: str = 'neighbourhood_cleansed',
                          target_col: str = 'is_occupied',
                          smoothing: float = 10.0,
                          lower_pct: float = 0.01,
                          upper_pct: float = 0.99) -> Pipeline:
    """
    Construye el sklearn Pipeline para modelos lineales.
    Cada paso es fit en Train y transform en Val/Test.

    Secuencia:
    1. Imputación contextual (mediana histórica por barrio)
    2. Target Encoding con Smoothing (barrio)
    3. Winsorización por percentiles
    4. StandardScaler
    """
    steps = [
        ('imputer', ContextualMedianImputer(numeric_cols=numeric_cols)),
        ('target_encoder', SmoothTargetEncoder(
            cat_col=cat_col,
            target_col=target_col,
            smoothing=smoothing
        )),
        ('winsorizer', PercentileWinsorizer(
            lower_pct=lower_pct,
            upper_pct=upper_pct,
            numeric_cols=numeric_cols
        )),
        ('scaler', StandardScaler()),
    ]

    pipeline = Pipeline(steps)

    print("[Pipeline Lineal] Construido: Imputer -> TargetEncoder -> Winsorizer -> StandardScaler.")
    return pipeline