"""
Pipeline No Lineal (Árboles)
Implementa el preprocesamiento específico para modelos de árboles (XGBoost, LightGBM), manteniendo valores nulos.
"""
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from pre_procesado.linear_pipeline import select_lags_by_mi

# =============================================================================
# 1. TRANSFORMADOR PARA MODELOS DE ÁRBOLES
# =============================================================================

class TreePreprocessor(BaseEstimator, TransformerMixin):
    """
    Custom Transformer para preparar datos para LightGBM/XGBoost.
    fit: Selecciona los mejores lags usando Mutual Information.
    transform: Convierte categóricas, elimina lags inútiles e IDs, preserva NaNs.
    """
    def __init__(self, 
                 cat_col: str = 'neighbourhood_cleansed',
                 target_col: str = 'is_occupied',
                 lag_prefix: str = 'is_occupied_lag_',
                 top_k_lags: int = 5,
                 mi_sample_frac: float = 0.3,
                 drop_cols: list = None,
                 verbose: bool = True):
        self.cat_col = cat_col
        self.target_col = target_col
        self.lag_prefix = lag_prefix
        self.top_k_lags = top_k_lags
        self.mi_sample_frac = mi_sample_frac
        self.drop_cols = drop_cols or ['listing_id', 'date']
        self.verbose = verbose

    def fit(self, X, y):
        # 1. Selección de lags por MI en tiempo de fit
        if self.verbose: print("🌲 [TreePreprocessor] Ejecutando fit: Seleccionando features...")
        self.selected_lags_ = select_lags_by_mi(
            X, y,
            lag_prefix=self.lag_prefix,
            top_k=self.top_k_lags,
            sample_frac=self.mi_sample_frac,
            verbose=self.verbose
        )
        return self

    def transform(self, X):
        X_out = X.copy()
        
        # 1. Convertir barrio a tipo 'category' nativo
        if self.cat_col in X_out.columns:
            X_out[self.cat_col] = X_out[self.cat_col].astype('category')

        # 2. Identificar lags a eliminar
        all_lag_cols = [c for c in X_out.columns if c.startswith(self.lag_prefix)]
        lags_to_drop = [c for c in all_lag_cols if c not in self.selected_lags_]

        # 3. Ensamblar lista de columnas a borrar (incluyendo el target y las IDs/Basura)
        cols_to_drop = self.drop_cols + lags_to_drop + [self.target_col]
        cols_to_drop = [c for c in cols_to_drop if c in X_out.columns]

        X_out.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        return X_out

# =============================================================================
# 2. CONSTRUCTOR DEL PIPELINE NO LINEAL
# =============================================================================

def build_tree_pipeline(cat_col: str = 'neighbourhood_cleansed',
                        target_col: str = 'is_occupied',
                        lag_prefix: str = 'is_occupied_lag_',
                        top_k_lags: int = 5,
                        mi_sample_frac: float = 0.3,
                        drop_cols: list = None,
                        verbose: bool = True) -> Pipeline:
    """
    Construye el sklearn Pipeline para modelos de árboles.
    """
    steps = [
        ('tree_preprocessor', TreePreprocessor(
            cat_col=cat_col,
            target_col=target_col,
            lag_prefix=lag_prefix,
            top_k_lags=top_k_lags,
            mi_sample_frac=mi_sample_frac,
            drop_cols=drop_cols,
            verbose=verbose
        ))
    ]
    
    pipeline = Pipeline(steps)
    
    if verbose: print("[Pipeline Árboles] Construido: TreePreprocessor (Sin escalado, NaNs nativos).")
    return pipeline