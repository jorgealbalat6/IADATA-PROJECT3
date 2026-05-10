import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
import geopandas as gpd
from statsmodels.tsa.stattools import ccf, grangercausalitytests
from sklearn.feature_selection import mutual_info_regression, mutual_info_classif
import warnings

def aggregate_daily_timeseries(df: pd.DataFrame, target_col: str = 'is_occupied', continuous_cols: list = []) -> pd.DataFrame:
    """
    Agrega el dataset a nivel diario y rolling window 7 dias para suavizar tendencia.
    """
    print("[+] Agregando serie temporal a frecuencia diaria...")
    
    # Diccionario de agregación
    agg_dict = {
        target_col: 'mean',           # Tasa de ocupación diaria
        'listing_id': 'count'         # Volumen de la muestra
    }
    
    
    for col in continuous_cols:
        if col in df.columns:
            agg_dict[col] = 'mean'
            
    ts_df = df.groupby('date').agg(agg_dict).rename(columns={target_col: 'tasa_ocupacion', 'listing_id': 'record_count'})
    ts_df.index = pd.to_datetime(ts_df.index)
    ts_df = ts_df.asfreq('D')
    
    # Suavizado del target
    ts_df['tasa_ocupacion_suavizada_7d'] = ts_df['tasa_ocupacion'].rolling(window=7, min_periods=1).mean()
    
    
    for col in continuous_cols:
        if col in ts_df.columns:
            ts_df[f'{col}_suavizada_7d'] = ts_df[col].rolling(window=7, min_periods=1).mean()
    
    print(f"[+] Serie agregada creada. Dimensiones: {ts_df.shape}")
    return ts_df

def plot_cross_correlation(ts_df: pd.DataFrame, target: str, predictors: list, max_lags: int = 14):
    """
    Calcula y grafica la Función de Correlación Cruzada (CCF).
    Muestra si un predictor "lidera" o "retrasa" al target linealmente.
    """
    print("\n" + "="*60)
    print("📈 FUNCIÓN DE CORRELACIÓN CRUZADA (CCF)")
    print("="*60)
    
    df_clean = ts_df[[target] + predictors].dropna()
    
    fig, axes = plt.subplots(len(predictors), 1, figsize=(12, 3 * len(predictors)), sharex=True)
    # Si solo hay un predictor, axes no es un array, lo forzamos a lista para iterar
    if len(predictors) == 1: 
        axes = [axes]
        
    for i, pred in enumerate(predictors):
        # ccf calcula la correlación cruzada hacia adelante
        ccf_values = ccf(df_clean[target], df_clean[pred])[:max_lags+1]
        
        # SOLUCIÓN AL ERROR: Se elimina 'use_line_collection=True'
        axes[i].stem(range(max_lags+1), ccf_values, basefmt="k-")
        axes[i].axhline(0, color='black', linewidth=1)
        
        # Banda de significancia aproximada (95%)
        conf_level = 1.96 / np.sqrt(len(df_clean))
        axes[i].axhspan(-conf_level, conf_level, alpha=0.2, color='blue')
        
        axes[i].set_title(f'CCF: {target} vs Lags de {pred}', fontsize=12)
        axes[i].set_ylabel('Correlación')
        
    plt.xlabel('Lags (Días hacia el pasado del predictor)')
    plt.tight_layout()
    plt.show()

def calculate_mi_with_lags(ts_df: pd.DataFrame, target: str, predictors: list, lags: list = [1, 2, 7]):
    """
    Calcula la Información Mutua entre el target actual y valores pasados (lags) de los predictores.
    Detecta relaciones no lineales en el tiempo.
    """
    print("\n" + "="*60)
    print(f"🧠 INFORMACIÓN MUTUA (MI) CON LAGS: {lags}")
    print("="*60)
    
    df_lagged = ts_df[[target] + predictors].copy()
    feature_names = []
    
    # Crear las columnas de lags
    for pred in predictors:
        for lag in lags:
            col_name = f"{pred}_lag_{lag}"
            df_lagged[col_name] = df_lagged[pred].shift(lag)
            feature_names.append(col_name)
            
    # Eliminar los primeros N días que ahora tienen NaNs por el shift
    df_clean = df_lagged.dropna()
    
    X = df_clean[feature_names]
    y = df_clean[target]
    
    # Determinamos si el target es continuo (tasa) o binario para usar la función MI correcta
    if y.nunique() > 2:
        mi_scores = mutual_info_regression(X, y, random_state=42)
    else:
        mi_scores = mutual_info_classif(X, y, random_state=42)
        
    mi_series = pd.Series(mi_scores, index=feature_names).sort_values(ascending=False)
    
    plt.figure(figsize=(10, 6))
    sns.barplot(x=mi_series.values, y=mi_series.index, palette='mako')
    plt.title('Información Mutua de Variables Rezagadas (Lags)', fontsize=14)
    plt.xlabel('MI Score (Poder predictivo no lineal)')
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
    
    print(mi_series.to_frame(name='MI Score'))

def run_granger_causality(ts_df: pd.DataFrame, target: str, predictors: list, max_lag: int = 7):
    """
    Realiza el Test de Causalidad de Granger.
    Evalúa si la historia de un predictor mejora estadísticamente la predicción del target.
    """
    print("\n" + "="*60)
    print(f"🔗 TEST DE CAUSALIDAD DE GRANGER (Max Lag: {max_lag})")
    print("="*60)
    print("H0: El predictor NO Granger-causa el target.")
    print("Regla: Si p-valor < 0.05, RECHAZAMOS H0 (Sí hay Granger-Causalidad).\n")
    
    df_clean = ts_df[[target] + predictors].dropna()
    
    for pred in predictors:
        print(f"--- Evaluando: {pred} -> {target} ---")
        # El algoritmo espera un array 2D donde la columna 0 es el Target y la 1 es el Predictor
        data = df_clean[[target, pred]]
        
        try:
            # verbose=False para no inundar la consola, extraeremos los p-valores del diccionario
            test_result = grangercausalitytests(data, maxlag=max_lag, verbose=False)
            
            # Extraemos el p-valor del test F (SSR) para cada lag
            for lag in range(1, max_lag + 1):
                p_value = test_result[lag][0]['ssr_ftest'][1]
                significativo = "✅ SÍ" if p_value < 0.05 else "❌ NO"
                print(f"  Lag {lag}: p-valor = {p_value:.4e} | Causalidad: {significativo}")
        except Exception as e:
            print(f"  Error al procesar {pred}: {str(e)}")
        print("")