import pandas as pd
import numpy as np
import geopandas as gpd
import warnings
import matplotlib.pyplot as plt
import seaborn as sns

from statsmodels.tsa.seasonal import MSTL
from statsmodels.tsa.stattools import adfuller, kpss
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.stats.diagnostic import acorr_ljungbox

#__________________________________________________________________
# DINÁMICA Y DEPENDENCIA TEMPORAL
#__________________________________________________________________


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


def run_stationarity_tests(ts_df: pd.DataFrame, col: str = 'tasa_ocupacion_suavizada_7d'):
    """
    Ejecuta ADF (Dickey-Fuller) y KPSS.
    - ADF H0: La serie TIENE raíz unitaria (No es estacionaria). p < 0.05 -> Es estacionaria.
    - KPSS H0: La serie ES estacionaria. p < 0.05 -> No es estacionaria.
    """
    print("\n" + "="*60)
    print(f"📈 TESTS DE ESTACIONARIEDAD: {col}")
    print("="*60)
    
    data = ts_df[col].dropna()
    
    # ADF Test
    adf_result = adfuller(data, autolag='AIC')
    adf_p = adf_result[1]
    print(f"1. Test ADF (Dickey-Fuller Aumentado):")
    print(f"   - p-valor: {adf_p:.4f}")
    print(f"   - Conclusión: {'Es ESTACIONARIA' if adf_p < 0.05 else 'NO es estacionaria (Tiene tendencia/raíz unitaria)'}")
    
    # KPSS Test
    kpss_result = kpss(data, regression='c', nlags="auto")
    kpss_p = kpss_result[1]
    print(f"\n2. Test KPSS:")
    print(f"   - p-valor: {kpss_p:.4f}")
    print(f"   - Conclusión: {'NO es estacionaria' if kpss_p < 0.05 else 'Es ESTACIONARIA'}")
    
    # Ljung-Box Test (Box-Pierce) sobre la serie cruda
    lb_result = acorr_ljungbox(data, lags=[7], return_df=True)
    lb_p = lb_result['lb_pvalue'].iloc[0]
    print(f"\n3. Test Ljung-Box (Lag 7 - Autocorrelación global):")
    print(f"   - p-valor: {lb_p:.4f}")
    print(f"   - Conclusión: {'Tiene autocorrelación fuerte' if lb_p < 0.05 else 'Es Ruido Blanco (Difícil de predecir)'}")
    print("-" * 60)

def plot_autocorrelation(ts_df: pd.DataFrame, col: str = 'tasa_ocupacion_suavizada_7d', lags: int = 30):
    """
    Plotea ACF (Autocorrelación) y PACF (Autocorrelación Parcial).
    Dicta qué retardos (lags) debemos usar como variables (ej. lag_1, lag_7).
    """
    data = ts_df[col].dropna()
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    
    plot_acf(data, lags=lags, ax=axes[0], color='teal')
    axes[0].set_title(f'ACF (Autocorrelation Function): {col}')
    axes[0].set_xlabel('Lags (Días)')
    
    plot_pacf(data, lags=lags, ax=axes[1], color='darkorange', method='ywm')
    axes[1].set_title(f'PACF (Partial Autocorrelation Function): {col}')
    axes[1].set_xlabel('Lags (Días)')
    
    plt.tight_layout()
    plt.show()


def plot_mstl_decomposition(ts_df: pd.DataFrame, col: str = 'tasa_ocupacion_suavizada_7d', periods: tuple = (7, 30)):
    """
    Descomposición MSTL para MÚLTIPLES estacionalidades.
    Extrae simultáneamente varios ciclos (ej. semanal y mensual) y la tendencia macro.
    """
    data = ts_df[col].dropna()
    
    # Configuramos MSTL con la tupla de periodos
    mstl = MSTL(data, periods=periods)
    res = mstl.fit()
    
    # Ploteamos el resultado
    fig = res.plot()
    # Ajustamos dinámicamente el tamaño según cuántos componentes haya
    fig.set_size_inches(10,6) 
    plt.suptitle(f'Descomposición MSTL (Múltiples Estacionalidades): {periods}', fontsize=15, y=1.02)
    plt.tight_layout()
    plt.show()
    
    return res # Devolvemos el objeto para poder evaluar sus residuos luego

def test_mstl_residuals_ljungbox(mstl_result, lags: list = [7, 14, 28]):
    """
    Aplica el test de Ljung-Box directamente al objeto de resultados del MSTL.
    """
    print("\n" + "="*60)
    print(f"🧩 TEST DE RUIDO BLANCO (LJUNG-BOX) EN RESIDUOS MSTL")
    print("="*60)
    
    # Extraemos los residuos puros del objeto MSTL
    residuos = mstl_result.resid.dropna()
    
    # Aplicar Ljung-Box
    lb_result = acorr_ljungbox(residuos, lags=lags, return_df=True)
    
    lb_report = lb_result.rename(columns={'lb_stat': 'Estadístico', 'lb_pvalue': 'p-valor'})
    lb_report['Interpretación'] = np.where(lb_report['p-valor'] > 0.05, 'Ruido Blanco (✅)', 'Autocorrelación (⚠️)')
    print(lb_report.to_string())
    
    print("\nConclusión General:")
    p_val_eval = lb_result['lb_pvalue'].iloc[0]
    
    if p_val_eval > 0.05:
        print("   ✅ Los residuos son ruido blanco puro (p > 0.05).")
    else:
        print("   ⚠️ Los residuos AÚN tienen autocorrelación (p < 0.05).")
    print("-" * 60)