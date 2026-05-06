import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from shapely import wkt
import geopandas as gpd
import warnings

def analyze_seasonal_spatial_clusters(df: pd.DataFrame, target: str = 'is_occupied', price_col: str = 'listing_price'):
    """
    Analiza cómo la agrupación de precios y ocupación cambia entre Temporada Alta (Verano) y el resto del año.
    Valida la hipótesis de que el comportamiento se vuelve aleatorio en verano.
    """
    print("\n" + "="*60)
    print("☀️ DINÁMICA ESTACIONAL: VERANO vs RESTO DEL AÑO")
    print("="*60)

    # Definimos verano como Julio, Agosto y Septiembre (meses 7, 8, 9)
    df_clean = df.copy()
    df_clean['is_summer'] = df_clean['month'].isin([7, 8, 9]).astype(int)

    # Comparamos la distribución de precios en barrios principales según la temporada
    top_barrios = df_clean['neighbourhood_cleansed'].value_counts().nlargest(10).index
    df_top = df_clean[df_clean['neighbourhood_cleansed'].isin(top_barrios)]

    plt.figure(figsize=(14, 6))
    # Aplicamos log1p al precio para visualizar mejor sin el efecto de outliers extremos
    df_top['log_price'] = np.log1p(df_top[price_col])
    
    sns.violinplot(data=df_top, x='neighbourhood_cleansed', y='log_price', hue='is_summer', split=True, palette='muted')
    plt.title('Distribución Espacial del Precio: Verano (1) vs Resto del Año (0)', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.ylabel('Precio (Log Scale)')
    plt.tight_layout()
    plt.show()

def compute_spatiotemporal_lags(df: pd.DataFrame, target: str = 'is_occupied', time_col: str = 'date', space_col: str = 'neighbourhood_cleansed'):
    """
    Calcula variables de rezago espacio-temporal (Spatio-Temporal Lags).
    Esta es la alternativa práctica al STCCF para Machine Learning.
    Calcula la ocupación media del barrio en los últimos X días.
    """
    print("\n" + "="*60)
    print("⏳🗺️ INGENIERÍA DE VARIABLES: REZAGOS ESPACIO-TEMPORALES")
    print("="*60)

    # Agrupamos por barrio y día para sacar la tasa diaria por zona
    daily_spatial_agg = df.groupby([space_col, time_col])[target].mean().reset_index()
    
    # Ordenamos para asegurar que el rolling window tenga sentido temporal
    daily_spatial_agg = daily_spatial_agg.sort_values(by=[space_col, time_col])

    # Calculamos la tasa de ocupación del barrio en la última semana (excluyendo hoy para evitar data leakage)
    # shift(1) asegura que estamos mirando los 7 días anteriores al día actual
    daily_spatial_agg['tasa_barrio_7d_pasados'] = daily_spatial_agg.groupby(space_col)[target].transform(
        lambda x: x.shift(1).rolling(window=7, min_periods=1).mean()
    )

    print("[+] Ejemplo de rezagos espacio-temporales generados:")
    print(daily_spatial_agg.dropna().head(10).to_string())
    print("\n[!] Estas variables (tasa_barrio_7d_pasados) son ORO puro para tu modelo XGBoost.")
    
    return daily_spatial_agg