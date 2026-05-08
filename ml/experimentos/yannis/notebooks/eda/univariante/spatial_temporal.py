import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from libpysal.weights import Queen
from esda.moran import Moran
import warnings
from shapely import wkt
from geopandas import GeoDataFrame

from splot.esda import plot_moran, lisa_cluster
import skgstat as skg


# =============================================================================
# COROPLETAS POR PERIODO (SMALL MULTIPLES)
# =============================================================================
def plot_choropleths_by_period(gdf_points, gdf_polygons, col, period_col='month'):
    """
    Genera mapas de coropletas facetados corrigiendo el error de aspecto y tipos de datos.
    """
    print(f"\n[+] Generando Coropletas por periodo para: {col}...")
    
    # Preparamos el periodo
    if period_col == 'month':
        # Aseguramos que month sea numérico para el cálculo
        gdf_points['month_num'] = gdf_points['month'].astype(float)
        gdf_points['quarter'] = gdf_points['month_num'].apply(lambda x: (x-1)//3 + 1 if not np.isnan(x) else np.nan)
        plot_period = 'quarter'
    else:
        plot_period = period_col

    # Agrupación y limpieza de claves
    agg_df = gdf_points.groupby(['neighbourhood_cleansed', plot_period])[col].mean().reset_index()
    agg_df['key_spatial'] = agg_df['neighbourhood_cleansed'].str.lower().str.strip()
    gdf_polygons['key_spatial'] = gdf_polygons['nom_barri'].str.lower().str.strip()
    
    # --- CORRECCIÓN DE TIPOS ---
    agg_df[col] = agg_df[col].astype(float)
    # ---------------------------

    periodos = sorted([p for p in agg_df[plot_period].unique() if pd.notna(p)])
    
    fig, axes = plt.subplots(1, len(periodos), figsize=(5 * len(periodos), 6))
    if len(periodos) == 1: axes = [axes]
    
    vmin, vmax = agg_df[col].min(), agg_df[col].max()
    
    for i, p in enumerate(periodos):
        data_periodo = agg_df[agg_df[plot_period] == p]
        
        merged = gdf_polygons.merge(data_periodo, on='key_spatial', how='left')
        
        merged = GeoDataFrame(merged, geometry='geometry')
        
        merged.plot(column=col, cmap='YlOrRd', linewidth=0.5, edgecolor='0.5', 
                        ax=axes[i], vmin=vmin, vmax=vmax, 
                        legend=(i == len(periodos)-1),
                        missing_kwds={'color': 'lightgrey'}, 
                        aspect='equal') 
        
        axes[i].set_title(f'Trimestre {int(p)}' if plot_period == 'quarter' else f'Periodo {p}')
        axes[i].axis('off')
        
    plt.suptitle(f'Evolución Espacial de {col}', fontsize=16)
    plt.tight_layout()
    plt.show()

    
# =============================================================================
# EVOLUCIÓN TEMPORAL DE LA AUTOCORRELACIÓN ESPACIAL (MORAN'S I OVER TIME)
# =============================================================================

def plot_moran_over_time(gdf_points: gpd.GeoDataFrame, gdf_polygons: gpd.GeoDataFrame, col: str):
    """
    Calcula el Índice de Moran Global para cada mes y lo grafica en una serie temporal.
    Identifica si el "clustering" espacial es estacional.
    """
    print(f"\n[+] Calculando Índice de Moran mensual para: {col}...")
    
    # 1. Matriz de pesos basada en los polígonos
    gdf_polygons['key_spatial'] = gdf_polygons['nom_barri'].str.lower().str.strip()
    w = Queen.from_dataframe(gdf_polygons)
    w.transform = 'r'
    
    meses = sorted(gdf_points['month'].unique())
    moran_values = []
    
    for m in meses:
        # Extraer datos del mes
        data_mes = gdf_points[gdf_points['month'] == m]
        agg_mes = data_mes.groupby('neighbourhood_cleansed')[col].mean().reset_index()
        agg_mes['key_spatial'] = agg_mes['neighbourhood_cleansed'].str.lower().str.strip()
        
        # Unir con polígonos manteniendo el orden de los polígonos (crítico para PySAL)
        merged = gdf_polygons[['key_spatial', 'geometry']].merge(agg_mes, on='key_spatial', how='left')
        
        # Imputar media global del mes si algún barrio no tuvo datos ese mes para no romper la matriz W
        merged[col] = merged[col].fillna(merged[col].mean())
        
        y = merged[col].values
        moran = Moran(y, w)
        moran_values.append(moran.I)
        
    # Graficar la evolución
    plt.figure(figsize=(10, 4))
    sns.lineplot(x=meses, y=moran_values, marker='o', color='crimson', linewidth=2)
    plt.axhline(0, color='black', linestyle='--', linewidth=1) # Línea de aleatoriedad
    
    plt.title(f'Evolución de la Autocorrelación Espacial (Moran I) - {col}', fontsize=14)
    plt.xlabel('Mes del Año')
    plt.ylabel('Índice de Moran (I)')
    plt.xticks(meses)
    plt.grid(True, alpha=0.3)
    plt.show()