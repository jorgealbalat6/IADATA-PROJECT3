import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import wkt
import warnings
import matplotlib.pyplot as plt
import seaborn as sns

import libpysal as ps
from libpysal.weights import Queen
from esda.moran import Moran, Moran_Local
from splot.esda import plot_moran, lisa_cluster
import skgstat as skg

# =============================================================================
# 1. AGREGACIÓN ESPACIAL Y MATRIZ DE PESOS
# =============================================================================

def prepare_spatial_data(gdf_points, gdf_polygons, vars_to_agg):
    if isinstance(vars_to_agg, str):
        vars_to_agg = [vars_to_agg]

    points = gdf_points.copy()
    polygons = gdf_polygons.copy()

    points['key_spatial'] = points['neighbourhood_cleansed'].astype(str).str.lower().str.strip()
    polygons['key_spatial'] = polygons['nom_barri'].astype(str).str.lower().str.strip()

    agg_dict = {v: 'mean' for v in vars_to_agg if v in points.columns}
    df_agg = points.groupby('key_spatial').agg(agg_dict).reset_index()

    gdf_result = polygons.merge(df_agg, on='key_spatial', how='inner')

    if not isinstance(gdf_result, gpd.GeoDataFrame):
        gdf_result = gpd.GeoDataFrame(gdf_result, geometry='geometry', crs=polygons.crs)
    
    print(f"✅ Éxito: Se han vinculado {len(gdf_result)} barrios.")
    return gdf_result

# =============================================================================
# 2. AUTOCORRELACIÓN ESPACIAL GLOBAL Y LOCAL (MORAN & LISA)
# =============================================================================

def run_spatial_autocorrelation(gdf_spatial: gpd.GeoDataFrame, col):
    """
    Calcula el Índice de Moran Global y los Clústeres LISA (Hotspots).
    """
    print("\n" + "="*60)
    print(f"🗺️ ANÁLISIS DE AUTOCORRELACIÓN ESPACIAL: {col}")
    print("="*60)
    
    # Limpiamos nulos para el cálculo
    gdf_clean = gdf_spatial.dropna(subset=[col, 'geometry'])
    
    # Matriz de Pesos Espaciales (Queen contiguity: vecinos que comparten bordes o vértices)
    w = Queen.from_dataframe(gdf_clean)
    w.transform = 'r' # Estandarización por filas (necesario para Moran)
    
    y = gdf_clean[col].values
    
    # 1. MORAN GLOBAL
    moran = Moran(y, w)
    print(f"🔹 Índice de Moran Global (I): {moran.I:.4f}")
    print(f"🔹 p-valor: {moran.p_sim:.4f}")
    
    if moran.p_sim < 0.05 and moran.I > 0:
        print("   ✅ Conclusión: Existe CLUSTERING ESPACIAL significativo (Valores similares se atraen).")
    elif moran.p_sim < 0.05 and moran.I < 0:
        print("   ✅ Conclusión: Existe DISPERSIÓN ESPACIAL (Patrón de tablero de ajedrez).")
    else:
        print("   ⚠️ Conclusión: Distribución ALEATORIA (El espacio no influye).")

    # 2. VISUALIZACIÓN LISA (Local Moran)
    moran_loc = Moran_Local(y, w)
    
    fig, ax = plt.subplots(figsize=(10, 8))
    # lisa_cluster mapea los Hotspots (Alto-Alto) y Coldspots (Bajo-Bajo)
    lisa_cluster(moran_loc, gdf_clean, p=0.05, ax=ax, legend_kwds={'loc': 'upper left'})
    
    plt.title(f"Clústeres LISA (Hotspots / Coldspots): {col}", fontsize=14)
    plt.axis('off')
    plt.tight_layout()
    plt.show()

# =============================================================================
# 3. SEMIVARIOGRAMA (CONTINUIDAD ESPACIAL)
# =============================================================================

def plot_variogram(gdf_points: gpd.GeoDataFrame, col: str, n_samples: int = 5000):
    """
    Calcula y grafica el semivariograma empírico sobre una muestra de puntos.
    Mide la "distancia de influencia" de una variable.
    """
    print(f"\n[+] Calculando Semivariograma para {col} (Muestra: {n_samples} puntos)...")
    
    # Usamos EPSG:3857 para medir en metros reales
    gdf_metros = gdf_points.to_crs("EPSG:3857").dropna(subset=[col])
    
    if len(gdf_metros) > n_samples:
        sample = gdf_metros.sample(n=n_samples, random_state=42)
    else:
        sample = gdf_metros
        
    coords = np.column_stack((sample.geometry.x, sample.geometry.y))
    values = sample[col].values
    
    # Modelo de variograma exponencial (muy común en datos espaciales)
    V = skg.Variogram(coords, values, model='exponential', n_lags=15)
    
    fig = V.plot()
    # fig.set_size_inches(10, 6)
    plt.title(f"Semivariograma Empírico: {col} (En Metros)", fontsize=14)
    plt.show()
    
    rango = V.parameters[0]
    print(f"🔹 Rango estimado (Distancia de influencia): ~{int(rango)} metros.")