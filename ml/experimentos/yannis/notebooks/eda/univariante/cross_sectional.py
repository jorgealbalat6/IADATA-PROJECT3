import pandas as pd
import numpy as np
import geopandas as gpd
import warnings
import matplotlib.pyplot as plt
import seaborn as sns
import contextily as ctx
import math
from scipy.stats import skew, kurtosis

#------------------------------------------------------------------------------
# FUNCIONCES PARA ANALISIS DE ESTRUCTURA/DESCRIPCION DE:
#------------------------------------------------------------------------------

#____________________________________________________________________________
# VARIABLES CONTINUAS
#____________________________________________________________________________

def print_shape_statistics(df: pd.DataFrame, continuous_cols: list):
    """
    Imprime Skewness (Asimetría) y Kurtosis (Curtosis)
    """
    print("\n" + "="*60)
    print(" ESTADÍSTICOS DE SKEWNESS Y KURTOSIS)")
    print("="*60)
    
    stats_list = []
    for col in continuous_cols:
        if col in df.columns:
            data = df[col].dropna()
            if not data.empty:
                col_skew = skew(data)
                col_kurt = kurtosis(data)
                stats_list.append({
                    'Variable': col,
                    'Skewness': round(col_skew, 2),
                    'Kurtosis': round(col_kurt, 2)
                })
                
    stats_df = pd.DataFrame(stats_list).sort_values(by='Skewness', key=abs, ascending=False)
    print(stats_df.to_string(index=False))
    print("-" * 60)


def plot_continuous_distributions(df: pd.DataFrame, continuous_cols: list):
    """
    Genera histogramas con estimación de densidad (KDE) para variables continuas puras.
    """
    num_cols = len(continuous_cols)
    cols_plot = 3
    rows_plot = math.ceil(num_cols / cols_plot)
    
    fig, axes = plt.subplots(rows_plot, cols_plot, figsize=(15, 4 * rows_plot))
    if num_cols == 1: axes = [axes]
    elif num_cols > 1: axes = axes.flatten()
    
    for i, col in enumerate(continuous_cols):
        if col in df.columns:
            if col == 'listing_price' or col == 'precipitation_mm':
                upper_limit = df[col].quantile(0.95) #acotado a percentil 95 para visibilidad
                data_to_plot = df[df[col] <= upper_limit]
                title_suffix = ' (Hasta P95)'
            else:
                data_to_plot = df
                title_suffix = ''
                
            sns.histplot(data=data_to_plot, x=col, bins=50, kde=True, ax=axes[i], color='teal')
            axes[i].set_title(f'Distribución Continua: {col}{title_suffix}')
            axes[i].set_ylabel('Frecuencia Absoluta')
            
    for j in range(i + 1, len(axes)): axes[j].axis('off')
    plt.tight_layout()
    plt.show()

#____________________________________________________________________________
# VARIABLES DISCRETAS
#____________________________________________________________________________

def plot_discrete_distributions(df: pd.DataFrame, discrete_cols: list):
    """
    Genera gráficos de barras (countplots) para variables numéricas discretas.
    """
    num_cols = len(discrete_cols)
    cols_plot = 2
    rows_plot = math.ceil(num_cols / cols_plot)
    
    fig, axes = plt.subplots(rows_plot, cols_plot, figsize=(12, 4 * rows_plot))
    if num_cols == 1: axes = [axes]
    elif num_cols > 1: axes = axes.flatten()
    
    for i, col in enumerate(discrete_cols):
        if col in df.columns:
            upper_limit = df[col].quantile(0.99)
            data_to_plot = df[df[col] <= upper_limit]
            
            sns.countplot(data=data_to_plot, x=col, ax=axes[i], color='steelblue')
            axes[i].set_title(f'Distribución Discreta: {col} (Hasta P99)')
            axes[i].set_ylabel('Frecuencia Absoluta')
            axes[i].tick_params(axis='x', rotation=45)
            
    for j in range(i + 1, len(axes)): axes[j].axis('off')
    plt.tight_layout()
    plt.show()

#____________________________________________________________________________
# VARIABLES CATEGÓRICAS
#____________________________________________________________________________

def plot_categorical_proportions(df: pd.DataFrame, categorical_cols: list):
    """
    Countplot de categorías. Balance entre clases visible
    """
    num_cols = len(categorical_cols)
    if num_cols == 0:
        print("No se proporcionaron columnas para graficar.")
        return
        
    cols_plot = 2
    rows_plot = math.ceil(num_cols / cols_plot)
    
    # Creamos la figura y los ejes
    fig, axes = plt.subplots(rows_plot, cols_plot, figsize=(12, 4 * rows_plot))

    if num_cols == 1:
        axes_flat = [axes] if not isinstance(axes, np.ndarray) else axes.flatten()
    else:
        axes_flat = axes.flatten()
    
    for i, col in enumerate(categorical_cols):
        if col in df.columns:
            proportions = df[col].value_counts(normalize=True).mul(100)

            if len(proportions) > 15:
                top_15 = proportions.head(15)
                others_pct = proportions.iloc[15:].sum()
                proportions = pd.concat([top_15, pd.Series({'Otras': others_pct})])

            y_labels = proportions.index.astype(str)
            
            # Gráfico con Seaborn
            sns.barplot(
                x=proportions.values, 
                y=y_labels, 
                ax=axes_flat[i], 
                palette='viridis', 
                hue=y_labels, 
                legend=False
            )
            
            # Estética
            axes_flat[i].set_title(f'Proporción: {col} (%)', fontsize=12, fontweight='bold')
            axes_flat[i].set_xlabel('Porcentaje (%)')
            axes_flat[i].set_xlim(0, 115) 

            for p in axes_flat[i].patches:
                width = p.get_width()
                if pd.isna(width): continue
                axes_flat[i].text(
                    width + 0.5, 
                    p.get_y() + p.get_height()/2, 
                    f'{width:.1f}%', 
                    va='center', 
                    fontsize=9
                )
        else:
            axes_flat[i].text(0.5, 0.5, f"Columna '{col}'\nno encontrada", 
                            ha='center', va='center')
                
    for j in range(num_cols, len(axes_flat)): 
        axes_flat[j].axis('off')
        
    plt.tight_layout()
    plt.show()