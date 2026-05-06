import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
import statsmodels.api as sm
import warnings
import geopandas as gpd
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.feature_selection import mutual_info_classif
from sklearn.preprocessing import LabelEncoder

def plot_correlation_matrices(df: pd.DataFrame, num_cols: list, target: str = 'is_occupied'):
    cols_to_corr = num_cols + [target]
    # Forzamos conversión a float para evitar conflictos con Int64 de Pandas
    df_clean = df[cols_to_corr].dropna().astype('float64')

    fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    corr_pearson = df_clean.corr(method='pearson')
    mask_pearson = np.triu(np.ones_like(corr_pearson, dtype=bool))
    sns.heatmap(corr_pearson, mask=mask_pearson, annot=True, fmt=".2f", 
                cmap="coolwarm", center=0, ax=axes[0], square=True, 
                linewidths=.5, cbar_kws={"shrink": .8})
    axes[0].set_title('Matriz de Correlación: Pearson (Lineal)', fontsize=14)

    corr_spearman = df_clean.corr(method='spearman')
    mask_spearman = np.triu(np.ones_like(corr_spearman, dtype=bool))
    sns.heatmap(corr_spearman, mask=mask_spearman, annot=True, fmt=".2f", 
                cmap="coolwarm", center=0, ax=axes[1], square=True, 
                linewidths=.5, cbar_kws={"shrink": .8})
    axes[1].set_title('Matriz de Correlación: Spearman (Monótona)', fontsize=14)

    plt.tight_layout()
    plt.show()

def calculate_vif(df: pd.DataFrame, num_cols: list):
    
    X = df[num_cols].dropna().astype('float64')
    X = sm.add_constant(X)
    
    vif_data = pd.DataFrame()
    vif_data["Variable"] = X.columns
    vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
    
    vif_data = vif_data[vif_data['Variable'] != 'const'].sort_values(by="VIF", ascending=False)
    
    print("\n" + "="*50)
    print("FACTOR DE INFLACIÓN DE LA VARIANZA (VIF)")
    print("="*50)
    print(vif_data.to_string(index=False))
    print("-" * 50)
    print("Regla: VIF > 5 (Precaución), VIF > 10 (Multicolinealidad severa).")


def compare_groups_kruskal(df: pd.DataFrame, num_cols: list, target: str = 'is_occupied'):
    print("\n" + "="*70)
    print("COMPARACIÓN DE DISTRIBUCIONES (TEST DE KRUSKAL-WALLIS)")
    print("="*70)
    
    for col in num_cols:
        grupos = [grupo.dropna().astype('float64') for nombre, grupo in df.groupby(target)[col]]
        
        # Validación de seguridad: Kruskal-Wallis necesita al menos 2 grupos
        if len(grupos) < 2:
            print(f"Variable: {col:<20} | ⚠️ Error: Menos de 2 grupos encontrados.")
            continue
            
        # El asterisco (*) desempaqueta la lista de grupos para pasarlos como argumentos separados
        stat, p_val = stats.kruskal(*grupos)
        
        significativo = "✅ SÍ" if p_val < 0.05 else "❌ NO"
        
        # Formateamos la salida para mostrar el estadístico H y el p-valor
        print(f"Var: {col:<22} | Estadístico (H): {stat:>9.2f} | p-valor: {p_val:.4e} | Difieren: {significativo}")

def analyze_categorical_associations(df: pd.DataFrame, cat_cols: list, target: str = 'is_occupied'):
    print("\n" + "="*50)
    print("ASOCIACIÓN CATEGÓRICA (CHI2 Y V DE CRAMER)")
    print("="*50)

    for col in cat_cols:
        contingency_table = pd.crosstab(df[col], df[target])
        chi2, p_val, dof, expected = stats.chi2_contingency(contingency_table)
        
        n = contingency_table.sum().sum()
        min_dim = min(contingency_table.shape) - 1
        cramer_v = np.sqrt(chi2 / (n * min_dim))
        
        significativo = "✅ SÍ" if p_val < 0.05 else "❌ NO"
        print(f"Var: {col:<18} | Chi2 p-valor: {p_val:.4e} | V-Cramer: {cramer_v:.4f} | Asociación: {significativo}")

def plot_lowess_analysis(df: pd.DataFrame, cont_cols: list, target: str = 'is_occupied', frac: float = 0.1):
    sample_size = min(10000, len(df))
    # Seleccionamos y convertimos a float64 para evitar el fallo en sm.nonparametric
    df_sample = df.sample(n=sample_size, random_state=42)[cont_cols + [target]].dropna().astype('float64')
    
    for col in cont_cols:
        print(f"[+] Calculando LOWESS para {col} (Muestra: {sample_size} registros)...")
        lowess = sm.nonparametric.lowess(df_sample[target], df_sample[col], frac=frac)
        
        plt.figure(figsize=(8, 5))
        sns.scatterplot(x=df_sample[col], y=df_sample[target], alpha=0.1, color='gray', label='Observaciones (0/1)')
        plt.plot(lowess[:, 0], lowess[:, 1], color='red', linewidth=2, label='Curva LOWESS')
        
        plt.title(f'Probabilidad de Ocupación vs {col} (LOWESS)', fontsize=14)
        plt.xlabel(col)
        plt.ylabel(f'Probabilidad ({target}=1)')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.show()


def plot_mutual_information(df: pd.DataFrame, features: list, target: str = 'is_occupied'):
    """
    Calcula y grafica la Información Mutua (MI) para medir la dependencia 
    lineal y no lineal entre las variables (numéricas/categóricas) y el target.
    """
    print("\n" + "="*60)
    print("🧠 INFORMACIÓN MUTUA (MUTUAL INFORMATION)")
    print("="*60)

    # 1. Limpieza de nulos estricta para el subset
    df_clean = df[features + [target]].dropna().copy()
    
    X = df_clean[features]
    y = df_clean[target].astype('int') # Aseguramos target binario limpio

    # 2. Preprocesado dinámico (MI requiere valores numéricos)
    for col in X.columns:
        if X[col].dtype == 'object' or X[col].dtype.name == 'category' or X[col].dtype == 'bool':
            # Codificamos las categóricas
            le = LabelEncoder()
            X[col] = le.fit_transform(X[col].astype(str))
        else:
            # Forzamos numéricas a float64 (Soluciona el problema de Int64 de Pandas)
            X[col] = X[col].astype('float64')

    # 3. Cálculo de la Información Mutua
    print(f"[+] Calculando MI para {len(features)} variables sobre {len(df_clean)} registros...")
    # random_state asegura reproducibilidad porque MI usa estimadores de densidad k-NN internamente
    mi_scores = mutual_info_classif(X, y, random_state=42)
    
    # 4. Estructuración y Ordenación
    mi_series = pd.Series(mi_scores, index=X.columns).sort_values(ascending=False)

    # 5. Visualización
    plt.figure(figsize=(10, 6))
    sns.barplot(x=mi_series.values, y=mi_series.index, palette='viridis')
    plt.title('Puntuación de Información Mutua (Importancia No Lineal)', fontsize=14)
    plt.xlabel('Mutual Information Score (Reducción de Incertidumbre)')
    plt.ylabel('Variables')
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

    # 6. Reporte en texto
    print(mi_series.to_frame(name='MI Score'))
    print("-" * 60)
    print("Regla: MI = 0 (Totalmente independientes).")
    print("       MI > 0 (Existe relación detectada, captura patrones NO lineales).")