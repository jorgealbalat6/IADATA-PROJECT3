"""
Pipeline Global de Preprocesamiento (Fase 1)
=============================================
Transformaciones seguras ejecutables sobre todo el dataset ANTES del split temporal.
Garantiza cero data leakage usando shift(1) en todas las variables derivadas del target.

Columnas esperadas del dataset de entrada (desde BigQuery + barrios):
- listing_id, date, is_occupied, neighbourhood_cleansed, listing_price
- day_of_week, month, is_weekend, is_holiday, days_to_next_holiday
- accommodates, bedrooms, beds, minimum_nights
- review_scores_rating, review_scores_cleanliness, review_scores_location, review_scores_value
- temp_max, temp_min, temp_mean, precipitation_mm
- num_events, num_concerts, num_sports, num_festivals, max_attendance, total_attendance
- room_type, instant_bookable
"""

import pandas as pd
import numpy as np


# =============================================================================
# 1. LIMPIEZA INICIAL
# =============================================================================

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina filas exactamente duplicadas. Primer paso del pipeline."""
    n_before = len(df)
    df_clean = df.drop_duplicates(keep='first')
    n_dropped = n_before - len(df_clean)
    print(f"[Duplicados] Eliminadas {n_dropped} filas ({n_dropped/n_before*100:.2f}%). "
          f"Retenidas: {len(df_clean)}")
    return df_clean


# =============================================================================
# 1.5. ELIMINAR VARIABLES REDUNDANTES
# =============================================================================

def remove_redundant_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina variables que pueden causar sobreajuste espacial/temporal.
    Se ejecuta al principio del Pipeline Global para limpiar el dataset.
    """
    df_clean = df.copy()
    
    # 1. Variables de Infraestructura y Coordenadas
    cols_to_drop = [
        'latitude', 'longitude',  # Sobreajuste espacial (micro-barrios)
        'geometry', 'key_spatial', 'nom_barri', # Residuos del cruce geográfico
        'year' # Memorización temporal. El modelo usará 'month' y 'lags'
    ]
    
    # Eliminar solo si existen para evitar errores
    cols_existentes = [c for c in cols_to_drop if c in df_clean.columns]
    df_clean.drop(columns=cols_existentes, inplace=True)
    
    if cols_existentes:
        print(f"[Redundantes] Columnas eliminadas: {cols_existentes}")
    
    return df_clean


# =============================================================================
# 2. FLAGS DE AUSENCIA (MNAR)
# =============================================================================

# Columnas con patrón MNAR (Missing Not At Random): la ausencia es informativa
MNAR_COLUMNS = [
    'review_scores_rating',
    'review_scores_cleanliness',
    'review_scores_location',
    'review_scores_value',
]


def create_missing_flags(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    """
    Crea variables indicadoras binarias (is_missing_*) para columnas con nulos informativos.
    Los valores nulos originales se preservan intactos para que cada pipeline
    posterior decida su estrategia de imputación.
    """
    if columns is None:
        columns = MNAR_COLUMNS

    df_out = df.copy()
    flags_created = []

    for col in columns:
        if col in df_out.columns:
            flag_name = f"is_missing_{col}"
            df_out[flag_name] = df_out[col].isna().astype(np.int8)
            flags_created.append(flag_name)

    print(f"[MNAR Flags] Creadas {len(flags_created)} variables indicadoras: {flags_created}")
    return df_out


# =============================================================================
# 3. LAGS A NIVEL INDIVIDUO (LISTING)
# =============================================================================

def create_individual_lags(df: pd.DataFrame, target: str = 'is_occupied',
                           max_lag: int = 14) -> pd.DataFrame:
    """
    Calcula lags del 1 al max_lag sobre el target agrupando por listing_id.
    Cada lag mira al pasado: lag_1 = valor de ayer, lag_7 = valor de hace 7 días.
    """
    df_out = df.sort_values(['listing_id', 'date']).copy()

    lag_cols = []
    for lag in range(2, max_lag + 1):
        col_name = f"{target}_lag_{lag}"
        df_out[col_name] = df_out.groupby('listing_id')[target].shift(lag)
        lag_cols.append(col_name)

    print(f"[Lags Individuales] Creados {len(lag_cols)} lags (2 a {max_lag}) "
          f"sobre '{target}' agrupados por listing_id.")
    return df_out


# =============================================================================
# 4. RACHAS DE OCUPACIÓN (CON SHIFT 1)
# =============================================================================

def create_occupancy_streaks(df: pd.DataFrame, target: str = 'is_occupied') -> pd.DataFrame:
    """
    Calcula días consecutivos que un listing lleva vacío u ocupado.
    Aplica shift(2) para que el conteo sea hasta t-1 y evitar data leakage.

    Lógica:
    - Se crea un marcador de cambio de estado (occupied -> vacant o viceversa).
    - Se agrupa por listing_id y por cada "bloque" de estado continuo.
    - Se cuenta la duración acumulada dentro de cada bloque.
    - Se aplica shift(2) al final para que el modelo solo vea rachas hasta ayer.
    """
    df_out = df.sort_values(['listing_id', 'date']).copy()

    # Detectar cambios de estado dentro de cada listing
    state_changed = df_out.groupby('listing_id')[target].transform(
        lambda x: x.ne(x.shift()).cumsum()
    )

    # Contar días consecutivos dentro de cada bloque de estado
    cumcount = df_out.groupby(['listing_id', state_changed]).cumcount() + 1

    # Separar rachas de ocupado y vacío
    df_out['streak_occupied_raw'] = np.where(df_out[target] == 1, cumcount, 0)
    df_out['streak_vacant_raw'] = np.where(df_out[target] == 0, cumcount, 0)

    # shift(2) para que el modelo vea la racha hasta t-2 (resiliencia ETL 48h)
    df_out['streak_occupied'] = df_out.groupby('listing_id')['streak_occupied_raw'].shift(2)
    df_out['streak_vacant'] = df_out.groupby('listing_id')['streak_vacant_raw'].shift(2)

    # Limpieza de columnas auxiliares
    df_out.drop(columns=['streak_occupied_raw', 'streak_vacant_raw'], inplace=True)

    print("[Rachas] Creadas 'streak_occupied' y 'streak_vacant' con shift(2).")
    return df_out


# =============================================================================
# 5. VARIABLES ESPACIO-TEMPORALES (NIVEL BARRIO)
# =============================================================================

def create_neighbourhood_features(df: pd.DataFrame,
                                  target: str = 'is_occupied',
                                  space_col: str = 'neighbourhood_cleansed',
                                  price_col: str = 'listing_price') -> pd.DataFrame:
    """
    Variables agregadas a nivel de barrio y día:
    1. Media de ocupación del barrio en el día anterior.
    2. Media móvil a 7 días de la ocupación del barrio (shift 1 incluido).
    3. Ratio de precio del listing respecto a la mediana del barrio (todos los pisos listados).
    """
    df_out = df.sort_values([space_col, 'date']).copy()

    # --- 5a. Ocupación media del barrio por día ---
    daily_occ = df_out.groupby([space_col, 'date'])[target].mean().rename('occ_barrio_dia')
    daily_occ = daily_occ.reset_index()

    # shift(2): valor de hace 2 días (resiliencia ETL 48h)
    daily_occ['occ_barrio_ayer'] = daily_occ.groupby(space_col)['occ_barrio_dia'].shift(2)

    # Media móvil 7 días con shift(2): ventana de t-8 a t-2
    daily_occ['occ_barrio_ma7'] = daily_occ.groupby(space_col)['occ_barrio_dia'].transform(
        lambda x: x.shift(2).rolling(window=7, min_periods=1).mean()
    )

    # Merge de vuelta al dataset (left join por barrio + fecha)
    df_out = df_out.merge(
        daily_occ[[space_col, 'date', 'occ_barrio_ayer', 'occ_barrio_ma7']],
        on=[space_col, 'date'],
        how='left'
    )

    # --- 5b. Ratio de precio vs mediana del barrio ---
    # La mediana se calcula con TODOS los pisos listados (estén ocupados o no)
    median_price = df_out.groupby([space_col, 'date'])[price_col].median().rename('precio_mediano_barrio')
    median_price = median_price.reset_index()

    df_out = df_out.merge(median_price, on=[space_col, 'date'], how='left')

    # Ratio: precio individual / precio mediano del barrio
    df_out['price_ratio_barrio'] = np.where(
        df_out['precio_mediano_barrio'] > 0,
        df_out[price_col] / df_out['precio_mediano_barrio'],
        np.nan
    )

    # Limpieza de la columna auxiliar de mediana
    df_out.drop(columns=['precio_mediano_barrio'], inplace=True)

    print("[Barrio Features] Creadas: 'occ_barrio_ayer', 'occ_barrio_ma7', 'price_ratio_barrio'.")
    return df_out


# =============================================================================
# 6. BINARIZACIÓN Y CASTEO FINAL
# =============================================================================

def binarize_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Binariza variables categóricas residuales (room_type, instant_bookable).
    No realiza casteos de tipos (float/category), delegando esto a los 
    pipelines específicos de cada modelo.
    """
    df_out = df.copy()
    
    # 1. si es casa entera, 0 si es habitación
    if 'room_type' in df_out.columns:
        df_out['is_entire_home'] = (df_out['room_type'] == 'Entire home/apt').astype(int)
        df_out.drop(columns=['room_type'], inplace=True)
    
    # Booleano a 0/1
    if 'instant_bookable' in df_out.columns:
        df_out['instant_bookable'] = df_out['instant_bookable'].astype(int)
        
    print("[Binarización] Variables residuales (room_type, instant_bookable) binarizadas.")
    return df_out


# =============================================================================
# ORQUESTADOR: PIPELINE GLOBAL COMPLETO
# =============================================================================

def run_global_pipeline(df: pd.DataFrame, max_lag: int = 14) -> pd.DataFrame:
    """
    Ejecuta secuencialmente todos los pasos del Pipeline Global (Fase 1).
    Devuelve el DataFrame enriquecido listo para el split temporal.

    Pasos:
    1. Eliminación de duplicados
    2. Flags de ausencia (MNAR)
    3. Lags individuales por listing (1 a max_lag)
    4. Rachas de ocupación con shift(1)
    5. Variables espacio-temporales a nivel barrio
    6. Variables cíclicas (seno/coseno del día de la semana)
    """
    print("=" * 60)
    print("🔧 PIPELINE GLOBAL (FASE 1)")
    print("=" * 60)

    n_initial = len(df)
    n_cols_initial = df.shape[1]

    # Paso 1: Limpieza
    df = remove_duplicates(df)

    # Paso 1.5: Redundantes
    df = remove_redundant_features(df)

    # Paso 2: Flags MNAR
    df = create_missing_flags(df)

    # Paso 3: Lags individuales
    df = create_individual_lags(df, max_lag=max_lag)

    # Paso 4: Rachas
    df = create_occupancy_streaks(df)

    # Paso 5: Features de barrio
    df = create_neighbourhood_features(df)

    # Paso 6: Binarización
    df = binarize_features(df)

    # Resumen
    n_new_cols = df.shape[1] - n_cols_initial
    print("\n" + "-" * 60)
    print(f"✅ Pipeline Global completado.")
    print(f"   Filas: {n_initial} -> {len(df)} | Columnas nuevas: +{n_new_cols}")
    print(f"   Shape final: {df.shape}")
    print("=" * 60)

    return df
