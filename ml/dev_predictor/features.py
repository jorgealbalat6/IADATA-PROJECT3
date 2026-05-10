"""
Motor de Datos para Inferencia (features.py)
=============================================
Responsabilidades:
  1. Consultas a BigQuery (historia del piso + barrio + contexto futuro).
  2. Cálculo del Golden Set (is_occupied_lag_2, streaks) — réplica de common_pipeline.
  3. Cálculo del Momentum dinámico del barrio (occ_barrio_ayer).
  4. Ensamblado del DataFrame de 1 fila para el Wrapper.
  5. Router Hot / Cold Start.
"""

import json
import pandas as pd
import numpy as np
from google.cloud import bigquery

# =============================================================================
# CONFIGURACIÓN DE TABLAS
# =============================================================================
PROJECT = "project3grupo1"
DATASET = "dbt_transform"
TABLE_HISTORY = f"{PROJECT}.{DATASET}.mart_occupancy_features"
TABLE_CONTEXT = f"{PROJECT}.{DATASET}.mart_inference_features"

HISTORY_WINDOW_DAYS = 60
MIN_DAYS_HOT_START = 14


# =============================================================================
# UTILIDADES
# =============================================================================

def load_neigh_mean_price(path: str) -> dict:
    """Carga el diccionario de precios medios de barrio desde un archivo JSON."""
    with open(path) as f:
        return json.load(f)


def load_neigh_mean_price_from_bytes(data: bytes) -> dict:
    """Carga el diccionario de precios desde bytes (descarga GCS)."""
    return json.loads(data)


# =============================================================================
# 1. CONSULTAS A BIGQUERY
# =============================================================================

def fetch_listing_history(
    listing_id: int,
    target_date: str,
    bq_client: bigquery.Client,
) -> pd.DataFrame:
    """
    Últimos 60 días de historia de ocupación de un piso.
    Devuelve columnas: date, is_occupied.
    """
    query = f"""
        SELECT date, is_occupied
        FROM `{TABLE_HISTORY}`
        WHERE listing_id = {listing_id}
          AND date < '{target_date}'
        ORDER BY date DESC
        LIMIT {HISTORY_WINDOW_DAYS}
    """
    return bq_client.query(query).to_dataframe()


def fetch_neighborhood_history(
    neighborhood: str,
    target_date: str,
    bq_client: bigquery.Client,
) -> pd.DataFrame:
    """
    Últimos 60 días de agregación de barrio.
    Calcula la media de ocupación y la mediana de precio por día.
    """
    query = f"""
        SELECT
            date,
            AVG(is_occupied) AS occ_barrio_raw,
            APPROX_QUANTILES(listing_price, 2)[OFFSET(1)] AS median_price_barrio
        FROM `{TABLE_HISTORY}`
        WHERE neighbourhood_cleansed = '{neighborhood}'
          AND date < '{target_date}'
        GROUP BY date
        ORDER BY date DESC
        LIMIT {HISTORY_WINDOW_DAYS}
    """
    return bq_client.query(query).to_dataframe()


def fetch_future_context(
    target_date: str,
    bq_client: bigquery.Client,
) -> dict:
    """
    Contexto de la fecha objetivo (clima, festivos, eventos).
    Devuelve un dict con los valores o {} si no hay datos.
    """
    query = f"""
        SELECT *
        FROM `{TABLE_CONTEXT}`
        WHERE date = '{target_date}'
        LIMIT 1
    """
    df = bq_client.query(query).to_dataframe()
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    row.pop("date", None)
    return row


# =============================================================================
# 2. CÁLCULO DEL GOLDEN SET (Hot Start)
#    Réplica exacta de common_pipeline.create_occupancy_streaks + shift(2)
# =============================================================================

def calculate_golden_set(history_df: pd.DataFrame) -> dict:
    """
    Calcula las únicas features temporales del piso que el modelo Hot Start usa:
      - is_occupied_lag_2  (shift 2 → resiliencia ETL 48h)
      - streak_occupied    (racha de ocupación con shift 2)
      - streak_vacant      (racha de vacío con shift 2)

    Parámetro
    ---------
    history_df : DataFrame con columnas [date, is_occupied], ordenado DESC desde BQ.
    """
    empty = {
        "is_occupied_lag_2": np.nan,
        "streak_occupied": np.nan,
        "streak_vacant": np.nan,
    }
    if history_df.empty or len(history_df) < 3:
        return empty

    # Ordenar cronológicamente (ascendente)
    df = history_df.sort_values("date", ascending=True).reset_index(drop=True)
    occ = df["is_occupied"]

    # --- is_occupied_lag_2: valor de hace 2 filas ---
    lag_2 = float(occ.iloc[-2])

    # --- Streaks (réplica de common_pipeline.create_occupancy_streaks) ---
    # Detectar cambios de estado
    state_changed = occ.ne(occ.shift()).cumsum()
    # Contar días consecutivos dentro de cada bloque
    cumcount = df.groupby(state_changed).cumcount() + 1
    # Separar rachas
    streak_occ_raw = np.where(occ == 1, cumcount, 0)
    streak_vac_raw = np.where(occ == 0, cumcount, 0)

    # shift(2): tomamos el valor de hace 2 posiciones (índice -3)
    streak_occupied = float(streak_occ_raw[-2])
    streak_vacant = float(streak_vac_raw[-2])

    return {
        "is_occupied_lag_2": lag_2,
        "streak_occupied": streak_occupied,
        "streak_vacant": streak_vacant,
    }


# =============================================================================
# 3. CÁLCULO DEL MOMENTUM DINÁMICO (Cold Start y Hot Start)
#    Réplica de common_pipeline.create_neighbourhood_features + shift(2)
# =============================================================================

def calculate_neighborhood_momentum(neigh_df: pd.DataFrame) -> dict:
    """
    Calcula el momentum del barrio a partir de los 60 días de historia agregada:
      - occ_barrio_ayer     : ocupación media del barrio hace 2 días (shift 2)
      - _median_price_barrio: mediana de precio del barrio (para price_ratio_barrio)
    """
    result = {"occ_barrio_ayer": np.nan, "_median_price_barrio": np.nan}

    if neigh_df.empty or len(neigh_df) < 3:
        return result

    df = neigh_df.sort_values("date", ascending=True).reset_index(drop=True)

    # occ_barrio_ayer = media de ocupación del barrio hace 2 días (shift 2)
    result["occ_barrio_ayer"] = float(df["occ_barrio_raw"].iloc[-2])

    # Mediana del precio del barrio (último día disponible)
    if "median_price_barrio" in df.columns:
        result["_median_price_barrio"] = float(df["median_price_barrio"].iloc[-1])

    return result


# =============================================================================
# 4. ENSAMBLADO DEL VECTOR DE INFERENCIA (Golden Set)
# =============================================================================

def assemble_inference_row(
    user_input: dict,
    context: dict,
    golden_set: dict,
    momentum: dict,
) -> pd.DataFrame:
    """
    Une las piezas en un DataFrame de 1 fila con las 15 variables
    que cubren los requisitos de AMBOS modelos (Hot y Cold).
    
    Cada Wrapper poda internamente lo que no necesita, pero el orden
    del resto debe ser el correcto.
    """
    row = {}

    # --- 1. Derivaciones rápidas ---
    target_dt = pd.to_datetime(user_input.get("fecha"))
    row["month"] = target_dt.month
    
    room_type = user_input.get("room_type", "")
    row["is_entire_home"] = 1 if room_type == "Entire home/apt" else 0

    # --- 2. Contexto y Base ---
    row["days_to_next_holiday"] = context.get("days_to_next_holiday", 30)
    row["neighbourhood_cleansed"] = user_input.get("neighbourhood_cleansed", "")
    row["accommodates"] = user_input.get("accommodates", 1)
    row["listing_price"] = user_input.get("listing_price", 0.0)
    row["minimum_nights"] = user_input.get("minimum_nights", 1)
    row["number_of_reviews"] = user_input.get("number_of_reviews", 0)
    row["review_scores_rating"] = user_input.get("review_scores_rating", np.nan)
    row["temp_mean"] = context.get("temp_mean", 20.0)

    # --- 3. Historia (Hot) ---
    row["is_occupied_lag_2"] = golden_set.get("is_occupied_lag_2", np.nan)
    row["streak_occupied"] = golden_set.get("streak_occupied", np.nan)
    row["streak_vacant"] = golden_set.get("streak_vacant", np.nan)

    # --- 4. Momentum (Cold/Universal) ---
    row["occ_barrio_ayer"] = momentum.get("occ_barrio_ayer", np.nan)
    
    median_price = momentum.get("_median_price_barrio", None)
    if median_price and median_price > 0:
        row["price_ratio_barrio"] = row["listing_price"] / median_price
    else:
        row["price_ratio_barrio"] = 1.0

    # Creamos el DataFrame
    df = pd.DataFrame([row])

    # --- Limpieza de tipos para XGBoost ---
    df = df.fillna(value=np.nan)
    numeric_cols = [
        'month', 'days_to_next_holiday', 'accommodates', 'listing_price', 
        'minimum_nights', 'number_of_reviews', 'review_scores_rating', 
        'temp_mean', 'is_occupied_lag_2', 'streak_occupied', 
        'streak_vacant', 'occ_barrio_ayer', 'price_ratio_barrio', 'is_entire_home'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    if 'neighbourhood_cleansed' in df.columns:
        df['neighbourhood_cleansed'] = df['neighbourhood_cleansed'].astype('category')

    # --- ORDEN MAESTRO (15 variables) ---
    # Este orden garantiza que al podar las basuras de cada modelo, 
    # el resultado sea el orden esperado por XGBoost en ambos casos.
    cols_order = [
        'month', 'days_to_next_holiday', 'neighbourhood_cleansed', 
        'accommodates', 'listing_price', 'minimum_nights', 
        'number_of_reviews', 'review_scores_rating', 'temp_mean', 
        'is_occupied_lag_2', 'streak_occupied', 'streak_vacant', 
        'occ_barrio_ayer', 'price_ratio_barrio', 'is_entire_home'
    ]
    
    df = df[[c for c in cols_order if c in df.columns]]
    return df




# =============================================================================
# 5. ROUTER: SELECCIÓN DE MODELO
# =============================================================================

def decide_model_type(
    listing_id: int | None,
    listing_history_df: pd.DataFrame,
) -> str:
    """
    Decide qué modelo usar:
      - 'hot'  : listing_id proporcionado y >= 14 días de historia.
      - 'cold' : sin listing_id, piso nuevo, o historia insuficiente.
    """
    if listing_id is None:
        return "cold"
    if listing_history_df.empty or len(listing_history_df) < MIN_DAYS_HOT_START:
        return "cold"
    return "hot"
