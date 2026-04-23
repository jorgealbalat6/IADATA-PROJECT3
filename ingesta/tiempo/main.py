"""
Cloud Function: Ingesta de meteorología Valencia (Open-Meteo API) -> BigQuery

Dos modos:
  1. Histórico: carga datos reales desde una fecha pasada hasta hoy.
  2. Forecast: carga previsión de los próximos 14 días (refresco diario).

Open-Meteo es 100% gratis, sin API key.

Trigger: Cloud Scheduler via HTTP o invocación manual.

Endpoints:
    GET/POST /                          -> forecast 14 días
    GET/POST /?mode=historical          -> histórico desde 2025-06-01 hasta ayer
    GET/POST /?mode=historical&start_date=2025-06-01&end_date=2025-12-31
    GET/POST /?mode=forecast            -> forecast 14 días (default)
"""

import logging
import os
from datetime import datetime, timedelta

import functions_framework
import pandas as pd
import requests
from google.cloud import bigquery

# ──────────────────────────────────────────────
# CONFIGURACION
# ──────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET = "airbnb_features"
TABLE = "weather"
TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE}"

# Valencia coordenadas
LATITUDE = 39.4699
LONGITUDE = -0.3763
TIMEZONE = "Europe/Madrid"

# Variables diarias a descargar
DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "windspeed_10m_max",
    "weathercode",
]

HISTORICAL_API = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API = "https://api.open-meteo.com/v1/forecast"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# FUNCIONES
# ──────────────────────────────────────────────

def fetch_historical(start_date: str, end_date: str) -> pd.DataFrame:
    """Descarga datos históricos reales de Open-Meteo."""
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(DAILY_VARS),
        "timezone": TIMEZONE,
    }

    logger.info(f"Fetching historical weather: {start_date} -> {end_date}")
    response = requests.get(HISTORICAL_API, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data["daily"])
    df["source"] = "historical"

    logger.info(f"Historical: {len(df)} días descargados")
    return df


def fetch_forecast() -> pd.DataFrame:
    """Descarga previsión de los próximos 14 días de Open-Meteo."""
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "daily": ",".join(DAILY_VARS),
        "timezone": TIMEZONE,
        "forecast_days": 14,
    }

    logger.info("Fetching forecast weather: próximos 14 días")
    response = requests.get(FORECAST_API, params=params, timeout=60)
    response.raise_for_status()

    data = response.json()
    df = pd.DataFrame(data["daily"])
    df["source"] = "forecast"

    logger.info(f"Forecast: {len(df)} días descargados")
    return df


def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas y ajusta tipos."""
    df = df.copy()

    # Renombrar columnas a nombres más limpios
    rename_map = {
        "time": "date",
        "temperature_2m_max": "temp_max",
        "temperature_2m_min": "temp_min",
        "temperature_2m_mean": "temp_mean",
        "precipitation_sum": "precipitation_mm",
        "rain_sum": "rain_mm",
        "windspeed_10m_max": "wind_max_kmh",
        "weathercode": "weather_code",
    }
    df = df.rename(columns=rename_map)

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ingested_at"] = datetime.utcnow()

    # Asegurar tipos numéricos
    for col in ["temp_max", "temp_min", "temp_mean", "precipitation_mm", "rain_mm", "wind_max_kmh"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "weather_code" in df.columns:
        df["weather_code"] = pd.to_numeric(df["weather_code"], errors="coerce").astype("Int64")

    logger.info(f"Transformado: {len(df)} filas")
    return df


def delete_existing_dates(dates: list, source: str):
    """
    Elimina filas existentes para esas fechas antes de re-insertar.
    - Si source='historical': borra TODAS las filas de esas fechas (historical + forecast)
    - Si source='forecast': borra solo forecast de esas fechas (no toca historical)
    """
    client = bigquery.Client(project=PROJECT_ID)

    dates_str = ", ".join([f"'{d}'" for d in dates])

    if source == "historical":
        # Histórico reemplaza todo: borra forecast y historical antiguo
        query = f"""
            DELETE FROM `{TABLE_REF}`
            WHERE date IN ({dates_str})
        """
    else:
        # Forecast solo reemplaza forecast anterior
        query = f"""
            DELETE FROM `{TABLE_REF}`
            WHERE date IN ({dates_str})
            AND source = 'forecast'
        """

    try:
        job = client.query(query)
        job.result()
        logger.info(f"Borradas filas existentes para {len(dates)} fechas (mode={source})")
    except Exception as e:
        # Si la tabla está vacía o no existe, no pasa nada
        logger.warning(f"Delete fallido (puede ser tabla vacía): {e}")


def load_to_bigquery(df: pd.DataFrame):
    """Carga el DataFrame a BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    logger.info(f"Cargando {len(df)} filas a {TABLE_REF}...")
    job = client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config)
    job.result()
    logger.info(f"Cargado OK: {job.output_rows} filas en {TABLE_REF}")


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

@functions_framework.http
def ingest_weather(request):
    """
    HTTP entry point.

    Query params:
        ?mode=forecast                  -> previsión 14 días (default)
        ?mode=historical                -> histórico desde 2025-06-01 hasta ayer
        ?mode=historical&start_date=2025-06-01&end_date=2025-12-31
    """
    mode = request.args.get("mode", "forecast")

    logger.info(f"=== Inicio ingesta meteorología Valencia | Modo: {mode} ===")

    try:
        if mode == "historical":
            end_date = request.args.get("end_date", (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))

            # Buscar la fecha más antigua de forecast en BigQuery
            start_date = request.args.get("start_date")
            if not start_date:
                client = bigquery.Client(project=PROJECT_ID)
                query = f"""
                    SELECT MIN(date) as min_date
                    FROM `{TABLE_REF}`
                    WHERE source = 'forecast'
                    AND date <= '{end_date}'
                """
                try:
                    result = client.query(query).result()
                    for row in result:
                        if row.min_date:
                            start_date = row.min_date.strftime("%Y-%m-%d")
                except Exception:
                    pass

                if not start_date:
                    return ({"status": "No hay forecasts pendientes de convertir a historical"}, 200)

            df = fetch_historical(start_date, end_date)
            df = transform_weather(df)

            # Borrar todo (forecast + historical) de esas fechas y meter el dato real
            delete_existing_dates(df["date"].tolist(), "historical")
            load_to_bigquery(df)

            result = {
                "mode": "historical",
                "period": f"{start_date} -> {end_date}",
                "rows_loaded": len(df),
            }

        else:  # forecast
            df = fetch_forecast()
            df = transform_weather(df)

            # Borrar forecast anterior para esas fechas
            delete_existing_dates(df["date"].tolist(), "forecast")
            load_to_bigquery(df)

            result = {
                "mode": "forecast",
                "period": f"{df['date'].min()} -> {df['date'].max()}",
                "rows_loaded": len(df),
            }

    except Exception as e:
        error_msg = f"ERROR - {str(e)}"
        logger.error(error_msg, exc_info=True)
        return ({"error": error_msg}, 500)

    logger.info(f"=== Fin ingesta meteo: {result} ===")
    return (result, 200)