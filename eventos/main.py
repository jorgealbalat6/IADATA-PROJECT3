"""
Cloud Function: Ingesta de eventos Valencia (PredictHQ API) -> BigQuery

Busca eventos (conciertos, deportes, festivales, conferencias, etc.)
en un radio de 30km alrededor de Valencia.

Trigger: Cloud Scheduler via HTTP o invocación manual.

Endpoints:
    GET/POST /                                    -> eventos próximos 30 días
    GET/POST /?start_date=2025-06-01&end_date=2026-12-31  -> rango personalizado
    GET/POST /?mode=historical                    -> desde 2025-06-01 hasta hoy
    GET/POST /?force=true                         -> re-ingesta aunque ya existan
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
PREDICTHQ_TOKEN = os.environ.get("PREDICTHQ_TOKEN")

DATASET = "airbnb_features"
TABLE = "events"
TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE}"

# Valencia coordenadas + radio
LATITUDE = 39.4699
LONGITUDE = -0.3763
RADIUS = "30km"

# Categorías de eventos relevantes para turismo/precios Airbnb
CATEGORIES = [
    "concerts",
    "sports",
    "festivals",
    "conferences",
    "expos",
    "performing-arts",
    "community",
]

API_URL = "https://api.predicthq.com/v1/events/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# FUNCIONES
# ──────────────────────────────────────────────

def fetch_events(start_date: str, end_date: str) -> list[dict]:
    """
    Llama a PredictHQ API y devuelve todos los eventos paginados.
    """
    headers = {
        "Authorization": f"Bearer {PREDICTHQ_TOKEN}",
        "Accept": "application/json",
    }

    all_events = []
    offset = 0
    limit = 200  # máximo por página

    while True:
        params = {
            "within": f"{RADIUS}@{LATITUDE},{LONGITUDE}",
            "category": ",".join(CATEGORIES),
            "active.gte": start_date,
            "active.lte": end_date,
            "sort": "start",
            "limit": limit,
            "offset": offset,
        }

        logger.info(f"Fetching events: offset={offset}")
        response = requests.get(API_URL, headers=headers, params=params, timeout=60)
        response.raise_for_status()

        data = response.json()
        results = data.get("results", [])
        all_events.extend(results)

        logger.info(f"Página: {len(results)} eventos | Total acumulado: {len(all_events)}")

        # Comprobar si hay más páginas
        if data.get("next") is None or len(results) < limit:
            break

        offset += limit

    logger.info(f"Total eventos descargados: {len(all_events)}")
    return all_events


def transform_events(events: list[dict]) -> pd.DataFrame:
    """Transforma los eventos de PredictHQ a DataFrame."""
    rows = []

    for e in events:
        location = e.get("location", [])
        # PredictHQ devuelve [lon, lat]
        lon = location[0] if len(location) > 0 else None
        lat = location[1] if len(location) > 1 else None

        rows.append({
            "event_id": e.get("id"),
            "title": e.get("title"),
            "category": e.get("category"),
            "start_date": e.get("start", "")[:10],  # YYYY-MM-DD
            "end_date": e.get("end", "")[:10] if e.get("end") else None,
            "duration_days": _calc_duration(e.get("start"), e.get("end")),
            "latitude": lat,
            "longitude": lon,
            "rank": e.get("rank"),
            "local_rank": e.get("local_rank"),
            "phq_attendance": e.get("phq_attendance"),
            "labels": ", ".join(e.get("labels", [])),
            "description": e.get("description", ""),
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date
        df["rank"] = pd.to_numeric(df["rank"], errors="coerce").astype("Int64")
        df["local_rank"] = pd.to_numeric(df["local_rank"], errors="coerce").astype("Int64")
        df["phq_attendance"] = pd.to_numeric(df["phq_attendance"], errors="coerce").astype("Int64")
        df["duration_days"] = pd.to_numeric(df["duration_days"], errors="coerce").astype("Int64")
        df["ingested_at"] = datetime.utcnow()

    logger.info(f"Transformado: {len(df)} eventos")
    return df


def _calc_duration(start: str, end: str) -> int:
    """Calcula duración en días."""
    try:
        if not start or not end:
            return 1
        s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
        days = (e - s).days
        return max(days, 1)
    except Exception:
        return 1


def delete_existing_dates(start_date: str, end_date: str):
    """Elimina eventos existentes en ese rango de fechas."""
    client = bigquery.Client(project=PROJECT_ID)

    query = f"""
        DELETE FROM `{TABLE_REF}`
        WHERE start_date >= '{start_date}'
        AND start_date <= '{end_date}'
    """

    try:
        job = client.query(query)
        job.result()
        logger.info(f"Borrados eventos existentes: {start_date} -> {end_date}")
    except Exception as e:
        logger.warning(f"Delete fallido (puede ser tabla vacía): {e}")


def load_to_bigquery(df: pd.DataFrame):
    """Carga el DataFrame a BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    logger.info(f"Cargando {len(df)} eventos a {TABLE_REF}...")
    job = client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config)
    job.result()
    logger.info(f"Cargado OK: {job.output_rows} filas en {TABLE_REF}")


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

@functions_framework.http
def ingest_events(request):
    """
    HTTP entry point.

    Query params:
        ?mode=upcoming                                     -> próximos 30 días (default)
        ?mode=historical                                   -> desde 2025-06-01 hasta hoy
        ?start_date=2025-06-01&end_date=2026-12-31         -> rango personalizado
    """
    mode = request.args.get("mode", "upcoming")
    custom_start = request.args.get("start_date")
    custom_end = request.args.get("end_date")

    if custom_start and custom_end:
        start_date = custom_start
        end_date = custom_end
    elif mode == "historical":
        start_date = "2025-06-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
    else:  # upcoming
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

    logger.info(f"=== Inicio ingesta eventos Valencia ===")
    logger.info(f"Modo: {mode} | Periodo: {start_date} -> {end_date}")

    try:
        events = fetch_events(start_date, end_date)
        df = transform_events(events)

        if df.empty:
            return ({"status": "No events found", "period": f"{start_date} -> {end_date}"}, 200)

        # Borrar eventos existentes en ese rango y re-insertar
        delete_existing_dates(start_date, end_date)
        load_to_bigquery(df)

        # Resumen por categoría
        category_counts = df["category"].value_counts().to_dict()

        result = {
            "period": f"{start_date} -> {end_date}",
            "total_events": len(df),
            "by_category": category_counts,
        }

    except Exception as e:
        error_msg = f"ERROR - {str(e)}"
        logger.error(error_msg, exc_info=True)
        return ({"error": error_msg}, 500)

    logger.info(f"=== Fin ingesta eventos: {result} ===")
    return (result, 200)