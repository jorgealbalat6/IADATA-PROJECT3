"""
Cloud Function: Ingesta de Inside Airbnb Valencia → BigQuery
Trigger: Cloud Scheduler via HTTP (mensual)
"""

import gzip
import io
import logging
import os
from datetime import date

import functions_framework
import pandas as pd
import requests
from google.cloud import bigquery

# ──────────────────────────────────────────────
# CONFIGURACION
# ──────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_RAW = "airbnb_raw"
BASE_URL = "https://data.insideairbnb.com/spain/vc/valencia"

# Snapshot conocido — actualizar cuando Inside Airbnb publique uno nuevo
# o implementar deteccion automatica
SNAPSHOT_DATE = "2025-09-23"

ARCHIVOS = {
    "listings": {
        "ruta": "data/listings.csv.gz",
        "tabla": "listings",
        "columnas": [
            "id", "name", "host_id", "host_name",
            "neighbourhood_cleansed", "latitude", "longitude",
            "room_type", "accommodates", "bedrooms", "bathrooms_text",
            "beds", "amenities", "price",
            "minimum_nights", "maximum_nights",
            "number_of_reviews", "review_scores_rating",
            "review_scores_cleanliness", "review_scores_location",
            "review_scores_value", "instant_bookable",
        ],
    },
    "calendar": {
        "ruta": "data/calendar.csv.gz",
        "tabla": "calendar",
        "columnas": [
            "listing_id", "date", "available",
            "price", "adjusted_price",
            "minimum_nights", "maximum_nights",
        ],
    },
    "reviews": {
        "ruta": "data/reviews.csv.gz",
        "tabla": "reviews",
        "columnas": [
            "listing_id", "id", "date",
            "reviewer_id", "reviewer_name", "comments",
        ],
    },
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# FUNCIONES AUXILIARES
# ──────────────────────────────────────────────

def download_gz_to_df(url: str, columnas: list[str]) -> pd.DataFrame:
    """Descarga un .csv.gz y devuelve un DataFrame con solo las columnas necesarias."""
    logger.info(f"Descargando: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()

    compressed = io.BytesIO(response.content)
    with gzip.open(compressed, "rb") as f:
        df = pd.read_csv(f, low_memory=False)

    size_mb = len(response.content) / (1024 * 1024)
    logger.info(f"Descargado: {size_mb:.2f} MB | {len(df):,} filas | {len(df.columns)} columnas")

    # Filtrar solo las columnas que necesitamos (y que existan)
    cols_disponibles = [c for c in columnas if c in df.columns]
    cols_faltantes = set(columnas) - set(cols_disponibles)
    if cols_faltantes:
        logger.warning(f"Columnas no encontradas en el CSV: {cols_faltantes}")

    return df[cols_disponibles]


def transform_listings(df: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    """Limpia y transforma la tabla de listings."""
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()

    # Asegurar tipos
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["host_id"] = pd.to_numeric(df["host_id"], errors="coerce").astype("Int64")
    df["accommodates"] = pd.to_numeric(df["accommodates"], errors="coerce").astype("Int64")
    df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["beds"] = pd.to_numeric(df["beds"], errors="coerce")
    df["minimum_nights"] = pd.to_numeric(df["minimum_nights"], errors="coerce").astype("Int64")
    df["maximum_nights"] = pd.to_numeric(df["maximum_nights"], errors="coerce").astype("Int64")
    df["number_of_reviews"] = pd.to_numeric(df["number_of_reviews"], errors="coerce").astype("Int64")
    df["review_scores_rating"] = pd.to_numeric(df["review_scores_rating"], errors="coerce")
    df["review_scores_cleanliness"] = pd.to_numeric(df["review_scores_cleanliness"], errors="coerce")
    df["review_scores_location"] = pd.to_numeric(df["review_scores_location"], errors="coerce")
    df["review_scores_value"] = pd.to_numeric(df["review_scores_value"], errors="coerce")

    # Eliminar filas sin ID
    df = df.dropna(subset=["id"])

    return df


def transform_calendar(df: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    """Limpia y transforma la tabla de calendar."""
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()

    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["available"] = df["available"].map({"t": True, "f": False})
    df["minimum_nights"] = pd.to_numeric(df["minimum_nights"], errors="coerce").astype("Int64")
    df["maximum_nights"] = pd.to_numeric(df["maximum_nights"], errors="coerce").astype("Int64")

    df = df.dropna(subset=["listing_id", "date"])

    return df


def transform_reviews(df: pd.DataFrame, snapshot_date: str) -> pd.DataFrame:
    """Limpia y transforma la tabla de reviews."""
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()

    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["reviewer_id"] = pd.to_numeric(df["reviewer_id"], errors="coerce").astype("Int64")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df = df.dropna(subset=["listing_id", "id", "date"])

    return df


TRANSFORMERS = {
    "listings": transform_listings,
    "calendar": transform_calendar,
    "reviews": transform_reviews,
}


def load_to_bigquery(df: pd.DataFrame, table_ref: str, partition_field: str, cluster_fields: list = None):
    """Carga un DataFrame a BigQuery con WRITE_APPEND y clustering opcional."""
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            field=partition_field,
        )
    )
    
    # Añadimos clustering_fields a la configuración solo si nos lo han pasado
    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    logger.info(f"Cargando {len(df):,} filas a {table_ref}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Espera a que termine
    logger.info(f"Cargado OK: {job.output_rows} filas en {table_ref}")


def check_already_ingested(tabla: str, snapshot_date: str) -> bool:
    """Comprueba si ya se ingesto este snapshot para evitar duplicados."""
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_RAW}.{tabla}"

    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{table_ref}`
        WHERE snapshot_date = '{snapshot_date}'
        LIMIT 1
    """
    try:
        result = client.query(query).result()
        for row in result:
            if row.cnt > 0:
                return True
    except Exception:
        # Tabla vacia o no existe aun — continuar
        pass

    return False


# ──────────────────────────────────────────────
# ENTRY POINT — Cloud Function
# ──────────────────────────────────────────────

@functions_framework.http
def ingest_airbnb(request):
    """
    HTTP entry point para Cloud Function.
    Disparada por Cloud Scheduler (mensual).

    Query params opcionales:
        ?snapshot_date=2025-09-23   (override de la fecha)
        ?force=true                 (forzar re-ingesta)
    """
    # Leer parametros
    snapshot = request.args.get("snapshot_date", SNAPSHOT_DATE)
    force = request.args.get("force", "false").lower() == "true"

    logger.info(f"=== Inicio ingesta Inside Airbnb Valencia ===")
    logger.info(f"Snapshot: {snapshot} | Force: {force}")

    results = {}
    errors = []

    for nombre, config in ARCHIVOS.items():
        tabla = config["tabla"]
        table_ref = f"{PROJECT_ID}.{DATASET_RAW}.{tabla}"

        try:
            # 1. Verificar si ya existe
            if not force and check_already_ingested(tabla, snapshot):
                msg = f"{nombre}: snapshot {snapshot} ya ingestado, skipping"
                logger.info(msg)
                results[nombre] = msg
                continue

            # 2. Descargar
            url = f"{BASE_URL}/{snapshot}/{config['ruta']}"
            df = download_gz_to_df(url, config["columnas"])

            # 3. Transformar
            transformer = TRANSFORMERS[nombre]
            df = transformer(df, snapshot)

            # 4. Determinar campo de particion Y CLUSTERING
            partition_field = "snapshot_date" if nombre == "listings" else "date"
            
            # Asignamos el clustering solo a la tabla que lo necesita
            if nombre == "calendar":
                cluster_fields = ["listing_id"]
            elif nombre == "listings":
                cluster_fields = ["neighbourhood_cleansed", "room_type"]
            elif nombre == "reviews":
                cluster_fields = ["listing_id"]
            else:
                cluster_fields = None

            # 5. Cargar a BigQuery
            load_to_bigquery(df, table_ref, partition_field, cluster_fields)

            results[nombre] = f"OK: {len(df):,} filas cargadas"

        except Exception as e:
            error_msg = f"{nombre}: ERROR - {str(e)}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)
            results[nombre] = error_msg

    # Respuesta
    status_code = 200 if not errors else 207  # 207 = Multi-Status (partial success)
    response = {
        "snapshot_date": snapshot,
        "results": results,
        "errors": errors,
    }

    logger.info(f"=== Fin ingesta: {response} ===")
    return (response, status_code)