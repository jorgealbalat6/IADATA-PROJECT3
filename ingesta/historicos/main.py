"""
Cloud Function: Ingesta de Inside Airbnb Valencia -> BigQuery

Solo ingesta listings que tienen precio en TODOS los snapshots.
Calendar y reviews se filtran a esos mismos listings.

Trigger: Cloud Scheduler via HTTP o invocacion manual.

Endpoints:
    GET/POST /             -> ingesta todos los snapshots
    GET/POST /?force=true  -> re-ingesta aunque ya existan
"""

import gzip
import io
import logging
import os

import functions_framework
import pandas as pd
import requests
from google.cloud import bigquery

# ──────────────────────────────────────────────
# CONFIGURACION
# ──────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_RAW = "airbnb_raw"
BASE_URL = "https://data.insideairbnb.com/spain/catalonia/barcelona"

SNAPSHOT_DATES = [
    #"2025-12-14",
    "2025-09-14",
    "2025-06-12",
]

LISTING_COLS = [
    "id", "name", "host_id", "host_name",
    "neighbourhood_cleansed", "latitude", "longitude",
    "room_type", "accommodates", "bedrooms", "bathrooms_text",
    "beds", "amenities", "price",
    "minimum_nights", "maximum_nights",
    "number_of_reviews", "review_scores_rating",
    "review_scores_cleanliness", "review_scores_location",
    "review_scores_value", "instant_bookable",
]

CALENDAR_COLS = [
    "listing_id", "date", "available",
    "price", "adjusted_price",
    "minimum_nights", "maximum_nights",
]

REVIEW_COLS = [
    "listing_id", "id", "date",
    "reviewer_id", "reviewer_name", "comments",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# FUNCIONES
# ──────────────────────────────────────────────
def download_gz_to_df(url: str, columnas: list[str]) -> pd.DataFrame:
    """Descarga un .csv.gz y devuelve un DataFrame."""
    logger.info(f"Descargando: {url}")
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    compressed = io.BytesIO(response.content)
    with gzip.open(compressed, "rb") as f:
        df = pd.read_csv(f, low_memory=False)

    size_mb = len(response.content) / (1024 * 1024)
    logger.info(f"Descargado: {size_mb:.2f} MB | {len(df):,} filas | {len(df.columns)} columnas")
    logger.info(f"Columnas del CSV: {list(df.columns)}")
    logger.info(f"Columnas pedidas: {columnas}")
    
    cols_disponibles = [c for c in columnas if c in df.columns]
    logger.info(f"Columnas encontradas: {cols_disponibles}")
    
    return df[cols_disponibles]
# def download_gz_to_df(url: str, columnas: list[str]) -> pd.DataFrame:
#     """Descarga un .csv.gz y devuelve un DataFrame."""
#     logger.info(f"Descargando: {url}")
#     response = requests.get(url, timeout=300)
#     response.raise_for_status()

#     compressed = io.BytesIO(response.content)
#     with gzip.open(compressed, "rb") as f:
#         df = pd.read_csv(f, low_memory=False)

#     size_mb = len(response.content) / (1024 * 1024)
#     logger.info(f"Descargado: {size_mb:.2f} MB | {len(df):,} filas | {len(df.columns)} columnas")

#     cols_disponibles = [c for c in columnas if c in df.columns]
#     return df[cols_disponibles]


def find_consistent_ids(snapshots: list[str]) -> set:
    """
    Descarga los listings de todos los snapshots y devuelve
    solo los IDs que tienen precio en TODOS ellos.
    """
    ids_por_snapshot = []

    for fecha in snapshots:
        url = f"{BASE_URL}/{fecha}/data/listings.csv.gz"
        df = download_gz_to_df(url, ["id", "price"])

        # Filtrar los que tienen precio
        df = df[df["price"].notna() & (df["price"] != "")]
        ids_con_precio = set(df["id"].dropna().astype(int).tolist())

        logger.info(f"Snapshot {fecha}: {len(ids_con_precio):,} listings con precio")
        ids_por_snapshot.append(ids_con_precio)

    # Interseccion: solo IDs presentes con precio en TODOS los snapshots
    consistent_ids = ids_por_snapshot[0]
    for s in ids_por_snapshot[1:]:
        consistent_ids = consistent_ids.intersection(s)

    logger.info(f"Listings consistentes (precio en todos los snapshots): {len(consistent_ids):,}")
    return consistent_ids


def transform_listings(df: pd.DataFrame, snapshot_date: str, valid_ids: set) -> pd.DataFrame:
    """Limpia listings y filtra solo los IDs consistentes."""
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()

    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df = df[df["id"].isin(valid_ids)]
    logger.info(f"Listings tras filtro consistente: {len(df):,}")

    # Convertir price de "$1,200.00" a 1200.0
    if "price" in df.columns:
        df["price"] = (
            df["price"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
        )
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Convertir instant_bookable de "t"/"f" a boolean
    if "instant_bookable" in df.columns:
        df["instant_bookable"] = df["instant_bookable"].map({"t": True, "f": False})

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

    df = df.dropna(subset=["id"])
    return df


def transform_calendar(df: pd.DataFrame, snapshot_date: str, valid_ids: set) -> pd.DataFrame:
    """Limpia calendar y filtra solo listings consistentes."""
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()

    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["available"] = df["available"].map({"t": True, "f": False})

    # Convertir price y adjusted_price de "$1,200.00" a 1200.0
    for col in ["price", "adjusted_price"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["minimum_nights"] = pd.to_numeric(df["minimum_nights"], errors="coerce").astype("Int64")
    df["maximum_nights"] = pd.to_numeric(df["maximum_nights"], errors="coerce").astype("Int64")

    df = df.dropna(subset=["listing_id", "date"])

    before = len(df)
    df = df[df["listing_id"].isin(valid_ids)]
    logger.info(f"Calendar filtrado: {len(df):,} filas (de {before:,})")
    logger.info(f"Calendar pre-filtro: {len(df):,} filas")
    logger.info(f"Listing IDs en calendar: {df['listing_id'].nunique():,}")
    logger.info(f"Valid IDs: {len(valid_ids):,}")
    logger.info(f"Interseccion: {len(set(df['listing_id'].dropna().astype(int).tolist()).intersection(valid_ids)):,}")

    return df


def transform_reviews(df: pd.DataFrame, snapshot_date: str, valid_ids: set) -> pd.DataFrame:
    """Limpia reviews y filtra solo listings consistentes."""
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()

    df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce").astype("Int64")
    df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    df["reviewer_id"] = pd.to_numeric(df["reviewer_id"], errors="coerce").astype("Int64")
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    df = df.dropna(subset=["listing_id", "id", "date"])

    before = len(df)
    df = df[df["listing_id"].isin(valid_ids)]
    logger.info(f"Reviews filtrado: {len(df):,} filas (de {before:,})")

    return df


def load_to_bigquery(df: pd.DataFrame, table_ref: str, partition_field: str, clustering_fields: list[str] | None = None):
    """Carga un DataFrame a BigQuery con WRITE_APPEND."""
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(field=partition_field),
        clustering_fields=clustering_fields,
    )

    logger.info(f"Cargando {len(df):,} filas a {table_ref}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info(f"Cargado OK: {job.output_rows} filas en {table_ref}")


def check_already_ingested(tabla: str, snapshot_date: str) -> bool:
    """Comprueba si ya se ingesto este snapshot."""
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
        pass
    return False


TABLA_CONFIG = {
    "listings": {"partition": "snapshot_date", "clustering": ["neighbourhood_cleansed", "room_type"]},
    "calendar": {"partition": "date",          "clustering": ["listing_id"]},
}

ARCHIVOS = {
    "listings": {"ruta": "data/listings.csv.gz", "tabla": "listings", "columnas": LISTING_COLS, "transform": transform_listings},
    "calendar": {"ruta": "data/calendar.csv.gz", "tabla": "calendar", "columnas": CALENDAR_COLS, "transform": transform_calendar},
}


# ──────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────

@functions_framework.http
def ingest_airbnb(request):
    """
    HTTP entry point.

    Query params:
        ?force=true  -> re-ingesta aunque ya existan
    """
    force = request.args.get("force", "false").lower() == "true"

    logger.info(f"=== Inicio ingesta Inside Airbnb Barcelona ===")
    logger.info(f"Snapshots: {SNAPSHOT_DATES} | Force: {force}")

    # PASO 1: Encontrar listings con precio en TODOS los snapshots
    logger.info("--- PASO 1: Buscando listings consistentes ---")
    valid_ids = find_consistent_ids(SNAPSHOT_DATES)

    if not valid_ids:
        return ({"error": "No se encontraron listings con precio en todos los snapshots"}, 500)

    # PASO 2: Ingestar cada snapshot con solo los listings consistentes
    all_results = []
    has_errors = False

    for fecha in SNAPSHOT_DATES:
        logger.info(f"--- PASO 2: Procesando snapshot {fecha} ---")
        snapshot_results = {}
        snapshot_errors = []

        for nombre, config in ARCHIVOS.items():
            tabla = config["tabla"]
            table_ref = f"{PROJECT_ID}.{DATASET_RAW}.{tabla}"

            try:
                if not force and check_already_ingested(tabla, fecha):
                    msg = f"snapshot {fecha} ya ingestado, skipping"
                    logger.info(f"{nombre}: {msg}")
                    snapshot_results[nombre] = msg
                    continue

                url = f"{BASE_URL}/{fecha}/{config['ruta']}"
                df = download_gz_to_df(url, config["columnas"])

                # Transformar con filtro de IDs consistentes
                df = config["transform"](df, fecha, valid_ids)

                # Cargar a BigQuery
                tc = TABLA_CONFIG[nombre]
                load_to_bigquery(df, table_ref, tc["partition"], tc["clustering"])

                snapshot_results[nombre] = f"OK: {len(df):,} filas cargadas"

            except Exception as e:
                error_msg = f"ERROR - {str(e)}"
                logger.error(f"{nombre}: {error_msg}", exc_info=True)
                snapshot_errors.append(f"{nombre}: {error_msg}")
                snapshot_results[nombre] = error_msg

        result = {"snapshot_date": fecha, "results": snapshot_results, "errors": snapshot_errors}
        all_results.append(result)
        if snapshot_errors:
            has_errors = True

    response = {
        "city": "Barcelona",
        "consistent_listings": len(valid_ids),
        "snapshots_processed": len(SNAPSHOT_DATES),
        "results": all_results,
    }

    status_code = 200 if not has_errors else 207
    logger.info(f"=== Fin ingesta: {response} ===")
    return (response, status_code)