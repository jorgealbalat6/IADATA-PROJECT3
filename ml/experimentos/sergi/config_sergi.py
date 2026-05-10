# =============================================================================
# config_sergi.py — Configuración de PRUEBA para upload con sufijo _sergi
# =============================================================================
# COPIA DE config.py con sufijos _sergi en todos los recursos para aislar
# la prueba de producción.
#
# Diferencias respecto a config.py:
#   MODEL_PREFIX              occupancy          → occupancy_sergi
#   SERVICE_NAME              occupancy-predictor → occupancy-predictor-sergi
#   MODEL_DISPLAY_NAME        occupancy-predictor → occupancy-predictor-sergi
#   ENDPOINT_DISPLAY_NAME     occupancy-predictor-endpoint → occupancy-predictor-endpoint-sergi
#
# El bucket GCS (project3grupo1-ml-models) NO cambia → los artefactos de
# prueba se guardan en gs://project3grupo1-ml-models/occupancy_sergi/
#
# Uso desde el notebook:
#   import config_sergi as CFG
#   from importlib import import_module
#   uploader_mod = import_module("02_upload_artifacts")   # nombre con guión
#   # O simplemente importa el módulo con exec/importlib según el entorno.
# =============================================================================

# ── GCP ───────────────────────────────────────────────────────────────────────
PROJECT_ID = "project3grupo1"
REGION     = "europe-west1"          # región de Vertex AI, Cloud Run, BigQuery
LOCATION   = "europe-west1"          # región de Artifact Registry

# ── GCS — artefactos del modelo ───────────────────────────────────────────────
GCS_BUCKET   = f"{PROJECT_ID}-ml-models"   # mismo bucket que producción
MODEL_PREFIX = "occupancy_sergi"            # ← carpeta de prueba (sufijo _sergi)

# Artefactos que el serving/main.py descargará al arrancar.
ARTIFACTS = {
    "model":          f"{MODEL_PREFIX}/model.pkl",
    "preprocessor":   f"{MODEL_PREFIX}/preprocessor.pkl",
    # Lookup opcional: precio medio por barrio (puede estar vacío → {})
    "neigh_mean_price": f"{MODEL_PREFIX}/neigh_mean_price.json",
    # Configs generadas desde este fichero — NO editar las rutas
    "bq_config":      f"{MODEL_PREFIX}/bq_config.json",
    "feature_config": f"{MODEL_PREFIX}/feature_config.json",
}

# ── BigQuery — tabla de contexto ─────────────────────────────────────────────
BQ_CONFIG = {
    "context_table": f"{PROJECT_ID}.dbt_transform.mart_inference_features",
    "date_column":   "date",
}

# ── Feature engineering ───────────────────────────────────────────────────────
# Alineado con la nueva query directa a BQ raw (Experimento_3_sergi).
# Eliminadas: log_price, price_per_person, price_vs_neigh_mean, log_min_nights, log_reviews
# Eliminadas del listing: bedrooms, beds (no existen en la nueva query)
# Añadidas: temporales, meteorología, eventos
FEATURE_CONFIG = [
    # ── Temporales (precomputadas en la query) ────────────────────────────────
    {"name": "month",               "source": "calendar", "field": "month",               "transform": "passthrough"},
    {"name": "day_of_week",         "source": "calendar", "field": "day_of_week",         "transform": "passthrough"},
    {"name": "is_weekend",          "source": "calendar", "field": "is_weekend",          "transform": "passthrough"},
    {"name": "days_to_next_holiday","source": "calendar", "field": "days_to_next_holiday","transform": "passthrough"},
    {"name": "is_holiday",          "source": "calendar", "field": "is_holiday",          "transform": "passthrough"},
    # ── Listing — categóricas ─────────────────────────────────────────────────
    {"name": "neighbourhood_cleansed", "source": "listing", "field": "neighbourhood_cleansed", "transform": "passthrough"},
    {"name": "room_type",              "source": "listing", "field": "room_type",              "transform": "passthrough"},
    # ── Listing — numéricas ───────────────────────────────────────────────────
    {"name": "accommodates",      "source": "listing", "field": "accommodates",      "transform": "passthrough"},
    {"name": "listing_price",     "source": "listing", "field": "listing_price",     "transform": "passthrough"},
    {"name": "minimum_nights",    "source": "listing", "field": "minimum_nights",    "transform": "passthrough"},
    {"name": "number_of_reviews", "source": "listing", "field": "number_of_reviews", "transform": "passthrough"},
    {"name": "review_scores_rating", "source": "listing", "field": "review_scores_rating", "transform": "fillna", "fill_value": 0},
    {"name": "has_reviews",       "source": "listing", "field": "number_of_reviews",  "transform": "gt_zero"},
    {"name": "instant_bookable",  "source": "listing", "field": "instant_bookable",   "transform": "bool_int"},
    # ── Meteorología ──────────────────────────────────────────────────────────
    {"name": "temp_mean",        "source": "weather", "field": "temp_mean",        "transform": "passthrough"},
    {"name": "precipitation_mm", "source": "weather", "field": "precipitation_mm", "transform": "passthrough"},
    # ── Eventos ───────────────────────────────────────────────────────────────
    {"name": "num_sports",       "source": "events", "field": "num_sports",       "transform": "passthrough"},
    {"name": "num_festivals",    "source": "events", "field": "num_festivals",    "transform": "passthrough"},
    {"name": "total_attendance", "source": "events", "field": "total_attendance", "transform": "passthrough"},
]

# ── Artifact Registry — imagen Docker ─────────────────────────────────────────
ARTIFACT_REPO = "app-repo"
SERVICE_NAME  = "occupancy-predictor-sergi"   # ← sufijo _sergi
IMAGE         = f"{LOCATION}-docker.pkg.dev/{PROJECT_ID}/{ARTIFACT_REPO}/{SERVICE_NAME}"

# ── Vertex AI Model Registry ─────────────────────────────────────────────────
MODEL_DISPLAY_NAME    = "occupancy-predictor-sergi"           # ← sufijo _sergi
ENDPOINT_DISPLAY_NAME = "occupancy-predictor-endpoint-sergi"  # ← sufijo _sergi

SERVING_PREDICT_ROUTE = "/predict"
SERVING_HEALTH_ROUTE  = "/health"
SERVING_PORT          = 8080

# ── Vertex AI Endpoint — recursos de máquina ──────────────────────────────────
MACHINE_TYPE      = "n1-standard-2"
MIN_REPLICA_COUNT = 1
MAX_REPLICA_COUNT = 3

# ── Service Account ───────────────────────────────────────────────────────────
SA_NAME  = "app-sa"
SA_EMAIL = f"{SA_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"

# ── Entorno del contenedor ────────────────────────────────────────────────────
CONTAINER_ENV_VARS = {
    "PROJECT":      PROJECT_ID,
    "BUCKET":       GCS_BUCKET,
    "MODEL_PREFIX": MODEL_PREFIX,   # occupancy_sergi
}

# ── Carpeta local con los ficheros del contenedor ─────────────────────────────
SERVING_DIR = "serving"
