# =============================================================================
# config.py — Configuración centralizada para Registry + Endpoint
# =============================================================================
# Edita SOLO este fichero para adaptar el pipeline a otro modelo o proyecto.
# Los notebooks y el serving/main.py leen todo desde aquí.
# =============================================================================

# ── GCP ───────────────────────────────────────────────────────────────────────
PROJECT_ID = "project3grupo1"
REGION     = "europe-west1"          # región de Vertex AI, Cloud Run, BigQuery
LOCATION   = "europe-west1"          # región de Artifact Registry (puede ser multi-región: "eu", "us")

# ── GCS — artefactos del modelo ───────────────────────────────────────────────
GCS_BUCKET   = f"{PROJECT_ID}-ml-models"   # bucket donde viven los .pkl / .json
MODEL_PREFIX = "occupancy"                 # carpeta dentro del bucket → occupancy/model.pkl

# Artefactos que el serving/main.py descargará al arrancar.
# Clave = nombre lógico, valor = blob path dentro del bucket.
# bq_config.json y feature_config.json se generan automáticamente desde este fichero.
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
# serving/main.py lee bq_config.json desde GCS; este dict es la fuente de verdad.
BQ_CONFIG = {
    "context_table": f"{PROJECT_ID}.dbt_transform.mart_inference_features",
    "date_column":   "date",
}

# ── Feature engineering ───────────────────────────────────────────────────────
# Cada entrada define UNA feature de salida.
# serving/main.py descarga feature_config.json y aplica las transformaciones.
#
# Campos obligatorios:
#   name       — nombre de la columna en el DataFrame final
#   source     — "listing" (payload) o "context" (BigQuery, se pasan automáticamente)
#   transform  — ver tabla de transforms disponibles abajo
#
# Transforms disponibles:
#   passthrough      → valor directo del campo (field)
#   log1p            → np.log1p(field)
#   bool_int         → int(bool(field))
#   gt_zero          → 1 si field > 0 else 0
#   fillna           → field si no es None/NaN, si no fill_value
#   ratio            → numerator / denominator   (ambos son campos del payload)
#   price_vs_neigh   → listing_price / neigh_mean_price[neighbourhood_field]
#
# Campos opcionales según transform:
#   field            — campo del payload a leer (por defecto = name)
#   fill_value       — valor de relleno para "fillna"
#   numerator        — campo numerador para "ratio"
#   denominator      — campo denominador para "ratio"
#   neighbourhood_field — campo del barrio para "price_vs_neigh"

FEATURE_CONFIG = [
    # ── Features categóricas (passthrough) ────────────────────────────────────
    {
        "name":      "neighbourhood_cleansed",
        "source":    "listing",
        "field":     "neighbourhood_cleansed",
        "transform": "passthrough",
    },
    {
        "name":      "room_type",
        "source":    "listing",
        "field":     "room_type",
        "transform": "passthrough",
    },
    {
        "name":      "accommodates",
        "source":    "listing",
        "field":     "accommodates",
        "transform": "passthrough",
    },
    # ── Features de precio ────────────────────────────────────────────────────
    {
        "name":      "log_price",
        "source":    "listing",
        "field":     "listing_price",
        "transform": "log1p",
    },
    {
        "name":        "price_per_person",
        "source":      "listing",
        "transform":   "ratio",
        "numerator":   "listing_price",
        "denominator": "accommodates",
    },
    {
        "name":               "price_vs_neigh_mean",
        "source":             "listing",
        "transform":          "price_vs_neigh",
        "field":              "listing_price",
        "neighbourhood_field": "neighbourhood_cleansed",
    },
    # ── Features de estancia ──────────────────────────────────────────────────
    {
        "name":      "log_min_nights",
        "source":    "listing",
        "field":     "minimum_nights",
        "transform": "log1p",
    },
    # ── Features de reviews ───────────────────────────────────────────────────
    {
        "name":      "log_reviews",
        "source":    "listing",
        "field":     "number_of_reviews",
        "transform": "log1p",
    },
    {
        "name":       "review_scores_rating",
        "source":     "listing",
        "field":      "review_scores_rating",
        "transform":  "fillna",
        "fill_value": 0,
    },
    {
        "name":      "has_reviews",
        "source":    "listing",
        "field":     "number_of_reviews",
        "transform": "gt_zero",
    },
    # ── Features de disponibilidad ────────────────────────────────────────────
    {
        "name":      "instant_bookable",
        "source":    "listing",
        "field":     "instant_bookable",
        "transform": "bool_int",
    },
    # ── Features de contexto (BigQuery) ──────────────────────────────────────
    # Las columnas "context" se añaden automáticamente desde la tabla BQ.
    # Solo necesitas declararlas si quieres aplicarles algún transform adicional.
    # Si no las declaras aquí se incluyen tal cual (passthrough implícito).
]

# ── Artifact Registry — imagen Docker ─────────────────────────────────────────
ARTIFACT_REPO = "app-repo"
SERVICE_NAME  = "occupancy-predictor"
IMAGE         = f"{LOCATION}-docker.pkg.dev/{PROJECT_ID}/{ARTIFACT_REPO}/{SERVICE_NAME}"

# ── Vertex AI Model Registry ─────────────────────────────────────────────────
MODEL_DISPLAY_NAME    = "occupancy-predictor"
ENDPOINT_DISPLAY_NAME = "occupancy-predictor-endpoint"

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
# serving/main.py solo necesita saber dónde están los artefactos en GCS.
# El resto (BQ_CONFIG, FEATURE_CONFIG) lo descarga como JSON al arrancar.
CONTAINER_ENV_VARS = {
    "PROJECT":      PROJECT_ID,
    "BUCKET":       GCS_BUCKET,
    "MODEL_PREFIX": MODEL_PREFIX,
}

# ── Carpeta local con los ficheros del contenedor ─────────────────────────────
SERVING_DIR = "serving"
