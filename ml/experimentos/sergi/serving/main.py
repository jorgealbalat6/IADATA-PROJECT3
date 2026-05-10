"""
serving/main.py — Servidor de predicción genérico para Vertex AI / Cloud Run
=============================================================================
Completamente data-driven: la tabla de BigQuery y las features se definen
en config.py y se suben a GCS como JSON. El contenedor los descarga al
arrancar — sin hardcodear nada específico del modelo aquí.

Variables de entorno esperadas (solo estas 3):
    PROJECT       — GCP project ID
    BUCKET        — bucket GCS donde viven los artefactos
    MODEL_PREFIX  — carpeta dentro del bucket (ej. "occupancy")

Artefactos que descarga de GCS al arrancar:
    {MODEL_PREFIX}/model.pkl
    {MODEL_PREFIX}/preprocessor.pkl
    {MODEL_PREFIX}/neigh_mean_price.json  (opcional)
    {MODEL_PREFIX}/bq_config.json         ← tabla y columna de fecha de BigQuery
    {MODEL_PREFIX}/feature_config.json    ← definición declarativa de features

Rutas expuestas:
    POST /predict  — recibe {"instances": [{...}]} → {"predictions": [{...}]}
    GET  /health   — health check
"""

import io
import json
import logging
import math
import os
import pickle

import pandas as pd
from flask import Flask, jsonify, request
from google.cloud import bigquery, storage

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config desde variables de entorno (las únicas 3 env vars necesarias) ──────
PROJECT      = os.environ.get("PROJECT", "project3grupo1")
BUCKET       = os.environ.get("BUCKET", "project3grupo1-ml-models")
MODEL_PREFIX = os.environ.get("MODEL_PREFIX", "occupancy")

app = Flask(__name__)


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _gcs_bucket():
    return storage.Client(project=PROJECT).bucket(BUCKET)


def _download_pkl(blob_path: str) -> object:
    logger.info(f"Descargando pkl: gs://{BUCKET}/{blob_path}")
    data = _gcs_bucket().blob(blob_path).download_as_bytes()
    return pickle.load(io.BytesIO(data))


def _download_json(blob_path: str) -> dict | list:
    logger.info(f"Descargando json: gs://{BUCKET}/{blob_path}")
    data = _gcs_bucket().blob(blob_path).download_as_bytes()
    return json.loads(data)


# ── Carga de artefactos al arrancar ───────────────────────────────────────────
logger.info("=== Cargando artefactos ===")

MODEL        = _download_pkl(f"{MODEL_PREFIX}/model.pkl")
PREPROCESSOR = _download_pkl(f"{MODEL_PREFIX}/preprocessor.pkl")

try:
    NEIGH_MEAN_PRICE: dict = _download_json(f"{MODEL_PREFIX}/neigh_mean_price.json")
    logger.info(f"  neigh_mean_price: {len(NEIGH_MEAN_PRICE)} barrios")
except Exception:
    NEIGH_MEAN_PRICE = {}
    logger.warning("  neigh_mean_price.json no encontrado — se usa dict vacío")

# Configuración de BigQuery — descargada de GCS, no hardcodeada
BQ_CONFIG: dict      = _download_json(f"{MODEL_PREFIX}/bq_config.json")
BQ_TABLE: str        = BQ_CONFIG["context_table"]
BQ_DATE_COL: str     = BQ_CONFIG["date_column"]

# Definición declarativa de features — descargada de GCS, no hardcodeada
FEATURE_CONFIG: list = _download_json(f"{MODEL_PREFIX}/feature_config.json")

BQ_CLIENT = bigquery.Client(project=PROJECT)

logger.info(f"  BQ table  : {BQ_TABLE}")
logger.info(f"  Features  : {len(FEATURE_CONFIG)} definidas")
logger.info("=== Artefactos listos ===")


# ── Motor de features ─────────────────────────────────────────────────────────

class FeatureBuilder:
    """
    Construye el DataFrame de features aplicando los transforms declarados
    en feature_config.json.

    Transforms disponibles
    ----------------------
    passthrough    → valor directo del campo (field)
    log1p          → math.log1p(value)
    bool_int       → int(bool(value))
    gt_zero        → 1 si value > 0 else 0
    fillna         → value si no es None/NaN, si no fill_value
    ratio          → numerator / denominator  (denominador mínimo 1)
    price_vs_neigh → listing_price / neigh_mean_price[neighbourhood_field]
    """

    _VALID_TRANSFORMS = {
        "passthrough", "log1p", "bool_int",
        "gt_zero", "fillna", "ratio", "price_vs_neigh",
    }

    def __init__(self, feature_config: list[dict], neigh_mean_price: dict):
        self._config           = feature_config
        self._neigh_mean_price = neigh_mean_price
        self._validate()

    def _validate(self):
        for f in self._config:
            t = f.get("transform")
            if t not in self._VALID_TRANSFORMS:
                raise ValueError(
                    f"Transform desconocido '{t}' en feature '{f.get('name')}'. "
                    f"Disponibles: {self._VALID_TRANSFORMS}"
                )

    def _apply_transform(self, feat_def: dict, instance: dict) -> object:
        """Aplica el transform de un feat_def sobre la instancia del payload."""
        transform = feat_def["transform"]
        field     = feat_def.get("field", feat_def["name"])
        raw       = instance.get(field)

        if transform == "passthrough":
            return raw

        if transform == "log1p":
            return math.log1p(float(raw or 0))

        if transform == "bool_int":
            return int(bool(raw))

        if transform == "gt_zero":
            return 1 if (raw or 0) > 0 else 0

        if transform == "fillna":
            is_missing = raw is None or (isinstance(raw, float) and math.isnan(raw))
            return feat_def.get("fill_value", 0) if is_missing else raw

        if transform == "ratio":
            num   = float(instance.get(feat_def["numerator"],   0) or 0)
            denom = float(instance.get(feat_def["denominator"], 1) or 1)
            return num / (denom if denom != 0 else 1)

        if transform == "price_vs_neigh":
            price  = float(instance.get(feat_def.get("field", "listing_price"), 0) or 0)
            barrio = instance.get(feat_def.get("neighbourhood_field", "neighbourhood_cleansed"), "")
            mean   = self._neigh_mean_price.get(barrio, price) or price or 1
            return price / mean

        raise ValueError(f"Transform no implementado: {transform}")

    def build(self, instance: dict, context_row: dict) -> pd.DataFrame:
        """
        Combina el contexto de fecha (BigQuery) con las features del listing.

        Parameters
        ----------
        instance    : dict — payload de la petición
        context_row : dict — fila de BigQuery sin la columna de fecha

        Returns
        -------
        pd.DataFrame — una fila lista para el preprocesador
        """
        # Las features de contexto van directas (ya vienen limpias de BQ)
        row: dict = dict(context_row)

        # Las features de listing se calculan según FEATURE_CONFIG
        for feat_def in self._config:
            if feat_def.get("source", "listing") == "listing":
                row[feat_def["name"]] = self._apply_transform(feat_def, instance)

        return pd.DataFrame([row])


# Instancia global — se crea una sola vez al arrancar el contenedor
_feature_builder = FeatureBuilder(FEATURE_CONFIG, NEIGH_MEAN_PRICE)


# ── BigQuery: contexto de la fecha ────────────────────────────────────────────

def get_context(date_str: str) -> dict | None:
    """Devuelve el contexto de la fecha desde BigQuery, o None si no existe."""
    query = f"""
        SELECT *
        FROM `{BQ_TABLE}`
        WHERE {BQ_DATE_COL} = '{date_str}'
        LIMIT 1
    """
    rows = list(BQ_CLIENT.query(query).result())
    if not rows:
        return None
    ctx = dict(rows[0])
    ctx.pop(BQ_DATE_COL, None)   # la fecha no es una feature del modelo
    return ctx


# ── Endpoints Flask ───────────────────────────────────────────────────────────

@app.route("/predict", methods=["POST"])
def predict():
    """
    Contrato Vertex AI Prediction:
        Recibe : {"instances": [{...}, ...]}
        Devuelve: {"predictions": [{...}, ...]}
    """
    body      = request.get_json(force=True) or {}
    instances = body.get("instances", [{}])

    predictions = []
    for instance in instances:
        fecha = instance.get("fecha") or instance.get("date")
        if not fecha:
            predictions.append({"error": "El campo 'fecha' es obligatorio"})
            continue

        context = get_context(fecha)
        if context is None:
            predictions.append({
                "error": f"Sin datos de contexto para la fecha '{fecha}'"
            })
            continue

        try:
            X     = _feature_builder.build(instance, context)
            X_tr  = PREPROCESSOR.transform(X)
            proba = float(MODEL.predict_proba(X_tr)[0][1])
            predictions.append({
                "fecha":        fecha,
                "probabilidad": round(proba, 4),
            })
        except Exception as exc:
            logger.exception("Error en predicción")
            predictions.append({"error": str(exc)})

    return jsonify({"predictions": predictions})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":       "ok",
        "model_prefix": MODEL_PREFIX,
        "bq_table":     BQ_TABLE,
        "features":     len(FEATURE_CONFIG),
    })


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("AIP_HTTP_PORT", os.environ.get("PORT", 8080)))
    app.run(host="0.0.0.0", port=port, debug=False)
