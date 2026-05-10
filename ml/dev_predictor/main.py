"""
Orquestador FastAPI: Airbnb Occupancy Predictor DEV
====================================================
Responsabilidades:
  1. Descarga de artefactos (.joblib) desde GCS al arrancar.
  2. Validación del JSON de entrada (PredictRequest).
  3. Coordinación del flujo: features.py → Wrapper → Respuesta.
  4. Devuelve 7 probabilidades (T+1 a T+7).
"""

import os
import sys
import joblib
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery, storage

# Asegurar que pre_procesado es importable para deserializar los .joblib
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import features

app = FastAPI(title="Airbnb Occupancy Predictor - DEV", version="2.0.0")

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
PROJECT = os.environ.get("PROJECT", "project3grupo1")
BUCKET = os.environ.get("BUCKET", "project3grupo1-ml-models")
MODEL_PREFIX = os.environ.get("MODEL_PREFIX", "occupancy_dev")

# =============================================================================
# CLIENTES GCP 
# =============================================================================
bq_client = bigquery.Client(project=PROJECT)
storage_client = storage.Client()

# =============================================================================
# ARTEFACTOS
# =============================================================================
wrapper_hot = None
wrapper_cold = None


def _download_from_gcs(blob_name: str, dest: str) -> str:
    """Descarga un archivo de GCS a /tmp."""
    bucket = storage_client.bucket(BUCKET)
    bucket.blob(f"{MODEL_PREFIX}/{blob_name}").download_to_filename(dest)
    return dest


@app.on_event("startup")
async def startup_event():
    """Carga los dos wrappers y el JSON de precios de barrio."""
    global wrapper_hot, wrapper_cold

    try:
        # 1. Modelos
        path_hot = _download_from_gcs(
            "direct_forecasting_model_t7.joblib", "/tmp/model_hot.joblib"
        )
        path_cold = _download_from_gcs(
            "direct_forecasting_coldstart_model_t7.joblib", "/tmp/model_cold.joblib"
        )
        wrapper_hot = joblib.load(path_hot)
        wrapper_cold = joblib.load(path_cold)
        print("Modelos Hot y Cold cargados correctamente.")
    except Exception as e:
        print(f"Error cargando modelos: {e}")
        raise RuntimeError(f"No se pudieron cargar los modelos: {e}")


# =============================================================================
# SCHEMAS (Pydantic)
# =============================================================================

class PredictRequest(BaseModel):
    fecha: str  # Formato YYYY-MM-DD
    listing_id: int | None = None  # Nullable → Cold Start
    neighbourhood_cleansed: str
    room_type: str
    accommodates: int
    listing_price: float
    minimum_nights: int
    number_of_reviews: int
    review_scores_rating: float | None = None
    instant_bookable: bool

class VertexPredictRequest(BaseModel):
    instances: list[PredictRequest]

class ForecastItem(BaseModel):
    horizonte: int
    probabilidad: float

class PredictResponse(BaseModel):
    listing_id: int | None
    model_used: str  # 'hot' | 'cold'
    forecast: list[ForecastItem]

class VertexPredictResponse(BaseModel):
    predictions: list[PredictResponse]


# =============================================================================
# ENDPOINT /predict
# =============================================================================

@app.post("/predict", response_model=VertexPredictResponse)
async def predict(vertex_req: VertexPredictRequest):
    """
    Flujo de inferencia completo:
      1. Fetch datos de BigQuery (historia + contexto).
      2. Feature engineering (Golden Set + Momentum).
      3. Router Hot/Cold.
      4. Predicción con el Wrapper correspondiente.
    Flujo de inferencia adaptado al estándar de Vertex AI:
      Espera {"instances": [ ... ]}
      Devuelve {"predictions": [ ... ]}
    """
    import pandas as pd
    all_predictions = []

    for req in vertex_req.instances:
        try:
            # 1. Contexto futuro (clima, festivos)
            context = features.fetch_future_context(req.fecha, bq_client)
            if not context:
                raise HTTPException(
                    status_code=404,
                    detail=f"No hay datos de contexto para la fecha {req.fecha}.",
                )

            # 2. Historia del listing (60 días)
            hist_listing = pd.DataFrame()
            if req.listing_id is not None:
                hist_listing = features.fetch_listing_history(
                    req.listing_id, req.fecha, bq_client
                )

            # 3. Historia del barrio (60 días para momentum dinámico)
            hist_neigh = features.fetch_neighborhood_history(
                req.neighbourhood_cleansed, req.fecha, bq_client
            )

            # 4. Feature engineering
            golden_set = features.calculate_golden_set(hist_listing)
            momentum = features.calculate_neighborhood_momentum(hist_neigh)

            # 5. Ensamblar la fila de inferencia
            df_row = features.assemble_inference_row(
                user_input=req.model_dump(),
                context=context,
                golden_set=golden_set,
                momentum=momentum,
            )

            # 6. Router: decidir modelo
            model_type = features.decide_model_type(req.listing_id, hist_listing)

            # 7. Predecir
            wrapper = wrapper_hot if model_type == "hot" else wrapper_cold
            preds = wrapper.predict_proba(df_row)

            forecast = [
                ForecastItem(
                    horizonte=h,
                    probabilidad=round(float(proba[0]), 4),
                )
                for h, proba in sorted(preds.items())
            ]

            all_predictions.append(
                PredictResponse(
                    listing_id=req.listing_id,
                    model_used=model_type,
                    forecast=forecast,
                )
            )
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error en la inferencia para listing {req.listing_id}: {e}"
            )

    return VertexPredictResponse(predictions=all_predictions)


# =============================================================================
# ENDPOINT /health
# =============================================================================

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "models_loaded": wrapper_hot is not None and wrapper_cold is not None,
    }
