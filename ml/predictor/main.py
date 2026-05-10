import os
import pickle
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import bigquery, storage
from features import build_listing_features, build_full_row, load_neigh_mean_price

app = FastAPI()

# Configuración 
PROJECT      = os.environ.get("PROJECT", "project3grupo1")
BUCKET       = os.environ.get("BUCKET", "project3grupo1-ml-models")
MODEL_PREFIX = "occupancy"

# Cargar artefactos al arrancar 
def download_artifact(bucket_name: str, blob_name: str, dest: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_name).download_to_filename(dest)

download_artifact(BUCKET, f"{MODEL_PREFIX}/model.pkl",            "/tmp/model.pkl")
download_artifact(BUCKET, f"{MODEL_PREFIX}/preprocessor.pkl",     "/tmp/preprocessor.pkl")
download_artifact(BUCKET, f"{MODEL_PREFIX}/neigh_mean_price.json","/tmp/neigh_mean_price.json")

with open("/tmp/model.pkl", "rb") as f:
    model = pickle.load(f)

with open("/tmp/preprocessor.pkl", "rb") as f:
    preprocessor = pickle.load(f)

neigh_mean_price = load_neigh_mean_price("/tmp/neigh_mean_price.json")

bq_client = bigquery.Client(project=PROJECT)

# Schemas 
class PredictRequest(BaseModel):
    fecha:                 str
    neighbourhood_cleansed: str
    room_type:             str
    accommodates:          int
    listing_price:         float
    minimum_nights:        int
    number_of_reviews:     int
    review_scores_rating:  float | None = None
    instant_bookable:      bool

class PredictResponse(BaseModel):
    fecha:        str
    probabilidad: float

# Endpoint 
@app.post("/predict", response_model=PredictResponse)
async def predict(req: PredictRequest):
    # 1. Consultar contexto de la fecha en BigQuery
    query = f"""
        SELECT *
        FROM `{PROJECT}.dbt_transform.mart_inference_features`
        WHERE date = '{req.fecha}'
        LIMIT 1
    """
    rows = list(bq_client.query(query).result())
    if not rows:
        raise HTTPException(status_code=404, detail=f"No hay datos para la fecha {req.fecha}")

    context_row = dict(rows[0])
    context_row.pop("date")

    # 2. Feature engineering del listing
    listing_features = build_listing_features(req.dict(), neigh_mean_price)

    # 3. Unir y construir DataFrame
    full_row = build_full_row(context_row, listing_features)

    # 4. Preprocesar y predecir
    X = preprocessor.transform(full_row)
    proba = float(model.predict_proba(X)[0][1])

    return PredictResponse(fecha=req.fecha, probabilidad=round(proba, 4))

@app.get("/health")
async def health():
    return {"status": "ok"}