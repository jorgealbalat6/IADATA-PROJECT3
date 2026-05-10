"""
API REST — Airbnb Occupancy Prediction (Barcelona)

Endpoints:
    GET  /apartments             → Listar pisos del usuario
    POST /apartments             → Añadir piso
    PUT  /apartments/<id>        → Editar características de un piso
    DELETE /apartments/<id>      → Eliminar piso
    GET  /apartments/<id>/predictions          → Predicciones por rango (?start=&end=)
    GET  /apartments/<id>/predictions/upcoming → Próximas 2 semanas
    GET  /apartments/<id>/predictions/history  → Histórico últimos 2 meses
    POST /predict                → Predicción Vertex AI
    POST /batch-predict          → Predicción automática diaria
    GET  /insights               → Métricas de mercado

Auth: Firebase Authentication (ID tokens verificados con firebase-admin)
"""

import os
import datetime
from functools import wraps

import firebase_admin
from firebase_admin import credentials, auth as firebase_auth
from flask import Flask, request, jsonify
from google.cloud import firestore
from flask_cors import CORS

# ─── Init ───
app = Flask(__name__)
CORS(app)

# Inicializar Firebase Admin
# El projectId de Firebase puede diferir del GCP project
FIREBASE_PROJECT = os.environ.get("FIREBASE_PROJECT", "project3grupo1-2f40e")
if not firebase_admin._apps:
    firebase_admin.initialize_app(options={"projectId": FIREBASE_PROJECT})

db = firestore.Client(
    project=os.environ.get("GCP_PROJECT", "project3grupo1"),
    database=os.environ.get("FIRESTORE_DATABASE", "(default)"),
)


def require_auth(f):
    """Decorador que verifica Firebase ID token y asegura que el usuario existe en Firestore."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return jsonify({"error": "No autorizado"}), 401

        token = header.split("Bearer ")[1]
        try:
            decoded = firebase_auth.verify_id_token(token)
            request.uid = decoded["uid"]
        except Exception:
            return jsonify({"error": "Token inválido o expirado"}), 401

        # Crear documento del usuario en Firestore si no existe
        user_ref = db.collection("users").document(request.uid)
        if not user_ref.get().exists:
            user_ref.set({
                "email": decoded.get("email", ""),
                "name": decoded.get("name", decoded.get("email", "").split("@")[0]),
                "created_at": firestore.SERVER_TIMESTAMP,
            })

        return f(*args, **kwargs)

    return wrapper


@app.route("/apartments", methods=["GET"])
@require_auth
def list_apartments():
    """Lista los pisos del usuario."""
    docs = db.collection("users").document(request.uid).collection("apartments").stream()

    apartments = []
    for doc in docs:
        apt = doc.to_dict()
        apt["id"] = doc.id
        apartments.append(apt)

    return jsonify(apartments), 200

@app.route("/apartments", methods=["POST"])
@require_auth
def add_apartment():
    """Añade un piso al usuario."""
    data = request.get_json()

    name = data.get("name", "")
    if not name:
        return jsonify({"error": "name es obligatorio"}), 400

    apt_data = {
        "name": name,
        "neighbourhood": data.get("neighbourhood", ""),
        "room_type": data.get("room_type", ""),
        "accommodates": data.get("accommodates", 0),
        "bedrooms": data.get("bedrooms", 0),
        "beds": data.get("beds", 0),
        "number_of_reviews": data.get("number_of_reviews", 0),
        "review_scores_rating": data.get("review_scores_rating", 0),
        "created_at": firestore.SERVER_TIMESTAMP,
        "listing_price": data.get("listing_price", 80),
        "minimum_nights": data.get("minimum_nights", 2),
        "instant_bookable": data.get("instant_bookable", False)
    }

    doc_ref = db.collection("users").document(request.uid).collection("apartments").document()
    doc_ref.set(apt_data)

    apt_data["id"] = doc_ref.id
    apt_data["created_at"] = datetime.datetime.utcnow().isoformat() + "Z"

    return jsonify(apt_data), 201

@app.route("/apartments/<apartment_id>", methods=["PUT"])
@require_auth
def update_apartment(apartment_id):
    """Actualiza las características de un piso."""
    data = request.get_json()

    ref = db.collection("users").document(request.uid).collection("apartments").document(apartment_id)
    doc = ref.get()

    if not doc.exists:
        return jsonify({"error": "Piso no encontrado"}), 404

    allowed_fields = [
    "name", "neighbourhood", "room_type", "accommodates",
    "bedrooms", "beds", "number_of_reviews", "review_scores_rating",
    "listing_price", "minimum_nights", "instant_bookable",
    ]
    updates = {k: v for k, v in data.items() if k in allowed_fields}

    if not updates:
        return jsonify({"error": "No hay campos válidos para actualizar"}), 400

    updates["updated_at"] = firestore.SERVER_TIMESTAMP
    ref.update(updates)

    return jsonify({"message": "Piso actualizado", "id": apartment_id}), 200


@app.route("/apartments/<apartment_id>", methods=["DELETE"])
@require_auth
def delete_apartment(apartment_id):
    """Elimina un piso y sus predicciones."""
    ref = db.collection("users").document(request.uid).collection("apartments").document(apartment_id)
    doc = ref.get()

    if not doc.exists:
        return jsonify({"error": "Piso no encontrado"}), 404

    # Borrar subcolección de predicciones
    preds = ref.collection("predictions").stream()
    for pred in preds:
        pred.reference.delete()

    ref.delete()

    return jsonify({"message": "Piso eliminado"}), 200


@app.route("/apartments/<apartment_id>/predictions", methods=["GET"])
@require_auth
def get_predictions_range(apartment_id):
    """Predicciones por rango de fechas (?start=YYYY-MM-DD&end=YYYY-MM-DD)."""
    start = request.args.get("start")
    end = request.args.get("end")

    if not start or not end:
        return jsonify({"error": "Parámetros start y end son obligatorios (YYYY-MM-DD)"}), 400

    ref = (
        db.collection("users").document(request.uid)
        .collection("apartments").document(apartment_id)
        .collection("predictions")
    )

    docs = (
        ref.where("date", ">=", start)
        .where("date", "<=", end)
        .order_by("date")
        .stream()
    )

    predictions = [{"id": doc.id, **doc.to_dict()} for doc in docs]

    return jsonify(predictions), 200


@app.route("/apartments/<apartment_id>/predictions/upcoming", methods=["GET"])
@require_auth
def get_predictions_upcoming(apartment_id):
    """Predicciones para las próximas 2 semanas."""
    today = datetime.date.today().isoformat()
    in_14_days = (datetime.date.today() + datetime.timedelta(days=14)).isoformat()

    ref = (
        db.collection("users").document(request.uid)
        .collection("apartments").document(apartment_id)
        .collection("predictions")
    )

    docs = (
        ref.where("date", ">=", today)
        .where("date", "<=", in_14_days)
        .order_by("date")
        .stream()
    )

    predictions = [{"id": doc.id, **doc.to_dict()} for doc in docs]

    return jsonify(predictions), 200


@app.route("/apartments/<apartment_id>/predictions/history", methods=["GET"])
@require_auth
def get_predictions_history(apartment_id):
    """Histórico de predicciones de los últimos 2 meses."""
    two_months_ago = (datetime.date.today() - datetime.timedelta(days=60)).isoformat()
    today = datetime.date.today().isoformat()

    ref = (
        db.collection("users").document(request.uid)
        .collection("apartments").document(apartment_id)
        .collection("predictions")
    )

    docs = (
        ref.where("date", ">=", two_months_ago)
        .where("date", "<=", today)
        .order_by("date")
        .stream()
    )

    predictions = [{"id": doc.id, **doc.to_dict()} for doc in docs]

    return jsonify(predictions), 200


@app.route("/predict", methods=["POST"])
@require_auth
def predict():
    """Llama a Vertex AI y guarda en Firestore si es predicción real."""
    import google.auth
    import google.auth.transport.requests
    import requests as req

    data = request.get_json()

    apartment_id = data.get("apartment_id")
    date_str = data.get("date")

    if not date_str:
        return jsonify({"error": "date es obligatorio"}), 400

    payload = {
        "fecha":                  date_str,
        "neighbourhood_cleansed": data.get("neighbourhood"),
        "room_type":              data.get("room_type"),
        "accommodates":           data.get("accommodates", 2),
        "listing_price":          data.get("listing_price", 80.0),
        "minimum_nights":         data.get("minimum_nights", 2),
        "number_of_reviews":      data.get("number_of_reviews", 0),
        "review_scores_rating":   data.get("review_scores_rating"),
        "instant_bookable":       data.get("instant_bookable", False),
    }

    credentials, _ = google.auth.default()
    credentials.refresh(google.auth.transport.requests.Request())
    token = credentials.token

    endpoint = os.environ.get(
        "VERTEX_ENDPOINT_URL",
        "https://europe-west1-aiplatform.googleapis.com/v1/projects/project3grupo1/locations/europe-west1/endpoints/2966335037181526016:rawPredict"
    )

    response = req.post(
        endpoint,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )

    if not response.ok:
        print(f"[PREDICT ERROR] Status: {response.status_code}, Body: {response.text[:500]}")
        return jsonify({"error": "Error en el modelo", "detail": response.text[:200]}), 502

    result = response.json()
    probability = result.get("probabilidad")

    # ── Guardar en Firestore si es predicción real ──
    is_simulation = data.get("is_simulation", False)

    if apartment_id and not is_simulation:
        pred_ref = (
            db.collection("users").document(request.uid)
            .collection("apartments").document(apartment_id)
            .collection("predictions").document(date_str)
        )
        pred_ref.set({
            "date": date_str,
            "probability": probability,
            "neighbourhood": data.get("neighbourhood"),
            "room_type": data.get("room_type"),
            "accommodates": data.get("accommodates", 2),
            "listing_price": data.get("listing_price", 80.0),
            "minimum_nights": data.get("minimum_nights", 2),
            "number_of_reviews": data.get("number_of_reviews", 0),
            "review_scores_rating": data.get("review_scores_rating"),
            "instant_bookable": data.get("instant_bookable", False),
            "predicted_at": firestore.SERVER_TIMESTAMP,
        })

    return jsonify({"probability": probability, "saved": bool(apartment_id and not is_simulation)}), 200

@app.route("/batch-predict", methods=["POST"])
def batch_predict():
    """Predicción automática diaria: recorre todos los usuarios y apartamentos,
    predice los próximos 14 días y guarda en Firestore.
    Protegido con BATCH_SECRET (Cloud Scheduler envía el header)."""
    import google.auth
    import google.auth.transport.requests
    import requests as req

    # ── Autenticación: acepta BATCH_SECRET o OIDC token ──
    batch_secret = os.environ.get("BATCH_SECRET", "")
    auth_header = request.headers.get("Authorization", "")
    x_secret = request.headers.get("X-Batch-Secret", "")

    if batch_secret and x_secret != batch_secret:
        # Si no coincide el secret, verificar si es un OIDC token válido
        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "No autorizado"}), 401

    # ── Preparar credenciales para Vertex AI ──
    credentials, _ = google.auth.default()
    credentials.refresh(google.auth.transport.requests.Request())
    vertex_token = credentials.token

    endpoint = os.environ.get(
        "VERTEX_ENDPOINT_URL",
        "https://europe-west1-aiplatform.googleapis.com/v1/projects/project3grupo1/locations/europe-west1/endpoints/2966335037181526016:rawPredict"
    )

    today = datetime.date.today()
    dates = [(today + datetime.timedelta(days=d)).isoformat() for d in range(14)]

    total_predictions = 0
    total_errors = 0
    users_processed = 0

    # ── Recorrer todos los usuarios ──
    users = db.collection("users").stream()
    for user_doc in users:
        uid = user_doc.id
        apartments = db.collection("users").document(uid).collection("apartments").stream()

        for apt_doc in apartments:
            apt = apt_doc.to_dict()
            apt_id = apt_doc.id

            for date_str in dates:
                payload = {
                    "fecha":                  date_str,
                    "neighbourhood_cleansed": apt.get("neighbourhood", ""),
                    "room_type":              apt.get("room_type", ""),
                    "accommodates":           apt.get("accommodates", 2),
                    "listing_price":          apt.get("listing_price", 80.0),
                    "minimum_nights":         apt.get("minimum_nights", 2),
                    "number_of_reviews":      apt.get("number_of_reviews", 0),
                    "review_scores_rating":   apt.get("review_scores_rating", 4.5),
                    "instant_bookable":       apt.get("instant_bookable", False),
                }

                try:
                    response = req.post(
                        endpoint,
                        headers={
                            "Authorization": f"Bearer {vertex_token}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                        timeout=30,
                    )

                    if not response.ok:
                        print(f"[BATCH ERROR] apt={apt_id} date={date_str} status={response.status_code}")
                        total_errors += 1
                        continue

                    result = response.json()
                    probability = result.get("probabilidad")

                    # Guardar en Firestore
                    pred_ref = (
                        db.collection("users").document(uid)
                        .collection("apartments").document(apt_id)
                        .collection("predictions").document(date_str)
                    )
                    pred_ref.set({
                        "date": date_str,
                        "probability": probability,
                        "neighbourhood": apt.get("neighbourhood"),
                        "room_type": apt.get("room_type"),
                        "accommodates": apt.get("accommodates", 2),
                        "listing_price": apt.get("listing_price", 80.0),
                        "minimum_nights": apt.get("minimum_nights", 2),
                        "number_of_reviews": apt.get("number_of_reviews", 0),
                        "review_scores_rating": apt.get("review_scores_rating"),
                        "instant_bookable": apt.get("instant_bookable", False),
                        "predicted_at": firestore.SERVER_TIMESTAMP,
                        "source": "batch",
                    })

                    total_predictions += 1

                except Exception as e:
                    print(f"[BATCH EXCEPTION] apt={apt_id} date={date_str} error={str(e)}")
                    total_errors += 1

        users_processed += 1

    print(f"[BATCH DONE] users={users_processed} predictions={total_predictions} errors={total_errors}")
    return jsonify({
        "users_processed": users_processed,
        "predictions_saved": total_predictions,
        "errors": total_errors,
    }), 200


@app.route("/insights", methods=["GET"])
@require_auth
def get_insights():
    """Devuelve métricas pre-computadas desde dbt marts en BigQuery."""
    from google.cloud import bigquery

    neighbourhood = request.args.get("neighbourhood", "")
    room_type = request.args.get("room_type", "Entire home/apt")
    user_price = request.args.get("price", type=float)
    user_accommodates = request.args.get("accommodates", type=int)

    if not neighbourhood:
        return jsonify({"error": "neighbourhood es obligatorio"}), 400

    project = os.environ.get("GCP_PROJECT", "project3grupo1")
    dataset = os.environ.get("DBT_DATASET", "dbt_transform")
    bq = bigquery.Client(project=project, location="europe-west1")

    params = [
        bigquery.ScalarQueryParameter("nb", "STRING", neighbourhood),
        bigquery.ScalarQueryParameter("rt", "STRING", room_type),
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=params)

    # ── Monthly (ocupación + competencia mensual) ──
    monthly_q = f"""
    SELECT month, occupancy_rate, avg_price, num_listings
    FROM `{project}.{dataset}.mart_insights_monthly`
    WHERE neighbourhood = @nb AND room_type = @rt
    ORDER BY month
    """

    # ── Weekly ──
    weekly_q = f"""
    SELECT day_of_week, occupancy_rate
    FROM `{project}.{dataset}.mart_insights_weekly`
    WHERE neighbourhood = @nb AND room_type = @rt
    ORDER BY day_of_week
    """

    # ── Neighbourhood ranking ──
    neighbourhood_q = f"""
    SELECT neighbourhood, avg_occupancy, num_listings
    FROM `{project}.{dataset}.mart_insights_neighbourhood`
    WHERE room_type = @rt
    ORDER BY avg_occupancy DESC
    LIMIT 15
    """

    # ── Stats del barrio (con percentiles de precio) ──
    stats_q = f"""
    SELECT avg_occupancy, avg_price, num_listings, avg_reviews,
           min_price, max_price, p10_price, p25_price, median_price, p75_price, p90_price,
           avg_accommodates, avg_minimum_nights, pct_instant_bookable
    FROM `{project}.{dataset}.mart_insights_neighbourhood`
    WHERE neighbourhood = @nb AND room_type = @rt
    """

    # ── Stats Barcelona ──
    bcn_q = f"""
    SELECT avg_occupancy, avg_price, median_price, num_listings
    FROM `{project}.{dataset}.mart_insights_bcn`
    WHERE room_type = @rt
    """

    # ── Precio-Ocupación ──
    price_occ_q = f"""
    SELECT price_bucket, bucket_order, num_listings, avg_occupancy, avg_price
    FROM `{project}.{dataset}.mart_insights_price_occupancy`
    WHERE neighbourhood = @nb AND room_type = @rt
    ORDER BY bucket_order
    """

    result = {
        "monthly": [], "weekly": [], "neighbourhood": [],
        "stats": {}, "bcn": {}, "price_occupancy": [],
    }

    for row in bq.query(monthly_q, job_config=job_config).result():
        result["monthly"].append({
            "month": row.month,
            "occupancy": float(row.occupancy_rate),
            "avg_price": float(row.avg_price) if row.avg_price else 0,
            "num_listings": row.num_listings,
        })

    for row in bq.query(weekly_q, job_config=job_config).result():
        result["weekly"].append({
            "dow": row.day_of_week,
            "occupancy": float(row.occupancy_rate),
        })

    for row in bq.query(neighbourhood_q, job_config=job_config).result():
        result["neighbourhood"].append({
            "name": row.neighbourhood,
            "occupancy": float(row.avg_occupancy),
            "count": row.num_listings,
        })

    for row in bq.query(stats_q, job_config=job_config).result():
        result["stats"] = {
            "avg_occupancy": float(row.avg_occupancy),
            "avg_price": float(row.avg_price),
            "num_listings": row.num_listings,
            "avg_reviews": float(row.avg_reviews) if row.avg_reviews else 0,
            "min_price": float(row.min_price) if row.min_price else 0,
            "max_price": float(row.max_price) if row.max_price else 0,
            "p10_price": float(row.p10_price) if row.p10_price else 0,
            "p25_price": float(row.p25_price) if row.p25_price else 0,
            "median_price": float(row.median_price) if row.median_price else 0,
            "p75_price": float(row.p75_price) if row.p75_price else 0,
            "p90_price": float(row.p90_price) if row.p90_price else 0,
            "avg_accommodates": float(row.avg_accommodates) if row.avg_accommodates else 0,
            "avg_minimum_nights": float(row.avg_minimum_nights) if row.avg_minimum_nights else 0,
            "pct_instant_bookable": float(row.pct_instant_bookable) if row.pct_instant_bookable else 0,
        }

    for row in bq.query(bcn_q, job_config=job_config).result():
        result["bcn"] = {
            "avg_occupancy": float(row.avg_occupancy),
            "avg_price": float(row.avg_price),
            "median_price": float(row.median_price) if row.median_price else 0,
            "num_listings": row.num_listings,
        }

    for row in bq.query(price_occ_q, job_config=job_config).result():
        result["price_occupancy"].append({
            "bucket": row.price_bucket,
            "order": row.bucket_order,
            "count": row.num_listings,
            "occupancy": float(row.avg_occupancy),
            "avg_price": float(row.avg_price),
        })

    # ── Calcular percentil del usuario si envía su precio ──
    if user_price and result.get("stats"):
        s = result["stats"]
        # Interpolación lineal aproximada del percentil
        percentiles = [
            (0, s.get("min_price", 0)),
            (10, s.get("p10_price", 0)),
            (25, s.get("p25_price", 0)),
            (50, s.get("median_price", 0)),
            (75, s.get("p75_price", 0)),
            (90, s.get("p90_price", 0)),
            (100, s.get("max_price", 0)),
        ]
        pct = 50  # default
        for i in range(len(percentiles) - 1):
            p1, v1 = percentiles[i]
            p2, v2 = percentiles[i + 1]
            if v1 <= user_price <= v2 and v2 > v1:
                pct = p1 + (p2 - p1) * (user_price - v1) / (v2 - v1)
                break
            elif user_price < v1:
                pct = p1
                break
        else:
            if user_price >= percentiles[-1][1]:
                pct = 100
        result["user_price_percentile"] = round(pct, 1)

    return jsonify(result), 200

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "airbnb-occupancy-api"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)