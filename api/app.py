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

@app.route("/chat", methods=["POST"])
@require_auth
def chat():
    """Agente conversacional con function calling sobre Gemini."""
    from google import genai
    from google.genai import types
    import google.auth
    import google.auth.transport.requests
    import requests as req

    data = request.get_json()
    messages = data.get("messages", [])
    if not messages:
        return jsonify({"error": "messages es obligatorio"}), 400

    uid = request.uid
    project = os.environ.get("GCP_PROJECT", "project3grupo1")
    dataset = os.environ.get("DBT_DATASET", "dbt_transform")

    gemini_client = genai.Client(vertexai=True, project=project, location="us-central1")

    # ── Herramientas con datos reales ──────────────────────────────────────────

    def tool_list_apartments():
        docs = db.collection("users").document(uid).collection("apartments").stream()
        return [
            {"_internal_id": doc.id, "name": apt.get("name"), "neighbourhood": apt.get("neighbourhood"),
             "room_type": apt.get("room_type"), "listing_price": apt.get("listing_price"),
             "accommodates": apt.get("accommodates"), "minimum_nights": apt.get("minimum_nights")}
            for doc in docs for apt in [doc.to_dict()]
        ]

    def tool_get_upcoming_predictions(apartment_id):
        apt_ref = db.collection("users").document(uid).collection("apartments").document(apartment_id)
        if not apt_ref.get().exists:
            return {"error": "Alojamiento no encontrado o no pertenece al usuario"}
        today = datetime.date.today().isoformat()
        in_14 = (datetime.date.today() + datetime.timedelta(days=14)).isoformat()
        docs = apt_ref.collection("predictions").where("date", ">=", today).where("date", "<=", in_14).order_by("date").stream()
        preds = [{"date": d.to_dict()["date"], "probability_pct": round(d.to_dict().get("probability", 0) * 100)} for d in docs]
        if not preds:
            return {"message": "Sin predicciones aún. Usa el Predictor para generarlas."}
        avg = round(sum(p["probability_pct"] for p in preds) / len(preds))
        return {"apartment_id": apartment_id, "predictions": preds, "avg_occupancy_pct": avg}

    def tool_get_insights(neighbourhood, room_type):
        from google.cloud import bigquery
        bq = bigquery.Client(project=project, location="europe-west1")
        params = [
            bigquery.ScalarQueryParameter("nb", "STRING", neighbourhood),
            bigquery.ScalarQueryParameter("rt", "STRING", room_type),
        ]
        cfg = bigquery.QueryJobConfig(query_parameters=params)
        MONTHS = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
        DAYS   = ['Dom','Lun','Mar','Mié','Jue','Vie','Sáb']

        monthly, weekly, stats = [], [], {}
        for r in bq.query(f"SELECT month, occupancy_rate FROM `{project}.{dataset}.mart_insights_monthly` WHERE neighbourhood=@nb AND room_type=@rt ORDER BY month", job_config=cfg).result():
            monthly.append({"month": MONTHS[r.month - 1], "occupancy_pct": round(float(r.occupancy_rate) * 100)})
        for r in bq.query(f"SELECT day_of_week, occupancy_rate FROM `{project}.{dataset}.mart_insights_weekly` WHERE neighbourhood=@nb AND room_type=@rt ORDER BY day_of_week", job_config=cfg).result():
            weekly.append({"day": DAYS[r.day_of_week - 1], "occupancy_pct": round(float(r.occupancy_rate) * 100)})
        for r in bq.query(f"SELECT avg_occupancy, avg_price, num_listings FROM `{project}.{dataset}.mart_insights_neighbourhood` WHERE neighbourhood=@nb AND room_type=@rt", job_config=cfg).result():
            stats = {"avg_occupancy_pct": round(float(r.avg_occupancy) * 100), "avg_price_eur": round(float(r.avg_price)), "num_listings": r.num_listings}
        return {"neighbourhood": neighbourhood, "room_type": room_type, "monthly": monthly, "weekly": weekly, "stats": stats}

    def tool_predict(apartment_id, date):
        apt_ref = db.collection("users").document(uid).collection("apartments").document(apartment_id)
        apt_doc = apt_ref.get()
        if not apt_doc.exists:
            return {"error": "Alojamiento no encontrado o no pertenece al usuario"}
        apt = apt_doc.to_dict()
        creds, _ = google.auth.default()
        creds.refresh(google.auth.transport.requests.Request())
        endpoint = os.environ.get("VERTEX_ENDPOINT_URL", "https://europe-west1-aiplatform.googleapis.com/v1/projects/project3grupo1/locations/europe-west1/endpoints/2966335037181526016:rawPredict")
        resp = req.post(endpoint, headers={"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"},
            json={"fecha": date, "neighbourhood_cleansed": apt.get("neighbourhood"), "room_type": apt.get("room_type"),
                  "accommodates": apt.get("accommodates", 2), "listing_price": apt.get("listing_price", 80.0),
                  "minimum_nights": apt.get("minimum_nights", 2), "number_of_reviews": apt.get("number_of_reviews", 0),
                  "review_scores_rating": apt.get("review_scores_rating"), "instant_bookable": apt.get("instant_bookable", False)}, timeout=30)
        if not resp.ok:
            return {"error": "Error al calcular la predicción"}
        prob = resp.json().get("probabilidad", 0)
        return {"apartment_id": apartment_id, "date": date, "occupancy_probability_pct": round(prob * 100)}

    def tool_get_upcoming_events(days=14):
        from google.cloud import bigquery
        bq = bigquery.Client(project=project, location="europe-west1")
        today = datetime.date.today().isoformat()
        end = (datetime.date.today() + datetime.timedelta(days=days)).isoformat()
        rows = bq.query(
            f"SELECT title, category, start_date, end_date, phq_attendance "
            f"FROM `{project}.dbt_transform.stg_events` "
            f"WHERE start_date >= @today AND start_date <= @end "
            f"ORDER BY phq_attendance DESC LIMIT 20",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("today", "STRING", today),
                bigquery.ScalarQueryParameter("end", "STRING", end),
            ])
        ).result()
        events = [{"title": r.title, "category": r.category, "start_date": str(r.start_date),
                   "end_date": str(r.end_date), "expected_attendance": r.phq_attendance} for r in rows]
        return {"period": f"{today} to {end}", "events": events, "total": len(events)}

    def tool_get_weather_forecast(days=7):
        from google.cloud import bigquery
        bq = bigquery.Client(project=project, location="europe-west1")
        today = datetime.date.today().isoformat()
        end = (datetime.date.today() + datetime.timedelta(days=days)).isoformat()
        WEATHER_DESC = {0:"Despejado",1:"Mayormente despejado",2:"Parcialmente nublado",3:"Nublado",
                        45:"Niebla",48:"Niebla con escarcha",51:"Llovizna ligera",53:"Llovizna moderada",
                        55:"Llovizna intensa",61:"Lluvia ligera",63:"Lluvia moderada",65:"Lluvia intensa",
                        71:"Nieve ligera",73:"Nieve moderada",75:"Nieve intensa",80:"Chubascos ligeros",
                        81:"Chubascos moderados",82:"Chubascos intensos",95:"Tormenta",99:"Tormenta con granizo"}
        rows = bq.query(
            f"SELECT date, temp_max, temp_min, precipitation_mm, weather_code "
            f"FROM `{project}.dbt_transform.stg_weather` "
            f"WHERE date >= @today AND date <= @end ORDER BY date",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("today", "STRING", today),
                bigquery.ScalarQueryParameter("end", "STRING", end),
            ])
        ).result()
        forecast = [{"date": str(r.date), "temp_max": r.temp_max, "temp_min": r.temp_min,
                     "precipitation_mm": r.precipitation_mm,
                     "description": WEATHER_DESC.get(r.weather_code, "Variable")} for r in rows]
        return {"forecast": forecast}

    def tool_get_upcoming_holidays(days=30):
        from google.cloud import bigquery
        bq = bigquery.Client(project=project, location="europe-west1")
        today = datetime.date.today().isoformat()
        end = (datetime.date.today() + datetime.timedelta(days=days)).isoformat()
        rows = bq.query(
            f"SELECT date, local_name, holiday_type "
            f"FROM `{project}.dbt_transform.stg_holidays` "
            f"WHERE date >= @today AND date <= @end AND applies_to_barcelona = true ORDER BY date",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("today", "STRING", today),
                bigquery.ScalarQueryParameter("end", "STRING", end),
            ])
        ).result()
        holidays = [{"date": str(r.date), "name": r.local_name, "type": r.holiday_type} for r in rows]
        return {"holidays": holidays}

    def tool_explain_prediction(apartment_id, date):
        from google.cloud import bigquery
        apt_ref = db.collection("users").document(uid).collection("apartments").document(apartment_id)
        apt_doc = apt_ref.get()
        if not apt_doc.exists:
            return {"error": "Alojamiento no encontrado o no pertenece al usuario"}
        # Predicción almacenada en Firestore
        pred_doc = apt_ref.collection("predictions").document(date).get()
        prediction = {"date": date, "probability_pct": round(pred_doc.to_dict().get("probability", 0) * 100)} if pred_doc.exists else {"date": date, "probability_pct": None}
        # Contexto del día: eventos, tiempo, festivos
        bq = bigquery.Client(project=project, location="europe-west1")
        day_before = (datetime.date.fromisoformat(date) - datetime.timedelta(days=1)).isoformat()
        day_after  = (datetime.date.fromisoformat(date) + datetime.timedelta(days=1)).isoformat()
        events = [{"title": r.title, "category": r.category, "attendance": r.phq_attendance}
                  for r in bq.query(
                      f"SELECT title, category, phq_attendance FROM `{project}.dbt_transform.stg_events` "
                      f"WHERE start_date <= @date AND end_date >= @day_before ORDER BY phq_attendance DESC LIMIT 5",
                      job_config=bigquery.QueryJobConfig(query_parameters=[
                          bigquery.ScalarQueryParameter("date", "STRING", date),
                          bigquery.ScalarQueryParameter("day_before", "STRING", day_before),
                      ])).result()]
        WEATHER_DESC = {0:"Despejado",1:"Mayormente despejado",2:"Parcialmente nublado",3:"Nublado",
                        61:"Lluvia ligera",63:"Lluvia moderada",65:"Lluvia intensa",80:"Chubascos",95:"Tormenta"}
        weather_rows = list(bq.query(
            f"SELECT temp_max, temp_min, precipitation_mm, weather_code FROM `{project}.dbt_transform.stg_weather` WHERE date = @date",
            job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("date", "STRING", date)])
        ).result())
        weather = {"temp_max": weather_rows[0].temp_max, "temp_min": weather_rows[0].temp_min,
                   "precipitation_mm": weather_rows[0].precipitation_mm,
                   "description": WEATHER_DESC.get(weather_rows[0].weather_code, "Variable")} if weather_rows else {}
        holidays = [{"name": r.local_name} for r in bq.query(
            f"SELECT local_name FROM `{project}.dbt_transform.stg_holidays` WHERE date = @date AND applies_to_barcelona = true",
            job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("date", "STRING", date)])
        ).result()]
        weekday = datetime.date.fromisoformat(date).strftime("%A")
        return {"prediction": prediction, "weekday": weekday, "events_that_day": events,
                "weather": weather, "holidays": holidays}

    TOOL_REGISTRY = {
        "list_apartments":          lambda args: tool_list_apartments(),
        "get_upcoming_predictions": lambda args: tool_get_upcoming_predictions(args["apartment_id"]),
        "get_insights":             lambda args: tool_get_insights(args["neighbourhood"], args["room_type"]),
        "predict":                  lambda args: tool_predict(args["apartment_id"], args["date"]),
        "get_upcoming_events":      lambda args: tool_get_upcoming_events(int(args.get("days", 14))),
        "get_weather_forecast":     lambda args: tool_get_weather_forecast(int(args.get("days", 7))),
        "get_upcoming_holidays":    lambda args: tool_get_upcoming_holidays(int(args.get("days", 30))),
        "explain_prediction":       lambda args: tool_explain_prediction(args["apartment_id"], args["date"]),
    }

    TOOLS = types.Tool(function_declarations=[
        types.FunctionDeclaration(name="list_apartments", description="Lista todos los alojamientos del usuario con sus características.",
            parameters={"type": "OBJECT", "properties": {}, "required": []}),
        types.FunctionDeclaration(name="get_upcoming_predictions", description="Predicciones de ocupación de los próximos 14 días para un alojamiento. Usa el _internal_id exacto de list_apartments como apartment_id.",
            parameters={"type": "OBJECT", "properties": {"apartment_id": {"type": "STRING"}}, "required": ["apartment_id"]}),
        types.FunctionDeclaration(name="get_insights", description="Datos de mercado reales del barrio desde BigQuery: precio medio, ocupación media y número de anuncios activos.",
            parameters={"type": "OBJECT", "properties": {"neighbourhood": {"type": "STRING"}, "room_type": {"type": "STRING"}}, "required": ["neighbourhood", "room_type"]}),
        types.FunctionDeclaration(name="predict", description="Genera una predicción de ocupación on-demand para una fecha concreta (YYYY-MM-DD). Solo funciona para fechas dentro del rango de datos históricos de Airbnb (antes de 2026). Para fechas próximas usa get_upcoming_predictions. Usa el _internal_id de list_apartments como apartment_id.",
            parameters={"type": "OBJECT", "properties": {"apartment_id": {"type": "STRING"}, "date": {"type": "STRING"}}, "required": ["apartment_id", "date"]}),
        types.FunctionDeclaration(name="get_upcoming_events", description="Eventos próximos en Barcelona (conciertos, festivales, deportes, etc.) que pueden aumentar la demanda.",
            parameters={"type": "OBJECT", "properties": {"days": {"type": "INTEGER", "description": "Días hacia adelante (por defecto 14)"}}, "required": []}),
        types.FunctionDeclaration(name="get_weather_forecast", description="Previsión meteorológica de Barcelona para los próximos días.",
            parameters={"type": "OBJECT", "properties": {"days": {"type": "INTEGER", "description": "Días hacia adelante (por defecto 7)"}}, "required": []}),
        types.FunctionDeclaration(name="get_upcoming_holidays", description="Festivos próximos en Barcelona que pueden afectar a la ocupación.",
            parameters={"type": "OBJECT", "properties": {"days": {"type": "INTEGER", "description": "Días hacia adelante (por defecto 30)"}}, "required": []}),
        types.FunctionDeclaration(name="explain_prediction", description="Explica por qué la predicción de ocupación de un día concreto es alta o baja: combina la predicción almacenada con eventos, tiempo y festivos de ese día. Usar siempre que el usuario pregunte 'por qué' sobre una predicción.",
            parameters={"type": "OBJECT", "properties": {"apartment_id": {"type": "STRING"}, "date": {"type": "STRING", "description": "Fecha en formato YYYY-MM-DD"}}, "required": ["apartment_id", "date"]}),
    ])

    today_str = datetime.date.today().isoformat()
    SYSTEM_PROMPT = f"""
    Today's date is {today_str}.

    You are an expert AI assistant for short-term rental hosts in Barcelona.
    Your goal is to help hosts maximize occupancy, revenue and pricing strategy for their Airbnb properties.

    # General behavior
    - Always respond in Spanish.
    - Be concise, practical and actionable.
    - Maximum 4 short paragraphs or 5 sentences.
    - Never expose technical details, IDs or raw database fields.
    - Never show '_internal_id' values to the user.

    # Tool usage rules
    You have access to tools and must proactively use them whenever needed.
    Do not ask the user for information that can be retrieved with tools.

    ## Property information
    - When the user asks about their apartments, occupancy or performance, first call `list_apartments`.
    - Use only the apartment `name` when referring to properties.

    ## Occupancy predictions
    - For future occupancy predictions (upcoming days/weeks), always use `get_upcoming_predictions`.
    - Never use `predict` for future dates.
    - `predict` only supports historical Airbnb dates before 2026.
    - If historical prediction data is unavailable, explain it clearly.

    ## Explanations
    - If the user asks WHY occupancy or demand is high/low, call `explain_prediction`.

    ## External context
    - For events, concerts or festivals → `get_upcoming_events`
    - For weather forecasts → `get_weather_forecast`
    - For holidays or long weekends → `get_upcoming_holidays`

    ## Pricing recommendations
    When recommending pricing or strategy:
    - Combine market insights, events, holidays and weather whenever relevant.
    - Use `get_insights` for neighborhood market trends.
    - Prioritize actionable recommendations over generic explanations.

    # Response style
    - Focus on decisions and recommendations.
    - Quantify impact whenever possible.
    - Avoid unnecessary disclaimers or filler text.
    """

    # ── Convertir historial de mensajes ───────────────────────────────────────
    contents = []
    for msg in messages[-10:]:
        role = "model" if msg.get("role") == "assistant" else "user"
        contents.append(types.Content(role=role, parts=[types.Part.from_text(text=msg["content"])]))

    # ── Bucle del agente ──────────────────────────────────────────────────────
    for _ in range(8):
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash",
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                tools=[TOOLS],
                max_output_tokens=500,
                temperature=0.7,
            ),
        )
        candidate = response.candidates[0].content
        contents.append(candidate)

        function_calls = [p.function_call for p in candidate.parts if p.function_call]
        if not function_calls:
            return jsonify({"reply": response.text}), 200

        tool_results = []
        for fc in function_calls:
            try:
                result_data = TOOL_REGISTRY[fc.name](dict(fc.args))
            except Exception as e:
                print(f"[CHAT TOOL ERROR] {fc.name}: {e}")
                result_data = {"error": str(e)}
            tool_results.append(types.Part.from_function_response(name=fc.name, response={"result": result_data}))

        contents.append(types.Content(role="user", parts=tool_results))

    return jsonify({"reply": "No pude obtener una respuesta. Inténtalo de nuevo."}), 200


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "airbnb-occupancy-api"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)