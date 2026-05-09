"""
API REST — Airbnb Occupancy Prediction (Barcelona)

Endpoints:
    POST /auth/register          → Registro de usuario
    POST /auth/login             → Login (devuelve token JWT)
    GET  /apartments             → Listar pisos del usuario
    POST /apartments             → Añadir piso
    PUT  /apartments/<id>        → Editar características de un piso
    DELETE /apartments/<id>      → Eliminar piso
    GET  /apartments/<id>/predictions          → Predicciones por rango (?start=&end=)
    GET  /apartments/<id>/predictions/upcoming → Próximas 2 semanas
    GET  /apartments/<id>/predictions/history  → Histórico últimos 2 meses
"""

import os
import datetime
import hashlib
import secrets
from functools import wraps

import jwt
from flask import Flask, request, jsonify
from google.cloud import firestore
from flask_cors import CORS

# ─── Init ───
app = Flask(__name__)
CORS(app)
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
db = firestore.Client(
    project=os.environ.get("GCP_PROJECT", "project3grupo1"),
    database=os.environ.get("FIRESTORE_DATABASE", "(default)"),
)


def hash_password(password, salt=None):
    """Hash password con SHA-256 + salt."""
    if salt is None:
        salt = secrets.token_hex(16)
    hashed = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return hashed, salt


def require_auth(f):
    """Decorador que exige token JWT válido."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return jsonify({"error": "No autorizado"}), 401

        token = header.split("Bearer ")[1]
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            request.uid = payload["uid"]
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expirado"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Token inválido"}), 401

        return f(*args, **kwargs)

    return wrapper


@app.route("/auth/register", methods=["POST"])
def register():
    """Registra un usuario nuevo."""
    data = request.get_json()
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")
    name = data.get("name", "")

    if not email or not password:
        return jsonify({"error": "email y password son obligatorios"}), 400

    # Comprobar si ya existe
    users = db.collection("users").where("email", "==", email).limit(1).stream()
    if any(True for _ in users):
        return jsonify({"error": "El email ya está registrado"}), 409

    hashed, salt = hash_password(password)

    doc_ref = db.collection("users").document()
    doc_ref.set({
        "email": email,
        "name": name,
        "password_hash": hashed,
        "salt": salt,
        "created_at": firestore.SERVER_TIMESTAMP,
    })

    return jsonify({"uid": doc_ref.id, "email": email}), 201


@app.route("/auth/login", methods=["POST"])
def login():
    """Login: devuelve JWT token."""
    data = request.get_json()
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"error": "email y password son obligatorios"}), 400

    # Buscar usuario
    users = db.collection("users").where("email", "==", email).limit(1).stream()
    user_doc = None
    for doc in users:
        user_doc = doc
        break

    if not user_doc:
        return jsonify({"error": "Credenciales incorrectas"}), 401

    user_data = user_doc.to_dict()
    hashed, _ = hash_password(password, user_data["salt"])

    if hashed != user_data["password_hash"]:
        return jsonify({"error": "Credenciales incorrectas"}), 401

    # Generar JWT (expira en 24h)
    token = jwt.encode(
        {
            "uid": user_doc.id,
            "email": email,
            "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24),
        },
        JWT_SECRET,
        algorithm="HS256",
    )

    return jsonify({"token": token, "uid": user_doc.id}), 200


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
        "https://europe-west1-aiplatform.googleapis.com/v1/projects/project3grupo1/locations/europe-west1/endpoints/8138156259263119360:rawPredict"
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
        return jsonify({"error": "Error en el modelo"}), 502

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