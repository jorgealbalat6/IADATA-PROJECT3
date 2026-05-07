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

# ─── Init ───
app = Flask(__name__)
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

    listing_id = str(data.get("listing_id", ""))
    if not listing_id:
        return jsonify({"error": "listing_id es obligatorio"}), 400

    apt_data = {
        "listing_id": listing_id,
        "name": data.get("name", ""),
        "neighbourhood": data.get("neighbourhood", ""),
        "room_type": data.get("room_type", ""),
        "accommodates": data.get("accommodates", 0),
        "bedrooms": data.get("bedrooms", 0),
        "beds": data.get("beds", 0),
        "latitude": data.get("latitude", 0),
        "longitude": data.get("longitude", 0),
        "added_at": firestore.SERVER_TIMESTAMP,
    }

    db.collection("users").document(request.uid).collection("apartments").document(listing_id).set(apt_data)

    return jsonify({"message": "Piso añadido", "id": listing_id}), 201


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
        "bedrooms", "beds", "latitude", "longitude",
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
    """Llama al endpoint de Vertex AI y devuelve la predicción."""
    import google.auth
    import google.auth.transport.requests
    import requests as req

    data = request.get_json()

    payload = {
        "fecha":                  data.get("date"),
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
    return jsonify({"probability": result.get("probabilidad")}), 200


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "airbnb-occupancy-api"}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)