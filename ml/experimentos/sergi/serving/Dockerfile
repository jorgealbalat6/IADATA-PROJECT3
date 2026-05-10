FROM python:3.11-slim

WORKDIR /app

# Dependencia del sistema necesaria para scikit-learn / xgboost en slim
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# AIP_HTTP_PORT es inyectado por Vertex AI al arrancar el contenedor.
# Si no existe (Cloud Run local), se usa PORT o 8080 como fallback.
ENV AIP_HTTP_PORT=8080

CMD exec gunicorn \
    --bind :${AIP_HTTP_PORT} \
    --workers 1 \
    --threads 8 \
    --timeout 120 \
    main:app
