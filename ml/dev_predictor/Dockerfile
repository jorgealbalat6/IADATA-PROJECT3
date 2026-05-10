# ── Stage 1: Build ──────────────────────────────────────────────────
FROM python:3.10-slim AS builder

WORKDIR /app

# Instalar dependencias primero (cache de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/install -r requirements.txt

# ── Stage 2: Runtime ────────────────────────────────────────────────
FROM python:3.10-slim

WORKDIR /app

# Copiar las dependencias instaladas
COPY --from=builder /install /usr/local/lib/python3.10/site-packages/

# Copiar el código de la aplicación
COPY main.py .
COPY features.py .

# Copiar los pipelines necesarios para deserializar los .joblib
COPY pre_procesado/ ./pre_procesado/

# Puerto estándar de Vertex AI
EXPOSE 8080

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

# Arrancar el servidor
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
