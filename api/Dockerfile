FROM python:3.12-slim
 
# Evita que Python escriba .pyc y que bufferee stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
 
WORKDIR /app
 
# Copia e instala dependencias primero (aprovecha caché de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
# Copia el código
COPY main.py .
 
# Cloud Run inyecta la variable PORT (por defecto 8080)
ENV PORT=8080
EXPOSE 8080
 
# Arranca con uvicorn en el puerto que indique Cloud Run
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT"]