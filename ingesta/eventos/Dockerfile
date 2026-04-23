FROM python:3.11-slim
 
WORKDIR /app
 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY main.py .
 
CMD ["functions-framework", "--target=ingest_events", "--port=8080"]