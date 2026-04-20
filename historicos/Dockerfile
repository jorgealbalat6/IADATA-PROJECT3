FROM python:3.11-slim
 
WORKDIR /app
 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY main.py .
 
# functions-framework sirve HTTP en el puerto 8080
CMD ["functions-framework", "--target=ingest_airbnb", "--port=8080"]