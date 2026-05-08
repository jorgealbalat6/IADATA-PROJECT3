import os
import subprocess
from dotenv import load_dotenv
from google.cloud import aiplatform

def registrar_modelo():
    load_dotenv()
    project_id = os.getenv("PROJECT_ID")
    region = os.getenv("REGION")
    bucket_name = os.getenv("BUCKET_NAME")
    model_display_name = os.getenv("MODEL_DISPLAY_NAME")
    
    # URI de los artefactos y contenedor preconstruido de Vertex AI para XGBoost
    artifact_uri = f"gs://{bucket_name}/models/xgboost-demo"
    # Utilizamos la imagen preconstruida recomendada para XGBoost CPU
    container_image_uri = "europe-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.2-1:latest"
    
    print("1. Inicializando Vertex AI SDK...")
    aiplatform.init(project=project_id, location=region)
    
    print(f"2. Registrando modelo '{model_display_name}' en Model Registry...")
    model = aiplatform.Model.upload(
        display_name=model_display_name,
        artifact_uri=artifact_uri,
        serving_container_image_uri=container_image_uri,
    )
    
    print(f"Modelo registrado exitosamente con ID: {model.name}")
    
    # Guardamos el ID del modelo en el .env temporalmente para que lo recoja el script 3
    with open(".env", "a") as f:
        f.write(f"\nVERTEX_MODEL_ID={model.name}")

if __name__ == "__main__":
    registrar_modelo()
    
    print("==> Transición al Script 3...")
    subprocess.run(["python", "script_3_crear_endpoint_y_desplegar.py"], check=True)