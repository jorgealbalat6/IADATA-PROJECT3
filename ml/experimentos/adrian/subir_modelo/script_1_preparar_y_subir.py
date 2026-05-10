import os
import sys
import subprocess

def ejecutar_setup():
    print("1. Ejecutando login_gcp.bat y dependencias...")
    subprocess.run(["login_gcp.bat"], shell=True, check=True)
    
    # 1º Python instala dotenv y google-cloud-storage usando tu entorno actual
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)

def subir_a_cloud_storage():
    # 2º Como ya se han instalado, AHORA SÍ le decimos a Python que las importe
    from dotenv import load_dotenv
    from google.cloud import storage
    
    load_dotenv()
    project_id = os.getenv("PROJECT_ID")
    bucket_name = os.getenv("BUCKET_NAME")
    local_model_path = os.getenv("LOCAL_MODEL_PATH")
    
    print(f"2. Subiendo artefacto desde {local_model_path} a gs://{bucket_name}/models/xgboost-demo/")
    storage_client = storage.Client(project=project_id)
    
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name, location=os.getenv("REGION"))
    
    blob = bucket.blob("models/xgboost-demo/model.bst") 
    blob.upload_from_filename(local_model_path)
    print("Subida completada.")

if __name__ == "__main__":
    ejecutar_setup()
    subir_a_cloud_storage()
    
    print("==> Transición al Script 2 (Registro)...")
    subprocess.run([sys.executable, "script_2_registrar_modelo.py"], check=True)