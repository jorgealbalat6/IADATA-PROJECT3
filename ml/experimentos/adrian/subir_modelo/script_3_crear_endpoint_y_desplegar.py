import os
from dotenv import load_dotenv
from google.cloud import aiplatform

def crear_y_desplegar():
    load_dotenv()
    project_id = os.getenv("PROJECT_ID")
    region = os.getenv("REGION")
    endpoint_name = os.getenv("ENDPOINT_DISPLAY_NAME")
    model_id = os.getenv("VERTEX_MODEL_ID")
    
    aiplatform.init(project=project_id, location=region)
    
    print(f"1. Creando Endpoint '{endpoint_name}'...")
    endpoint = aiplatform.Endpoint.create(display_name=endpoint_name)
    print(f"Endpoint creado con ID: {endpoint.name}")
    
    print("2. Recuperando modelo registrado...")
    model = aiplatform.Model(model_name=model_id)
    
    print("3. Desplegando modelo en el Endpoint (esto puede tardar 5-10 minutos)...")
    # Máquina barata n1-standard-2 para pruebas, min_replica=1
    model.deploy(
        endpoint=endpoint,
        machine_type="n1-standard-2",
        min_replica_count=1,
        max_replica_count=1,
        traffic_split={"0": 100}
    )
    
    print("¡Despliegue completado con éxito!")
    print(f"Puedes probar predicciones enviando tráfico a este Endpoint ID: {endpoint.resource_name}")

if __name__ == "__main__":
    crear_y_desplegar()