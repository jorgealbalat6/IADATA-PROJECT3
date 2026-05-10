from google.cloud import aiplatform
import json
import time

# 1. Configuración
PROJECT_ID = 'project3grupo1'
REGION = 'europe-west1'
ENDPOINT_ID = "2225492898479079424"

aiplatform.init(project=PROJECT_ID, location=REGION)
endpoint = aiplatform.Endpoint(ENDPOINT_ID)

def run_test(name, instance):
    print(f"\n{'='*80}")
    print(f"TEST: {name}")
    print(f"{'='*80}")
    
    start_time = time.time()
    try:
        response = endpoint.predict(instances=[instance])
        latency = time.time() - start_time
        
        prediction = response.predictions[0]
        print("% Probabilidad de que el piso sea alquilado")
        print(f" Modelo utilizado: {prediction['model_used'].upper()}")
        print(f"Latencia: {latency:.3f} segundos")
        print("-" * 80)
        
        for item in prediction['forecast']:
            h = item['horizonte']
            prob = item['probabilidad']
            bar = "█" * int(prob * 20)
            print(f"  T + {int(h)}     | {prob:.4f} {bar}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

# --- CASO 1: HOT START (Listing con mucha historia) ---
hot_instance = {
    "fecha": "2026-05-10",
    "listing_id": 18674, 
    "neighbourhood_cleansed": "Eixample",
    "room_type": "Entire home/apt",
    "accommodates": 4,
    "listing_price": 150.0,
    "minimum_nights": 2,
    "number_of_reviews": 45,
    "review_scores_rating": 4.7,
    "instant_bookable": True
}

# --- CASO 2: COLD START (Listing nuevo sin historia) ---
cold_instance = {
    "fecha": "2026-05-10",
    "listing_id": None, # Listing ID nulo fuerza Cold Start
    "neighbourhood_cleansed": "Benimaclet",
    "room_type": "Private room",
    "accommodates": 2,
    "listing_price": 45.0,
    "minimum_nights": 1,
    "number_of_reviews": 0,
    "review_scores_rating": None,
    "instant_bookable": False
}

run_test("HOT START\n(Piso con actividad en los ultimos 60 días de histórico con mínimo 14 dias de registro)", hot_instance)
run_test("COLD START\n(Piso sin actividad en los ultimos 60 días)", cold_instance)
