"""
Script de prueba del agente — corre localmente con gcloud ADC.
No necesita Flask ni Firestore: usa datos falsos.

Uso:
    python test_agent.py
    python test_agent.py "¿Cuál es mi ocupación esta semana?"
"""

import sys
import json
from google import genai
from google.genai import types

MODEL  = "gemini-2.5-flash"
client = genai.Client(
    vertexai=True,
    project="project3grupo1",
    location="us-central1",
)

# ── Datos falsos que simulan Firestore ──────────────────────────────────────

FAKE_APARTMENTS = [
    {
        "id": "apt1",
        "name": "Piso Gràcia Centro",
        "neighbourhood": "Gràcia",
        "room_type": "Entire home/apt",
        "listing_price": 95,
        "accommodates": 4,
        "minimum_nights": 2,
    },
    {
        "id": "apt2",
        "name": "Habitación Eixample",
        "neighbourhood": "Eixample",
        "room_type": "Private room",
        "listing_price": 55,
        "accommodates": 2,
        "minimum_nights": 1,
    },
]

FAKE_PREDICTIONS = {
    "apt1": [
        {"date": "2026-05-09", "probability": 0.82},
        {"date": "2026-05-10", "probability": 0.91},
        {"date": "2026-05-11", "probability": 0.88},
        {"date": "2026-05-12", "probability": 0.45},
        {"date": "2026-05-13", "probability": 0.50},
        {"date": "2026-05-14", "probability": 0.78},
        {"date": "2026-05-15", "probability": 0.85},
    ],
    "apt2": [
        {"date": "2026-05-09", "probability": 0.60},
        {"date": "2026-05-10", "probability": 0.65},
        {"date": "2026-05-11", "probability": 0.70},
        {"date": "2026-05-12", "probability": 0.30},
        {"date": "2026-05-13", "probability": 0.35},
        {"date": "2026-05-14", "probability": 0.55},
        {"date": "2026-05-15", "probability": 0.62},
    ],
}

FAKE_INSIGHTS = {
    ("Gràcia", "Entire home/apt"): {
        "avg_occupancy": 0.74,
        "avg_price": 102,
        "num_listings": 312,
        "best_months": ["Junio", "Julio", "Agosto"],
        "best_days": ["Viernes", "Sábado"],
    },
    ("Eixample", "Private room"): {
        "avg_occupancy": 0.68,
        "avg_price": 58,
        "num_listings": 540,
        "best_months": ["Julio", "Agosto", "Septiembre"],
        "best_days": ["Viernes", "Sábado", "Domingo"],
    },
}

# ── Implementación de las herramientas (versión fake) ───────────────────────

def tool_list_apartments():
    return [
        {k: v for k, v in apt.items() if k != "id"} | {"apartment_id": apt["id"]}
        for apt in FAKE_APARTMENTS
    ]

def tool_get_upcoming_predictions(apartment_id: str):
    preds = FAKE_PREDICTIONS.get(apartment_id, [])
    if not preds:
        return {"error": f"No se encontraron predicciones para {apartment_id}"}
    return {
        "apartment_id": apartment_id,
        "predictions": preds,
        "avg_probability": round(sum(p["probability"] for p in preds) / len(preds), 2),
    }

def tool_get_insights(neighbourhood: str, room_type: str):
    data = FAKE_INSIGHTS.get((neighbourhood, room_type))
    if not data:
        return {"error": f"No hay insights para {neighbourhood} / {room_type}"}
    return data

def tool_predict(apartment_id: str, date: str):
    import random
    probability = round(random.uniform(0.4, 0.95), 2)
    return {"apartment_id": apartment_id, "date": date, "probability": probability}

TOOL_REGISTRY = {
    "list_apartments":          lambda args: tool_list_apartments(),
    "get_upcoming_predictions": lambda args: tool_get_upcoming_predictions(args["apartment_id"]),
    "get_insights":             lambda args: tool_get_insights(args["neighbourhood"], args["room_type"]),
    "predict":                  lambda args: tool_predict(args["apartment_id"], args["date"]),
}

# ── Definición de herramientas para Gemini ──────────────────────────────────

TOOL_DECLARATIONS = [
    {
        "name": "list_apartments",
        "description": "Lista todos los alojamientos registrados por el usuario con sus características.",
        "parameters": {"type": "OBJECT", "properties": {}, "required": []},
    },
    {
        "name": "get_upcoming_predictions",
        "description": "Obtiene las predicciones de ocupación de los próximos días para un alojamiento.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "apartment_id": {"type": "STRING", "description": "ID del alojamiento"},
            },
            "required": ["apartment_id"],
        },
    },
    {
        "name": "get_insights",
        "description": "Obtiene datos de mercado reales del barrio: ocupación media, precio medio, mejores meses y días. Usa siempre el neighbourhood y room_type exactos que devuelve list_apartments.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "neighbourhood": {"type": "STRING", "description": "Nombre del barrio"},
                "room_type":     {"type": "STRING", "description": "Tipo de habitación"},
            },
            "required": ["neighbourhood", "room_type"],
        },
    },
    {
        "name": "predict",
        "description": "Genera una predicción de ocupación para una fecha concreta.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "apartment_id": {"type": "STRING", "description": "ID del alojamiento"},
                "date":         {"type": "STRING", "description": "Fecha en formato YYYY-MM-DD"},
            },
            "required": ["apartment_id", "date"],
        },
    },
]

SYSTEM_PROMPT = (
    "You are an expert assistant for short-term rental hosts in Barcelona. "
    "You help owners optimize prices, occupancy and strategy on Airbnb. "
    "IMPORTANT: You have tools available — always call them yourself to get real data before answering. "
    "Never ask the user for information you can retrieve with a tool call. "
    "When the user asks about their properties or occupancy, always call list_apartments first "
    "to get the exact neighbourhood and room_type values, then call the appropriate tool. "
    "Always reply to the user in Spanish, concisely and practically. Maximum 3-4 sentences."
)

# ── Bucle del agente ────────────────────────────────────────────────────────

TOOLS = types.Tool(function_declarations=[
    types.FunctionDeclaration(**d) for d in TOOL_DECLARATIONS
])

def call_gemini(contents):
    response = client.models.generate_content(
        model=MODEL,
        contents=contents,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            tools=[TOOLS],
            max_output_tokens=500,
            temperature=0.7,
        ),
    )
    return response

def run_agent(user_message: str, verbose: bool = True) -> str:
    contents = [{"role": "user", "parts": [{"text": user_message}]}]

    for iteration in range(5):
        if verbose:
            print(f"\n[iter {iteration + 1}] Llamando a Gemini...")

        response = call_gemini(contents)
        candidate = response.candidates[0].content
        contents.append(candidate)

        # Buscar function calls en la respuesta
        function_calls = [p.function_call for p in candidate.parts if p.function_call]

        if not function_calls:
            return response.text

        # Ejecutar todas las function calls y devolver resultados
        tool_results = []
        for fc in function_calls:
            name = fc.name
            args = dict(fc.args)
            if verbose:
                print(f"  → tool call: {name}({json.dumps(args, ensure_ascii=False)})")
            result_data = TOOL_REGISTRY[name](args)
            if verbose:
                print(f"  ← resultado: {json.dumps(result_data, ensure_ascii=False)[:200]}")
            tool_results.append(
                types.Part.from_function_response(name=name, response={"result": result_data})
            )

        contents.append(types.Content(role="user", parts=tool_results))

    return "El agente alcanzó el límite de iteraciones sin responder."


# ── Entrada ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else None

    if query:
        print(f"\nPregunta: {query}")
        print("-" * 60)
        answer = run_agent(query)
        print(f"\nRespuesta:\n{answer}")
    else:
        # Modo interactivo
        print("Agente StayCast — escribe 'salir' para terminar\n")
        while True:
            try:
                user_input = input("Tú: ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not user_input or user_input.lower() == "salir":
                break
            answer = run_agent(user_input)
            print(f"\nAgente: {answer}\n")
