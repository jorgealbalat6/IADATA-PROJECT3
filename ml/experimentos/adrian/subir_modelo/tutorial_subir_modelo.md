# 🚀 Guía Rápida de Despliegue en Google Cloud

¡Sigue estos pasos para subir tu modelo a la nube de forma rápida y sencilla! ✨

---

### 📋 Pasos a seguir

1.  **🔄 Sincroniza tu entorno local**  
    Asegúrate de haber hecho un `pull` o `merge` de mi rama en tu ordenador para tener todos los archivos actualizados y evitar conflictos.

2.  **📂 Localiza la carpeta de despliegue**  
    Busca la carpeta llamada `subir_modelo` que esta en ml/experimentos/adrian/ y **cópiala entera**.

3.  **📍 Ubica los archivos**  
    Pega esa carpeta exactamente en la misma carpeta donde tengas guardado tu **notebook con el XGBoost que quieras desplegar**.

4.  **⚙️ Configura tus variables**  
    Abre el archivo `.env` que está dentro de la carpeta que acabas de pegar. Cambia los valores de `MODEL_DISPLAY_NAME` y `ENDPOINT_DISPLAY_NAME` por los nombres que quieras usar para identificarlos en Google Cloud(Si eres yannis, te lo he dejado preparado[no hace falta que toques el .env], no hay nada subido con esos nombres).

    Abre tu notebook y pega en una nueva celda (al final del todo) el bloque de código Python de **autolocalización** que tienes aquí abajo:

```python
import os
import sys
from dotenv import load_dotenv

# 1. Sistema de "Autolocalización" a prueba de balas
ruta_actual = os.path.abspath("")

# Si el compañero ejecuta esta celda dos veces por accidente, 
# evitamos que se hunda más en las carpetas y lo devolvemos al inicio.
if os.path.basename(ruta_actual) == "subir_modelo":
    os.chdir("..")
    ruta_actual = os.path.abspath("")

ruta_subir_modelo = os.path.join(ruta_actual, "subir_modelo")

# 2. Comprobación de seguridad para evitar errores raros
if not os.path.exists(ruta_subir_modelo):
    print(f"❌ ¡ALTO! No se encuentra la carpeta 'subir_modelo'.")
    print(f"👉 Por favor, copia la carpeta 'subir_modelo' entera y pégala aquí: {ruta_actual}")
else:
    # 3. Entramos en la sala de máquinas con seguridad
    os.chdir(ruta_subir_modelo)
    print("✅ Directorio de trabajo correcto.")
    
    # 4. Cargamos el mapa (.env)
    load_dotenv(override=True)
    ruta_local = os.getenv("LOCAL_MODEL_PATH")

    # 5. Guardamos el modelo (AVISO!:CAMBIA 'best_model' POR LA VARIABLE DE TU MODELO)
    os.makedirs(os.path.dirname(ruta_local), exist_ok=True)
    best_model.save_model(ruta_local)
    print(f"✅ ¡Modelo guardado en {ruta_local}!")

    # 6. Lanzamos el efecto dominó asegurando que usa el mismo Python del cuaderno
    print("🚀 Iniciando despliegue en Google Cloud...")
    !{sys.executable} script_1_preparar_y_subir.py
```

> ⚠️ **Importante:** Recuerda cambiar la palabra `best_model` por el nombre exacto de la variable donde tengas guardado tu modelo XGBoost entrenado.

6.  **⚡ Ejecuta y espera**  
    Ejecuta esa celda y espera sin tocar nada. El código detectará tu ruta de forma automática, guardará tu modelo en el disco y lanzará la secuencia de scripts de conexión con Google Cloud.

7.  **🛑 Paso Final: Gestión de costes**  
    Cuando la celda termine y veas la confirmación de creación del endpoint:
    *   Despues de que aceptes los logins y esperes 5-10 minst, en la terminal se te pondra un codigo que sera el endpoint, copialo si quieres hacer pruebas con el o pegarlo en el front.
    *   Tambien puedes encotrarlo si te vas a Cloud, buscas modelos de  Model Registry, te vas al menu desplegable de la izquierda y pulsas "Endpoints"

---
¡Listo! Tu modelo está ahora gestionado correctamente. 🏆
