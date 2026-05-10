"""
upload_sergi.py
================
Script de PRUEBA para subir artefactos del Experimento 3 a GCS
y registrar/desplegar el modelo en Vertex AI usando el sufijo _sergi.

Usar DESPUÉS de entrenar el modelo en Experimento_3.ipynb o
Experimento_3_sergi.ipynb.

Flujo completo:
  1. Subida de artefactos a GCS     →  gs://project3grupo1-ml-models/occupancy_sergi/
  2. Build imagen Docker             →  europe-west1-docker.pkg.dev/.../occupancy-predictor-sergi
  3. Registro en Vertex AI           →  modelo "occupancy-predictor-sergi"
  4. Creación y deploy del endpoint  →  endpoint "occupancy-predictor-endpoint-sergi"

Uso desde un notebook (añadir celdas al final del Experimento 3):

    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.abspath(".")))

    import config_sergi as CFG
    import importlib, importlib.util

    # Cargar el módulo de upload (nombre con guión requiere importlib)
    spec = importlib.util.spec_from_file_location("upload_artifacts", "02_upload_artifacts.py")
    upload_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(upload_mod)
    ArtifactUploader = upload_mod.ArtifactUploader

    uploader = ArtifactUploader(CFG)

    # 1. Sube los JSONs de configuración
    uploader.upload_configs()

    # 2. Sube el modelo (objeto `model` del notebook)
    uploader.upload_pkl(model, CFG.ARTIFACTS["model"])

    # 3. Sube el preprocessor (si existe en el notebook como `preprocessor`)
    # uploader.upload_pkl(preprocessor, CFG.ARTIFACTS["preprocessor"])

    # 4. Sube el diccionario de precio medio por barrio (si existe como `neigh_mean_price`)
    # uploader.upload_json(neigh_mean_price, CFG.ARTIFACTS["neigh_mean_price"])

    # 5. Verifica que todo está en GCS
    uploader.verify()

Uso desde CLI (si tienes los ficheros guardados localmente):

    python upload_sergi.py --step upload --model-path ./model.pkl
    python upload_sergi.py --step registry
    python upload_sergi.py --step all --model-path ./model.pkl

"""

import argparse
import importlib.util
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ── Importar config_sergi ─────────────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import config_sergi as CFG  # noqa: E402

# ── Importar ArtifactUploader (nombre con guión requiere importlib) ───────────
_upload_spec = importlib.util.spec_from_file_location(
    "upload_artifacts",
    os.path.join(HERE, "02_upload_artifacts.py"),
)
_upload_mod = importlib.util.module_from_spec(_upload_spec)
_upload_spec.loader.exec_module(_upload_mod)
ArtifactUploader = _upload_mod.ArtifactUploader

# ── Importar VertexDeployer ───────────────────────────────────────────────────
_registry_spec = importlib.util.spec_from_file_location(
    "registry_endpoint",
    os.path.join(HERE, "03_registry_endpoint.py"),
)
_registry_mod = importlib.util.module_from_spec(_registry_spec)
_registry_spec.loader.exec_module(_registry_mod)
VertexDeployer = _registry_mod.VertexDeployer


# ── Paso 1: Upload de artefactos ──────────────────────────────────────────────

def step_upload(model_path: str, preprocessor_path: str = None, neigh_json_path: str = None):
    """
    Sube los artefactos del modelo a GCS bajo el prefijo occupancy_sergi/.

    Parámetros
    ----------
    model_path       : ruta local al fichero model.pkl
    preprocessor_path: ruta local al preprocessor.pkl (opcional)
    neigh_json_path  : ruta local al neigh_mean_price.json (opcional)
    """
    uploader = ArtifactUploader(CFG)

    logger.info("── Subiendo configuraciones (bq_config.json + feature_config.json)…")
    uploader.upload_configs()

    logger.info(f"── Subiendo modelo desde {model_path}…")
    uploader.upload_file(model_path, CFG.ARTIFACTS["model"])

    if preprocessor_path:
        logger.info(f"── Subiendo preprocessor desde {preprocessor_path}…")
        uploader.upload_file(preprocessor_path, CFG.ARTIFACTS["preprocessor"])

    if neigh_json_path:
        logger.info(f"── Subiendo neigh_mean_price desde {neigh_json_path}…")
        uploader.upload_file(neigh_json_path, CFG.ARTIFACTS["neigh_mean_price"])

    logger.info("── Verificando artefactos en GCS…")
    uploader.verify()
    uploader.list_blobs()


# ── Paso 2: Build Docker + registro en Vertex AI ──────────────────────────────

def step_registry():
    """
    Construye la imagen Docker con Cloud Build y registra el modelo
    en Vertex AI Model Registry usando el sufijo _sergi.

    Requiere: gcloud CLI autenticado y Cloud Build habilitado.
    Tarda ~3-5 minutos.
    """
    deployer = VertexDeployer(CFG)
    deployer.build_and_push()
    model = deployer.register_model()
    logger.info(f"✅ Modelo registrado: {model.resource_name}")
    return model


# ── Paso 3: Crear y desplegar endpoint ───────────────────────────────────────

def step_endpoint(model=None):
    """
    Crea el endpoint en Vertex AI y despliega el modelo.
    Si model es None, busca el más reciente con display_name=occupancy-predictor-sergi.

    Tarda ~10-20 minutos la primera vez.
    """
    deployer = VertexDeployer(CFG)

    if model is None:
        model = deployer.get_or_create_model()

    endpoint = deployer.create_endpoint()
    deployer.deploy(model, endpoint)
    logger.info(f"✅ Endpoint listo: {endpoint.resource_name}")
    return endpoint


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(
        description="Prueba de subida _sergi: upload GCS + Vertex AI Registry/Endpoint"
    )
    parser.add_argument(
        "--step",
        choices=["upload", "registry", "endpoint", "all"],
        required=True,
        help=(
            "upload   → Sube artefactos a GCS\n"
            "registry → Build Docker + registra modelo en Vertex AI\n"
            "endpoint → Crea y despliega el endpoint\n"
            "all      → Ejecuta los tres pasos seguidos"
        ),
    )
    parser.add_argument("--model-path", default="./model.pkl",
                        help="Ruta local al fichero model.pkl (para --step upload o all)")
    parser.add_argument("--preprocessor-path", default=None,
                        help="Ruta local al preprocessor.pkl (opcional)")
    parser.add_argument("--neigh-json-path", default=None,
                        help="Ruta local al neigh_mean_price.json (opcional)")
    args = parser.parse_args()

    if args.step in ("upload", "all"):
        step_upload(
            model_path=args.model_path,
            preprocessor_path=args.preprocessor_path,
            neigh_json_path=args.neigh_json_path,
        )

    model = None
    if args.step in ("registry", "all"):
        model = step_registry()

    if args.step in ("endpoint", "all"):
        step_endpoint(model=model)


if __name__ == "__main__":
    _cli()
