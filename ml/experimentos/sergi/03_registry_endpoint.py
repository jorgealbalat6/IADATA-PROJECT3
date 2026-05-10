"""
03_registry_endpoint.py
========================
Construye la imagen Docker del servidor de predicción, la registra en
Vertex AI Model Registry y despliega un Vertex AI Endpoint.

Uso rápido:
    from registry_endpoint import VertexDeployer
    import config as CFG

    deployer = VertexDeployer(CFG)
    deployer.build_and_push()
    model    = deployer.register_model()
    endpoint = deployer.create_endpoint()
    deployer.deploy(model, endpoint)

Desde CLI:
    python 03_registry_endpoint.py --all
    python 03_registry_endpoint.py --build
    python 03_registry_endpoint.py --register
    python 03_registry_endpoint.py --deploy --model-name <resource> --endpoint-name <resource>
"""

import argparse
import logging
import subprocess
from typing import Optional

from google.cloud import aiplatform

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


class VertexDeployer:
    """
    Orquesta el ciclo completo:
        build Docker → Model Registry → Vertex AI Endpoint

    Compatible con cualquier modelo: toda la config viene de config.py.
    """

    def __init__(self, cfg):
        """
        Parameters
        ----------
        cfg : módulo config.py
            Debe exponer: PROJECT_ID, REGION, GCS_BUCKET, IMAGE, ARTIFACT_REPO,
            LOCATION, SERVING_DIR, MODEL_DISPLAY_NAME, ENDPOINT_DISPLAY_NAME,
            SERVING_PREDICT_ROUTE, SERVING_HEALTH_ROUTE, SERVING_PORT,
            CONTAINER_ENV_VARS, MACHINE_TYPE, MIN_REPLICA_COUNT,
            MAX_REPLICA_COUNT, SA_EMAIL
        """
        self.cfg = cfg
        aiplatform.init(
            project=cfg.PROJECT_ID,
            location=cfg.REGION,
            staging_bucket=f"gs://{cfg.GCS_BUCKET}",
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
        """Ejecuta un subproceso y muestra su salida."""
        logger.info(f"$ {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout[-4000:])
        if result.returncode != 0:
            logger.error(result.stderr[-2000:])
            if check:
                raise RuntimeError(f"Comando fallido: {' '.join(cmd)}")
        return result

    # ── Paso 1: Build & Push ──────────────────────────────────────────────────

    def ensure_artifact_registry(self) -> None:
        """Crea el repositorio de Artifact Registry si no existe (idempotente)."""
        logger.info(f"Verificando repositorio '{self.cfg.ARTIFACT_REPO}'...")
        r = self._run([
            "gcloud", "artifacts", "repositories", "create", self.cfg.ARTIFACT_REPO,
            "--repository-format=docker",
            f"--location={self.cfg.LOCATION}",
            "--description=Repositorio Docker de modelos ML",
            f"--project={self.cfg.PROJECT_ID}",
        ], check=False)
        if r.returncode == 0:
            logger.info("  Repositorio creado")
        else:
            logger.info("  Repositorio ya existía (OK)")

    def build_and_push(self) -> None:
        """
        Construye la imagen Docker con Cloud Build y la sube a Artifact Registry.
        No necesita Docker instalado localmente. Tarda ~3-5 min.
        """
        self.ensure_artifact_registry()
        logger.info(f"Build y push: {self.cfg.IMAGE}  (~3-5 min)")
        self._run([
            "gcloud", "builds", "submit",
            "--tag", self.cfg.IMAGE,
            "--project", self.cfg.PROJECT_ID,
            self.cfg.SERVING_DIR,
        ])
        logger.info(f"✅ Imagen disponible: {self.cfg.IMAGE}")

    # ── Paso 2: Model Registry ────────────────────────────────────────────────

    def register_model(self) -> aiplatform.Model:
        """
        Registra el modelo en Vertex AI Model Registry.

        Returns
        -------
        aiplatform.Model
        """
        logger.info(f"Registrando '{self.cfg.MODEL_DISPLAY_NAME}' en Model Registry...")
        model = aiplatform.Model.upload(
            display_name=self.cfg.MODEL_DISPLAY_NAME,
            serving_container_image_uri=self.cfg.IMAGE,
            serving_container_environment_variables=self.cfg.CONTAINER_ENV_VARS,
            serving_container_predict_route=self.cfg.SERVING_PREDICT_ROUTE,
            serving_container_health_route=self.cfg.SERVING_HEALTH_ROUTE,
            serving_container_ports=[self.cfg.SERVING_PORT],
        )
        logger.info(f"✅ Modelo registrado: {model.resource_name}")
        return model

    def get_or_create_model(self) -> aiplatform.Model:
        """
        Devuelve el modelo más reciente con el display_name configurado,
        o lo registra si no existe.
        """
        models = aiplatform.Model.list(
            filter=f'display_name="{self.cfg.MODEL_DISPLAY_NAME}"',
            order_by="create_time desc",
        )
        if models:
            logger.info(f"Modelo ya existente: {models[0].resource_name}")
            return models[0]
        return self.register_model()

    # ── Paso 3: Endpoint ──────────────────────────────────────────────────────

    def create_endpoint(self) -> aiplatform.Endpoint:
        """
        Crea un nuevo Vertex AI Endpoint.

        Returns
        -------
        aiplatform.Endpoint
        """
        logger.info(f"Creando endpoint '{self.cfg.ENDPOINT_DISPLAY_NAME}'...")
        endpoint = aiplatform.Endpoint.create(
            display_name=self.cfg.ENDPOINT_DISPLAY_NAME
        )
        logger.info(f"✅ Endpoint creado: {endpoint.resource_name}")
        return endpoint

    def get_or_create_endpoint(self) -> aiplatform.Endpoint:
        """
        Devuelve el endpoint más reciente con el display_name configurado,
        o lo crea si no existe.
        """
        endpoints = aiplatform.Endpoint.list(
            filter=f'display_name="{self.cfg.ENDPOINT_DISPLAY_NAME}"',
            order_by="create_time desc",
        )
        if endpoints:
            logger.info(f"Endpoint ya existente: {endpoints[0].resource_name}")
            return endpoints[0]
        return self.create_endpoint()

    def deploy(
        self,
        model: aiplatform.Model,
        endpoint: aiplatform.Endpoint,
    ) -> None:
        """
        Despliega el modelo en el endpoint. Tarda ~10-20 min la primera vez.

        Parameters
        ----------
        model    : aiplatform.Model    — modelo registrado en Model Registry
        endpoint : aiplatform.Endpoint — endpoint destino
        """
        logger.info("Desplegando modelo en el endpoint (~10-20 min)...")
        model.deploy(
            endpoint=endpoint,
            machine_type=self.cfg.MACHINE_TYPE,
            min_replica_count=self.cfg.MIN_REPLICA_COUNT,
            max_replica_count=self.cfg.MAX_REPLICA_COUNT,
            traffic_split={"0": 100},
            service_account=self.cfg.SA_EMAIL,
        )
        logger.info(f"✅ Modelo desplegado en: {endpoint.resource_name}")

    # ── Flujo completo ────────────────────────────────────────────────────────

    def run_all(self) -> aiplatform.Endpoint:
        """
        Ejecuta el ciclo completo:
            build → register → create endpoint → deploy

        Returns
        -------
        aiplatform.Endpoint — endpoint listo para recibir predicciones
        """
        self.build_and_push()
        model    = self.register_model()
        endpoint = self.create_endpoint()
        self.deploy(model, endpoint)
        return endpoint


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(
        description="Build Docker → Model Registry → Vertex AI Endpoint"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Ejecuta el ciclo completo: build + register + endpoint + deploy"
    )
    parser.add_argument(
        "--build", action="store_true",
        help="Solo construye y sube la imagen Docker"
    )
    parser.add_argument(
        "--register", action="store_true",
        help="Solo registra el modelo en Model Registry (la imagen debe existir)"
    )
    parser.add_argument(
        "--deploy", action="store_true",
        help="Solo despliega. Requiere --model-name y --endpoint-name"
    )
    parser.add_argument(
        "--model-name", metavar="RESOURCE_NAME",
        help="Resource name del modelo ya registrado (para --deploy)"
    )
    parser.add_argument(
        "--endpoint-name", metavar="RESOURCE_NAME",
        help="Resource name del endpoint destino (para --deploy)"
    )
    args = parser.parse_args()

    import config as CFG
    deployer = VertexDeployer(CFG)

    if args.all:
        endpoint = deployer.run_all()
        logger.info(f"Endpoint listo: {endpoint.resource_name}")

    elif args.build:
        deployer.build_and_push()

    elif args.register:
        model = deployer.register_model()
        logger.info(f"Resource name: {model.resource_name}")

    elif args.deploy:
        if not args.model_name or not args.endpoint_name:
            parser.error("--deploy requiere --model-name y --endpoint-name")
        model    = aiplatform.Model(args.model_name)
        endpoint = aiplatform.Endpoint(args.endpoint_name)
        deployer.deploy(model, endpoint)

    else:
        parser.print_help()


if __name__ == "__main__":
    _cli()
