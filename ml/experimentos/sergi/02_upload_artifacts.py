"""
02_upload_artifacts.py
======================
Sube los artefactos del modelo entrenado a Google Cloud Storage.

Uso rápido:
    from upload_artifacts import ArtifactUploader
    import config as CFG

    uploader = ArtifactUploader(CFG)
    uploader.upload_configs()                          # sube bq_config.json y feature_config.json
    uploader.upload_pkl(model,        CFG.ARTIFACTS["model"])
    uploader.upload_pkl(preprocessor, CFG.ARTIFACTS["preprocessor"])
    uploader.upload_json(neigh_dict,  CFG.ARTIFACTS["neigh_mean_price"])
    uploader.verify()

Desde CLI:
    python 02_upload_artifacts.py --configs            # solo sube los JSON de config
    python 02_upload_artifacts.py --verify             # verifica que todos los artefactos existen
    python 02_upload_artifacts.py --list               # lista el contenido del bucket
    python 02_upload_artifacts.py --file ruta/model.pkl --blob occupancy/model.pkl
"""

import argparse
import io
import json
import logging
import pickle
from pathlib import Path

from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


class ArtifactUploader:
    """
    Sube artefactos de ML a un bucket de GCS.

    Compatible con cualquier proyecto: toda la config viene de config.py.
    """

    def __init__(self, cfg):
        """
        Parameters
        ----------
        cfg : módulo config.py
            Debe exponer: PROJECT_ID, GCS_BUCKET, REGION, MODEL_PREFIX,
                          ARTIFACTS, BQ_CONFIG, FEATURE_CONFIG
        """
        self.cfg    = cfg
        self.client = storage.Client(project=cfg.PROJECT_ID)
        self.bucket = self._get_or_create_bucket()

    # ── Bucket ────────────────────────────────────────────────────────────────

    def _get_or_create_bucket(self) -> storage.Bucket:
        try:
            bucket = self.client.create_bucket(
                self.cfg.GCS_BUCKET,
                location=self.cfg.REGION,
            )
            logger.info(f"Bucket creado: gs://{self.cfg.GCS_BUCKET}")
        except Exception:
            bucket = self.client.bucket(self.cfg.GCS_BUCKET)
            logger.info(f"Bucket existente: gs://{self.cfg.GCS_BUCKET}")
        return bucket

    # ── Métodos de subida ─────────────────────────────────────────────────────

    def upload_pkl(self, obj: object, blob_path: str) -> None:
        """Serializa un objeto Python con pickle y lo sube a GCS en memoria."""
        buf = io.BytesIO()
        pickle.dump(obj, buf)
        buf.seek(0)
        self.bucket.blob(blob_path).upload_from_file(
            buf, content_type="application/octet-stream"
        )
        logger.info(f"✅ Subido (pkl) : gs://{self.cfg.GCS_BUCKET}/{blob_path}")

    def upload_json(self, obj: dict | list, blob_path: str) -> None:
        """Serializa un dict/list como JSON y lo sube a GCS en memoria."""
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.bucket.blob(blob_path).upload_from_string(
            data, content_type="application/json"
        )
        logger.info(f"✅ Subido (json): gs://{self.cfg.GCS_BUCKET}/{blob_path}")

    def upload_file(self, local_path: str, blob_path: str) -> None:
        """Sube un fichero local (pkl, json, csv…) directamente a GCS."""
        self.bucket.blob(blob_path).upload_from_filename(local_path)
        size_kb = Path(local_path).stat().st_size / 1024
        logger.info(
            f"✅ Subido (file): gs://{self.cfg.GCS_BUCKET}/{blob_path}"
            f"  ← {local_path}  ({size_kb:.1f} KB)"
        )

    def upload_all_files(self, artifacts: dict[str, str]) -> None:
        """
        Sube varios ficheros locales de una vez.

        Parameters
        ----------
        artifacts : dict[str, str]
            { local_path: blob_path }  Ej: {"./model.pkl": "occupancy/model.pkl"}
        """
        for local_path, blob_path in artifacts.items():
            self.upload_file(local_path, blob_path)

    def upload_configs(self) -> None:
        """
        Genera y sube a GCS los ficheros de configuración derivados de config.py:
            - bq_config.json      ← BQ_CONFIG
            - feature_config.json ← FEATURE_CONFIG

        Llama a este método SIEMPRE antes de build_and_push para que el
        contenedor tenga la configuración más reciente.
        """
        logger.info("Subiendo configuraciones desde config.py → GCS...")

        self.upload_json(
            self.cfg.BQ_CONFIG,
            self.cfg.ARTIFACTS["bq_config"],
        )
        self.upload_json(
            self.cfg.FEATURE_CONFIG,
            self.cfg.ARTIFACTS["feature_config"],
        )
        logger.info("✅ Configuraciones subidas")

    # ── Verificación ──────────────────────────────────────────────────────────

    def verify(self) -> dict[str, bool]:
        """
        Comprueba que todos los artefactos definidos en config.ARTIFACTS
        existen en GCS.

        Returns
        -------
        dict[str, bool]
            { nombre_artefacto: existe }
        """
        logger.info(
            f"Verificando artefactos en gs://{self.cfg.GCS_BUCKET}/{self.cfg.MODEL_PREFIX}/"
        )
        results = {}
        for name, blob_path in self.cfg.ARTIFACTS.items():
            exists = self.bucket.blob(blob_path).exists()
            status = "✅" if exists else "❌"
            logger.info(f"  {status} {name:20s} → {blob_path}")
            results[name] = exists

        missing = [k for k, v in results.items() if not v]
        if missing:
            logger.warning(f"Artefactos NO encontrados: {missing}")
        else:
            logger.info("Todos los artefactos están presentes en GCS.")
        return results

    def list_blobs(self) -> list[str]:
        """Lista todos los blobs bajo el prefijo MODEL_PREFIX."""
        blobs = list(
            self.client.list_blobs(
                self.cfg.GCS_BUCKET, prefix=f"{self.cfg.MODEL_PREFIX}/"
            )
        )
        logger.info(
            f"Contenido de gs://{self.cfg.GCS_BUCKET}/{self.cfg.MODEL_PREFIX}/"
        )
        for b in blobs:
            logger.info(f"  {b.name:<55} {b.size / 1024:>8.1f} KB")
        return [b.name for b in blobs]


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(description="Sube artefactos ML a GCS")
    parser.add_argument(
        "--configs", action="store_true",
        help="Genera y sube bq_config.json y feature_config.json desde config.py"
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Verifica que los artefactos de config.ARTIFACTS existen en GCS"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Lista todos los blobs bajo MODEL_PREFIX"
    )
    parser.add_argument(
        "--file", metavar="LOCAL_PATH",
        help="Ruta local del fichero a subir"
    )
    parser.add_argument(
        "--blob", metavar="BLOB_PATH",
        help="Ruta destino en GCS (relativa al bucket)"
    )
    args = parser.parse_args()

    import config as CFG
    uploader = ArtifactUploader(CFG)

    if args.configs:
        uploader.upload_configs()
    if args.verify:
        uploader.verify()
    if args.list:
        uploader.list_blobs()
    if args.file and args.blob:
        uploader.upload_file(args.file, args.blob)
    if not any(vars(args).values()):
        parser.print_help()


if __name__ == "__main__":
    _cli()

import argparse
import io
import json
import logging
import pickle
from pathlib import Path

from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


class ArtifactUploader:
    """
    Sube artefactos de ML a un bucket de GCS.

    Compatible con cualquier proyecto: toda la config viene de config.py.
    """

    def __init__(self, cfg):
        """
        Parameters
        ----------
        cfg : módulo config.py
            Debe exponer: PROJECT_ID, GCS_BUCKET, REGION, MODEL_PREFIX, ARTIFACTS
        """
        self.cfg    = cfg
        self.client = storage.Client(project=cfg.PROJECT_ID)
        self.bucket = self._get_or_create_bucket()

    # ── Bucket ────────────────────────────────────────────────────────────────

    def _get_or_create_bucket(self) -> storage.Bucket:
        try:
            bucket = self.client.create_bucket(
                self.cfg.GCS_BUCKET,
                location=self.cfg.REGION,
            )
            logger.info(f"Bucket creado: gs://{self.cfg.GCS_BUCKET}")
        except Exception:
            bucket = self.client.bucket(self.cfg.GCS_BUCKET)
            logger.info(f"Bucket existente: gs://{self.cfg.GCS_BUCKET}")
        return bucket

    # ── Métodos de subida ─────────────────────────────────────────────────────

    def upload_pkl(self, obj: object, blob_path: str) -> None:
        """Serializa un objeto Python con pickle y lo sube a GCS en memoria."""
        buf = io.BytesIO()
        pickle.dump(obj, buf)
        buf.seek(0)
        self.bucket.blob(blob_path).upload_from_file(
            buf, content_type="application/octet-stream"
        )
        logger.info(f"✅ Subido (pkl): gs://{self.cfg.GCS_BUCKET}/{blob_path}")

    def upload_json(self, obj: dict, blob_path: str) -> None:
        """Serializa un dict como JSON y lo sube a GCS en memoria."""
        data = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.bucket.blob(blob_path).upload_from_string(
            data, content_type="application/json"
        )
        logger.info(f"✅ Subido (json): gs://{self.cfg.GCS_BUCKET}/{blob_path}")

    def upload_file(self, local_path: str, blob_path: str) -> None:
        """Sube un fichero local (pkl, json, csv…) directamente a GCS."""
        self.bucket.blob(blob_path).upload_from_filename(local_path)
        size_kb = Path(local_path).stat().st_size / 1024
        logger.info(
            f"✅ Subido (file): gs://{self.cfg.GCS_BUCKET}/{blob_path}"
            f"  ← {local_path}  ({size_kb:.1f} KB)"
        )

    def upload_all_files(self, artifacts: dict[str, str]) -> None:
        """
        Sube varios ficheros locales de una vez.

        Parameters
        ----------
        artifacts : dict[str, str]
            { local_path: blob_path }  Ej: {"./model.pkl": "occupancy/model.pkl"}
        """
        for local_path, blob_path in artifacts.items():
            self.upload_file(local_path, blob_path)

    # ── Verificación ──────────────────────────────────────────────────────────

    def verify(self) -> dict[str, bool]:
        """
        Comprueba que todos los artefactos definidos en config.ARTIFACTS
        existen en GCS.

        Returns
        -------
        dict[str, bool]
            { nombre_artefacto: existe }
        """
        logger.info(
            f"Verificando artefactos en gs://{self.cfg.GCS_BUCKET}/{self.cfg.MODEL_PREFIX}/"
        )
        results = {}
        for name, blob_path in self.cfg.ARTIFACTS.items():
            exists = self.bucket.blob(blob_path).exists()
            status = "✅" if exists else "❌"
            logger.info(f"  {status} {name:25s} → {blob_path}")
            results[name] = exists

        missing = [k for k, v in results.items() if not v]
        if missing:
            logger.warning(f"Artefactos NO encontrados: {missing}")
        else:
            logger.info("Todos los artefactos están presentes en GCS.")
        return results

    def list_blobs(self) -> list[str]:
        """Lista todos los blobs bajo el prefijo MODEL_PREFIX."""
        blobs = list(
            self.client.list_blobs(
                self.cfg.GCS_BUCKET, prefix=f"{self.cfg.MODEL_PREFIX}/"
            )
        )
        logger.info(
            f"Contenido de gs://{self.cfg.GCS_BUCKET}/{self.cfg.MODEL_PREFIX}/"
        )
        for b in blobs:
            logger.info(f"  {b.name:<55} {b.size / 1024:>8.1f} KB")
        return [b.name for b in blobs]


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(description="Sube artefactos ML a GCS")
    parser.add_argument(
        "--verify", action="store_true",
        help="Verifica que los artefactos de config.ARTIFACTS existen en GCS"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Lista todos los blobs bajo MODEL_PREFIX"
    )
    parser.add_argument(
        "--file", metavar="LOCAL_PATH",
        help="Ruta local del fichero a subir"
    )
    parser.add_argument(
        "--blob", metavar="BLOB_PATH",
        help="Ruta destino en GCS (relativa al bucket)"
    )
    args = parser.parse_args()

    import config as CFG
    uploader = ArtifactUploader(CFG)

    if args.verify:
        uploader.verify()
    if args.list:
        uploader.list_blobs()
    if args.file and args.blob:
        uploader.upload_file(args.file, args.blob)
    if not any(vars(args).values()):
        parser.print_help()


if __name__ == "__main__":
    _cli()
