"""
Cloud Run: Ejecuta dbt run para transformar datos de Airbnb Barcelona.

Endpoints:
    GET/POST /                   -> dbt run completo
    GET/POST /?select=marts      -> solo modelos de marts
    GET/POST /?full_refresh=true -> dbt run --full-refresh
"""

import logging
import subprocess

import functions_framework

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@functions_framework.http
def run_dbt(request):
    """HTTP entry point."""
    select = request.args.get("select", None)
    full_refresh = request.args.get("full_refresh", "false").lower() == "true"

    cmd = ["dbt", "run", "--profiles-dir", "/app", "--project-dir", "/app"]

    if select:
        cmd += ["--select", select]

    if full_refresh:
        cmd += ["--full-refresh"]

    logger.info(f"Ejecutando: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )

        logger.info(result.stdout)
        if result.stderr:
            logger.warning(result.stderr)

        if result.returncode == 0:
            return ({"status": "OK", "output": result.stdout[-2000:]}, 200)
        else:
            return ({"status": "ERROR", "output": result.stdout[-2000:], "error": result.stderr[-2000:]}, 500)

    except subprocess.TimeoutExpired:
        return ({"status": "TIMEOUT"}, 500)
    except Exception as e:
        return ({"status": "ERROR", "error": str(e)}, 500)