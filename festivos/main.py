import logging
import os
from datetime import datetime

import functions_framework
import pandas as pd
import requests
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET = "airbnb_features"
TABLE = "holidays"
TABLE_REF = f"{PROJECT_ID}.{DATASET}.{TABLE}"

API_BASE = "https://date.nager.at/api/v3/PublicHolidays"
COUNTRY = "ES"
VALENCIA_REGION = "ES-VC"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_holidays(year: int) -> list[dict]:
    url = f"{API_BASE}/{year}/{COUNTRY}"
    logger.info(f"Fetching holidays: {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    holidays = response.json()
    logger.info(f"API devolvio {len(holidays)} festivos para {year}")
    return holidays
def transform_holidays(holidays: list[dict]) -> pd.DataFrame:
    """
    Transforma todos los festivos de España.
    Marca cuáles son nacionales, cuáles aplican a Valencia,
    y guarda las comunidades afectadas.
    """
    rows = []

    for h in holidays:
        counties = h.get("counties")

        rows.append({
            "date": h["date"],
            "local_name": h.get("localName"),
            "name": h.get("name"),
            "country_code": h.get("countryCode", COUNTRY),
            "is_national": counties is None,
            "applies_to_valencia": counties is None or (counties is not None and VALENCIA_REGION in counties),
            "counties": ", ".join(counties) if counties else None,
            "holiday_type": ", ".join(h.get("types", [])),
            "fixed": h.get("fixed", False),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["ingested_at"] = datetime.utcnow()

    logger.info(f"Festivos España: {len(df)} total | Aplican a Valencia: {df['applies_to_valencia'].sum()}")
    return df

def check_already_ingested(year: int) -> bool:
    """Comprueba si ya se ingesto este año."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT COUNT(*) as cnt
        FROM `{TABLE_REF}`
        WHERE EXTRACT(YEAR FROM date) = {year}
        LIMIT 1
    """
    try:
        result = client.query(query).result()
        for row in result:
            if row.cnt > 0:
                return True
    except Exception:
        pass
    return False


def load_to_bigquery(df: pd.DataFrame):
    """Carga el DataFrame a BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    logger.info(f"Cargando {len(df)} festivos a {TABLE_REF}...")
    job = client.load_table_from_dataframe(df, TABLE_REF, job_config=job_config)
    job.result()
    logger.info(f"Cargado OK: {job.output_rows} filas en {TABLE_REF}")

@functions_framework.http
def ingest_holidays(request):
    """
    HTTP entry point.

    Query params:
        ?year=2026           -> ingesta un año
        ?year=2025,2026      -> ingesta varios años
        ?force=true          -> re-ingesta aunque ya existan
    """
    year_param = request.args.get("year", str(datetime.now().year))
    years = [int(y.strip()) for y in year_param.split(",")]
    force = request.args.get("force", "false").lower() == "true"

    logger.info(f"=== Inicio ingesta festivos Valencia ===")
    logger.info(f"Años: {years} | Force: {force}")

    all_results = []

    for year in years:
        try:
            if not force and check_already_ingested(year):
                msg = f"Año {year} ya ingestado, skipping"
                logger.info(msg)
                all_results.append({"year": year, "status": msg})
                continue

            holidays = fetch_holidays(year)
            df = transform_holidays(holidays)

            if df.empty:
                msg = f"No hay festivos para Valencia en {year}"
                logger.warning(msg)
                all_results.append({"year": year, "status": msg})
                continue

            load_to_bigquery(df)
            all_results.append({
                "year": year,
                "status": "OK",
                "holidays_loaded": len(df),
            })

        except Exception as e:
            error_msg = f"ERROR - {str(e)}"
            logger.error(f"Año {year}: {error_msg}", exc_info=True)
            all_results.append({"year": year, "status": error_msg})

    response = {
        "region": "Valencia (ES-VC)",
        "results": all_results,
    }

    logger.info(f"=== Fin ingesta festivos: {response} ===")
    return (response, 200)