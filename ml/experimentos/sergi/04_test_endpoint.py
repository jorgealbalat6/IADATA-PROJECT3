"""
04_test_endpoint.py
====================
Pruebas del Vertex AI Endpoint: predicción individual, carga concurrente
y validación de manejo de errores.

Uso rápido:
    from test_endpoint import EndpointTester
    import config as CFG

    tester = EndpointTester(CFG, endpoint_resource_name="projects/.../endpoints/...")
    tester.test_single(instance)
    tester.test_batch(instances)
    tester.test_load(n=50, workers=8)
    tester.test_errors()
    tester.run_all()

Desde CLI:
    python 04_test_endpoint.py --endpoint projects/.../endpoints/123 --all
    python 04_test_endpoint.py --endpoint projects/.../endpoints/123 --load --n 100
    python 04_test_endpoint.py --endpoint projects/.../endpoints/123 --single
"""

import argparse
import concurrent.futures
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from google.cloud import aiplatform

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


# ── Resultado de un test ──────────────────────────────────────────────────────

@dataclass
class TestResult:
    ok: bool
    latencia: float
    prediction: Any = None
    error: Optional[str] = None

    def __str__(self):
        status = "✅" if self.ok else "❌"
        return f"{status}  lat={self.latencia:.2f}s  pred={self.prediction}"


# ── Clase principal ───────────────────────────────────────────────────────────

class EndpointTester:
    """
    Prueba un Vertex AI Endpoint de forma sistemática.

    Compatible con cualquier modelo: los casos de prueba se pasan como
    parámetros, y la instancia base se configura en `default_instance`.
    """

    # Instancia de ejemplo para el modelo de ocupación de Barcelona.
    # Sustituye este dict por el schema de tu modelo si lo reutilizas.
    DEFAULT_INSTANCE = {
        "fecha":                  "2026-05-16",
        "neighbourhood_cleansed": "la Barceloneta",
        "room_type":              "Entire home/apt",
        "accommodates":           4,
        "listing_price":          180.0,
        "minimum_nights":         2,
        "number_of_reviews":      120,
        "review_scores_rating":   4.9,
        "instant_bookable":       True,
    }

    def __init__(self, cfg, endpoint_resource_name: str):
        """
        Parameters
        ----------
        cfg : módulo config.py
        endpoint_resource_name : str
            Resource name completo del endpoint.
            Ej: "projects/project3grupo1/locations/europe-west1/endpoints/123456"
        """
        aiplatform.init(project=cfg.PROJECT_ID, location=cfg.REGION)
        self.endpoint = aiplatform.Endpoint(endpoint_resource_name)
        self.cfg      = cfg
        logger.info(f"Endpoint: {self.endpoint.display_name}  ({endpoint_resource_name})")

    # ── Llamada base ──────────────────────────────────────────────────────────

    def _call(self, instance: dict) -> TestResult:
        """Realiza una única llamada al endpoint y devuelve un TestResult."""
        t0 = time.time()
        try:
            resp = self.endpoint.predict(instances=[instance])
            pred = resp.predictions[0]
            ok   = "error" not in pred
            return TestResult(
                ok=ok,
                latencia=time.time() - t0,
                prediction=pred,
                error=pred.get("error") if not ok else None,
            )
        except Exception as exc:
            return TestResult(ok=False, latencia=time.time() - t0, error=str(exc))

    # ── Test 1: predicción individual ─────────────────────────────────────────

    def test_single(
        self,
        instances: Optional[list[dict]] = None,
        label: Optional[list[str]] = None,
    ) -> list[TestResult]:
        """
        Prueba una lista de instancias de forma secuencial.

        Parameters
        ----------
        instances : list[dict] | None
            Si None usa DEFAULT_INSTANCE.
        label : list[str] | None
            Etiquetas para el log (misma longitud que instances).

        Returns
        -------
        list[TestResult]
        """
        if instances is None:
            instances = [self.DEFAULT_INSTANCE]
        if label is None:
            label = [f"Caso {i+1}" for i in range(len(instances))]

        print("\n" + "=" * 65)
        print(f"{'TEST 1 — PREDICCIONES INDIVIDUALES':^65}")
        print("=" * 65)

        results = []
        for lbl, inst in zip(label, instances):
            r = self._call(inst)
            results.append(r)
            pred = r.prediction or {}
            print(f"\n  📌 {lbl}")
            print(f"     fecha        : {pred.get('fecha', '—')}")
            print(f"     probabilidad : {pred.get('probabilidad', '—')}")
            print(f"     latencia     : {r.latencia:.2f}s")
            if r.error:
                print(f"     ⚠️  error: {r.error}")
        return results

    # ── Test 2: lote (batch) ──────────────────────────────────────────────────

    def test_batch(self, instances: list[dict]) -> list[TestResult]:
        """
        Envía todas las instancias en una sola llamada al endpoint
        (Vertex AI admite múltiples instancias por petición).

        Returns
        -------
        list[TestResult]
        """
        print("\n" + "=" * 65)
        print(f"{'TEST 2 — BATCH ({} instancias)'.format(len(instances)):^65}")
        print("=" * 65)

        t0   = time.time()
        resp = self.endpoint.predict(instances=instances)
        lat  = time.time() - t0

        results = []
        for i, pred in enumerate(resp.predictions):
            ok = "error" not in pred
            r  = TestResult(ok=ok, latencia=lat, prediction=pred)
            results.append(r)
            status = "✅" if ok else "❌"
            print(f"  {status} [{i+1:>2}] {pred}")

        logger.info(f"Batch completado: {len(instances)} instancias en {lat:.2f}s")
        return results

    # ── Test 3: carga concurrente ─────────────────────────────────────────────

    def test_load(
        self,
        n: int = 50,
        workers: int = 8,
        base_instance: Optional[dict] = None,
    ) -> dict:
        """
        Envía n peticiones concurrentes con variaciones aleatorias en el precio
        y el número de reviews.

        Parameters
        ----------
        n        : int  — número total de peticiones
        workers  : int  — hilos concurrentes
        base_instance : dict | None — instancia base (usa DEFAULT_INSTANCE si None)

        Returns
        -------
        dict — resumen con media, p50, p95, errores
        """
        base = base_instance or self.DEFAULT_INSTANCE

        def _variant(_):
            inst = {
                **base,
                "listing_price":      round(random.uniform(40, 300), 2),
                "number_of_reviews":  random.randint(0, 200),
            }
            return self._call(inst)

        print("\n" + "=" * 75)
        print(f"{'TEST 3 — CARGA CONCURRENTE':^75}")
        print("=" * 75)
        print(f"  n={n} | workers={workers}")

        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(_variant, range(n)))
        total = time.time() - t0

        lats    = sorted(r.latencia for r in results)
        errores = sum(1 for r in results if not r.ok)
        media   = sum(lats) / len(lats)
        p50     = lats[int(len(lats) * 0.50)]
        p95     = lats[int(len(lats) * 0.95)]

        summary = {
            "n": n, "errores": errores,
            "media_s": round(media, 3),
            "p50_s":   round(p50, 3),
            "p95_s":   round(p95, 3),
            "total_s": round(total, 3),
        }
        print(
            f"  errores={errores} | media={media:.2f}s | "
            f"p50={p50:.2f}s | p95={p95:.2f}s | total={total:.1f}s"
        )
        return summary

    # ── Test 4: errores esperados ─────────────────────────────────────────────

    def test_errors(self, error_cases: Optional[list[dict]] = None) -> list[TestResult]:
        """
        Valida que el servidor maneja correctamente entradas inválidas.

        Parameters
        ----------
        error_cases : list[dict] | None
            Lista de dicts con keys "label" e "instance".
            Si None usa los casos de error por defecto del modelo de ocupación.

        Returns
        -------
        list[TestResult]
        """
        default_cases = [
            {
                "label": "Sin campo 'fecha' → debe devolver error",
                "instance": {
                    k: v for k, v in self.DEFAULT_INSTANCE.items() if k != "fecha"
                },
            },
            {
                "label": "Fecha sin datos en BigQuery → debe devolver error",
                "instance": {**self.DEFAULT_INSTANCE, "fecha": "1999-01-01"},
            },
        ]
        cases = error_cases or default_cases

        print("\n" + "=" * 65)
        print(f"{'TEST 4 — VALIDACIÓN DE ERRORES':^65}")
        print("=" * 65)

        results = []
        for tc in cases:
            r = self._call(tc["instance"])
            error_capturado = r.error is not None or (
                isinstance(r.prediction, dict) and "error" in r.prediction
            )
            status = "✅ error capturado" if error_capturado else "⚠️  sin error (inesperado)"
            print(f"\n  📌 {tc['label']}")
            print(f"     Resultado : {status}")
            msg = r.error or (r.prediction or {}).get("error", r.prediction)
            print(f"     Mensaje   : {msg}")
            results.append(r)
        return results

    # ── Flujo completo ────────────────────────────────────────────────────────

    def run_all(
        self,
        single_instances: Optional[list[dict]] = None,
        single_labels: Optional[list[str]] = None,
        load_n: int = 50,
        load_workers: int = 8,
        error_cases: Optional[list[dict]] = None,
    ) -> dict:
        """
        Ejecuta los 4 tests en secuencia y devuelve un resumen.

        Returns
        -------
        dict — con keys "single", "load", "errors"
        """
        single  = self.test_single(single_instances, single_labels)
        load    = self.test_load(n=load_n, workers=load_workers)
        errors  = self.test_errors(error_cases)

        summary = {
            "single_ok":   sum(1 for r in single if r.ok),
            "single_fail": sum(1 for r in single if not r.ok),
            "load":        load,
            "errors_ok":   sum(1 for r in errors if r.error or (
                isinstance(r.prediction, dict) and "error" in r.prediction
            )),
        }
        print("\n" + "=" * 65)
        print(f"{'RESUMEN FINAL':^65}")
        print("=" * 65)
        print(f"  Individuales OK  : {summary['single_ok']}")
        print(f"  Individuales KO  : {summary['single_fail']}")
        print(f"  Carga — p95      : {summary['load']['p95_s']}s")
        print(f"  Carga — errores  : {summary['load']['errores']}")
        print(f"  Errores capturados: {summary['errors_ok']}/{len(errors)}")
        return summary


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli():
    parser = argparse.ArgumentParser(description="Prueba un Vertex AI Endpoint")
    parser.add_argument(
        "--endpoint", required=True, metavar="RESOURCE_NAME",
        help="Resource name del endpoint. Ej: projects/.../endpoints/123"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Ejecuta todos los tests"
    )
    parser.add_argument(
        "--single", action="store_true",
        help="Prueba de predicción individual con la instancia por defecto"
    )
    parser.add_argument(
        "--load", action="store_true",
        help="Prueba de carga concurrente"
    )
    parser.add_argument(
        "--n", type=int, default=50,
        help="Número de peticiones para la prueba de carga (default: 50)"
    )
    parser.add_argument(
        "--workers", type=int, default=8,
        help="Hilos concurrentes para la prueba de carga (default: 8)"
    )
    parser.add_argument(
        "--errors", action="store_true",
        help="Prueba de validación de errores"
    )
    args = parser.parse_args()

    import config as CFG
    tester = EndpointTester(CFG, endpoint_resource_name=args.endpoint)

    if args.all:
        tester.run_all(load_n=args.n, load_workers=args.workers)
    else:
        if args.single:
            tester.test_single()
        if args.load:
            tester.test_load(n=args.n, workers=args.workers)
        if args.errors:
            tester.test_errors()
        if not (args.single or args.load or args.errors):
            parser.print_help()


if __name__ == "__main__":
    _cli()
