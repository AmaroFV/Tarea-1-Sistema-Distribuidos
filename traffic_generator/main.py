import os
import random
import time
from typing import Dict

import httpx
import numpy as np


ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]


def pick_zone(distribution: str, *, zipf_s: float) -> str:
    if distribution == "uniform":
        return random.choice(ZONES)

    # zipf: valores en [1..N], con más masa en 1
    n = len(ZONES)
    while True:
        k = int(np.random.zipf(zipf_s))
        if 1 <= k <= n:
            return ZONES[k - 1]


def build_query(distribution: str, *, zipf_s: float) -> Dict:
    q = random.choice(QUERY_TYPES)

    confidence_min = random.choice([0.0, 0.5, 0.8])
    bins = random.choice([5, 10])

    if q in {"Q1", "Q2", "Q3", "Q5"}:
        z = pick_zone(distribution, zipf_s=zipf_s)
        body = {
            "query_type": q,
            "zone_id": z,
            "confidence_min": confidence_min,
            "bins": bins,
        }
        # bins/confidence_min extra no molesta al backend
        return body

    # Q4: dos zonas distintas
    a = pick_zone(distribution, zipf_s=zipf_s)
    b = pick_zone(distribution, zipf_s=zipf_s)
    while b == a:
        b = pick_zone(distribution, zipf_s=zipf_s)

    return {
        "query_type": "Q4",
        "zone_id_a": a,
        "zone_id_b": b,
        "confidence_min": confidence_min,
    }


def main() -> None:
    cache_url = os.getenv("CACHE_URL", "http://cache_service:8001/query")
    distribution = os.getenv("DISTRIBUTION", "zipf").strip().lower()
    if distribution not in {"zipf", "uniform"}:
        distribution = "zipf"

    zipf_s = float(os.getenv("ZIPF_S", "1.2"))
    rate_rps = float(os.getenv("RATE_RPS", "5"))
    total_requests = int(os.getenv("TOTAL_REQUESTS", "200"))
    warmup_requests = int(os.getenv("WARMUP_REQUESTS", "20"))

    interval_s = 1.0 / rate_rps if rate_rps > 0 else 0.0

    sent = 0
    ok = 0
    failed = 0
    start = time.time()

    print(
        f"Traffic generator iniciado. distribution={distribution} rate_rps={rate_rps} total={total_requests}"
    )

    with httpx.Client(timeout=10.0) as client:
        while sent < (warmup_requests + total_requests):
            body = build_query(distribution, zipf_s=zipf_s)
            try:
                r = client.post(cache_url, json=body)
                if 200 <= r.status_code < 300:
                    ok += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

            sent += 1

            if sent % 25 == 0:
                elapsed = time.time() - start
                print(
                    f"sent={sent} ok={ok} failed={failed} elapsed_s={elapsed:.1f}",
                    flush=True,
                )

            if interval_s > 0:
                time.sleep(interval_s)

    print("Listo. El contenedor quedará en pausa (para inspección).", flush=True)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()

