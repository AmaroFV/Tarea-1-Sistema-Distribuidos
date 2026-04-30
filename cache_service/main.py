import asyncio
import json
import os
import time
from typing import Any, Optional

import httpx
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()


class QueryRequest(BaseModel):
    query_type: str
    zone_id: Optional[str] = None
    zone_id_a: Optional[str] = None
    zone_id_b: Optional[str] = None
    confidence_min: float = 0.0
    bins: int = 5


REDIS_HOST = os.getenv("REDIS_HOST", "redis-server")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESPONSE_GEN_URL = os.getenv("RESPONSE_GEN_URL", "http://response_generator:8000/query")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics_service:8002/event")
TTL_SECONDS = int(os.getenv("TTL_SECONDS", "60"))
EVICTION_POLL_SECONDS = float(os.getenv("EVICTION_POLL_SECONDS", "5"))

redis_client: Optional[redis.Redis] = None


def _cache_key(req: QueryRequest) -> str:
    q = req.query_type.upper()

    if q == "Q1":
        if not req.zone_id:
            raise ValueError("zone_id requerido para Q1")
        return f"count:{req.zone_id}:conf={req.confidence_min}"

    if q == "Q2":
        if not req.zone_id:
            raise ValueError("zone_id requerido para Q2")
        return f"area:{req.zone_id}:conf={req.confidence_min}"

    if q == "Q3":
        if not req.zone_id:
            raise ValueError("zone_id requerido para Q3")
        return f"density:{req.zone_id}:conf={req.confidence_min}"

    if q == "Q4":
        if not req.zone_id_a or not req.zone_id_b:
            raise ValueError("zone_id_a y zone_id_b requeridos para Q4")
        return f"compare:density:{req.zone_id_a}:{req.zone_id_b}:conf={req.confidence_min}"

    if q == "Q5":
        if not req.zone_id:
            raise ValueError("zone_id requerido para Q5")
        return f"confidence_dist:{req.zone_id}:bins={req.bins}"

    raise ValueError("Tipo de consulta no soportado")


def _zone_for_metrics(req: QueryRequest) -> Optional[str]:
    q = req.query_type.upper()
    if q in {"Q1", "Q2", "Q3", "Q5"}:
        return req.zone_id
    if q == "Q4":
        if req.zone_id_a and req.zone_id_b:
            return f"{req.zone_id_a}|{req.zone_id_b}"
        return None
    return None


async def _emit_event(
    *,
    event_type: str,
    query_type: Optional[str],
    latency_ms: Optional[float],
    zone_id: Optional[str],
) -> None:
    payload = {
        "timestamp": time.time(),
        "event_type": event_type,
        "query_type": query_type,
        "latency_ms": latency_ms,
        "zone_id": zone_id,
    }
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            await client.post(METRICS_URL, json=payload)
    except Exception:
        # Métricas no deben botar el flujo principal
        return


@app.on_event("startup")
async def startup() -> None:
    global redis_client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        redis_client.ping()
    except Exception as e:
        raise RuntimeError(f"No se pudo conectar a Redis: {e}") from e

    asyncio.create_task(_eviction_poller())


async def _eviction_poller() -> None:
    if not redis_client:
        return

    last_evicted = 0
    while True:
        try:
            stats = redis_client.info("stats")
            current = int(stats.get("evicted_keys", 0))
            delta = max(0, current - last_evicted)
            last_evicted = current

            if delta > 0:
                # Emitimos eventos discretos (cap de seguridad para no spamear)
                emit_n = min(delta, 200)
                for _ in range(emit_n):
                    await _emit_event(
                        event_type="eviction",
                        query_type=None,
                        latency_ms=None,
                        zone_id=None,
                    )
        except Exception:
            pass

        await asyncio.sleep(EVICTION_POLL_SECONDS)


@app.post("/query")
async def query(req: QueryRequest) -> Any:
    if not redis_client:
        raise HTTPException(status_code=500, detail="Redis no inicializado")

    try:
        key = _cache_key(req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    q_type = req.query_type.upper()
    zone_id = _zone_for_metrics(req)

    start = time.perf_counter()
    cached = redis_client.get(key)
    if cached is not None:
        latency_ms = (time.perf_counter() - start) * 1000
        await _emit_event(
            event_type="cache_hit",
            query_type=q_type,
            latency_ms=latency_ms,
            zone_id=zone_id,
        )
        try:
            return json.loads(cached)
        except Exception:
            return {"result": cached}

    # miss -> delegar al response generator
    req_payload = req.dict() if hasattr(req, "dict") else req.model_dump()
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            last_exc: Optional[Exception] = None
            # `depends_on` no espera a que el servicio esté "ready".
            # Reintentamos para cubrir el período de startup/carga del CSV.
            for attempt in range(1, 6):
                try:
                    rg_resp = await client.post(RESPONSE_GEN_URL, json=req_payload)
                    rg_resp.raise_for_status()
                    result = rg_resp.json()
                    break
                except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                    last_exc = e
                    await asyncio.sleep(min(0.2 * (2 ** (attempt - 1)), 2.0))
                except httpx.HTTPStatusError:
                    raise
            else:
                raise last_exc or httpx.ConnectError("No connection", request=None)
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Error response_generator: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"No se pudo consultar response_generator: {e}") from e

    redis_client.set(key, json.dumps(result), ex=TTL_SECONDS)
    latency_ms = (time.perf_counter() - start) * 1000
    await _emit_event(
        event_type="cache_miss",
        query_type=q_type,
        latency_ms=latency_ms,
        zone_id=zone_id,
    )

    return result

