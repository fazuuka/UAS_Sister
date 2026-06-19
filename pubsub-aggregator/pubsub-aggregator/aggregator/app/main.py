"""
Aggregator Service — FastAPI Application

Endpoints:
  POST /publish          — publish single/batch event
  GET  /events           — daftar event unik yang diproses
  GET  /stats            — statistik agregat
  GET  /health           — health check (readiness probe)

Arsitektur:
  - FastAPI + asyncio (single process, multiple async tasks)
  - Redis Stream sebagai message queue (broker internal)
  - Postgres sebagai persistent dedup store + event store
  - N consumer workers async berjalan di background
"""

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pythonjsonlogger import jsonlogger
from redis.asyncio import Redis, from_url
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import AsyncSessionLocal, check_db_connection, get_db
from app.models import Event, EventBatch, EventResponse, PublishResponse, StatsResponse
from app.service import EventService
from app.worker import start_workers

# ─── Logging Setup ────────────────────────────────────────────────────────────
settings = get_settings()

handler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
handler.setFormatter(formatter)
logging.basicConfig(level=settings.log_level, handlers=[handler])
logger = logging.getLogger(__name__)

# ─── Global State ─────────────────────────────────────────────────────────────
redis_client: Redis | None = None
worker_tasks: list[asyncio.Task] = []


# ─── Lifespan (startup/shutdown) ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, worker_tasks

    logger.info("Starting aggregator service...")

    redis_client = from_url(settings.redis_url, decode_responses=False)
    await redis_client.ping()
    logger.info("Redis connected")

    worker_tasks = await start_workers(redis_client, settings.worker_count)
    logger.info(f"Started {settings.worker_count} consumer workers")

    yield

    logger.info("Shutting down workers...")
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)

    await redis_client.aclose()
    logger.info("Aggregator service stopped")


app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="Distributed log aggregator dengan idempotency, deduplication, dan transaksi",
    version="1.0.0",
    lifespan=lifespan,
)


def get_redis() -> Redis:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis not connected")
    return redis_client


@app.get("/health")
async def health_check():
    """Readiness probe: cek koneksi DB dan Redis."""
    db_ok = await check_db_connection()
    redis_ok = False
    try:
        if redis_client:
            await redis_client.ping()
            redis_ok = True
    except Exception:
        pass

    if not db_ok or not redis_ok:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "db": db_ok, "redis": redis_ok},
        )
    return {"status": "healthy", "db": db_ok, "redis": redis_ok}


@app.post("/publish", response_model=PublishResponse)
async def publish_events(
    body: EventBatch | Event,
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """
    Publish satu event atau batch event ke sistem.

    Alur:
    1. Validasi skema Pydantic (otomatis)
    2. Push event ke Redis Stream (at-least-once delivery)
    3. Proses langsung secara sinkron (idempotent) agar GET /events
       segera konsisten — sekaligus consumer worker background tetap
       memproses stream untuk skenario crash-recovery/replay.
    """
    if isinstance(body, Event):
        events = [body]
    else:
        events = body.events

    service = EventService(db, redis)

    pipeline = redis.pipeline()
    for event in events:
        pipeline.xadd(
            settings.redis_stream_key,
            {"event": json.dumps(event.model_dump(mode="json"))},
            maxlen=100000,
        )
    await pipeline.execute()

    result = await service.process_batch(events)

    logger.info(
        f"Published batch: accepted={result['accepted']} "
        f"duplicates={result['duplicate_dropped']} "
        f"invalid={result['invalid']}"
    )

    return PublishResponse(**result)


@app.get("/events", response_model=list[EventResponse])
async def get_events(
    topic: str | None = Query(default=None, description="Filter by topic"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Daftar event unik yang telah diproses. Filter by topic opsional."""
    service = EventService(db, redis)
    events = await service.get_events(topic=topic, limit=limit, offset=offset)
    return events


@app.get("/stats", response_model=StatsResponse)
async def get_stats(
    db: AsyncSession = Depends(get_db),
    redis: Redis = Depends(get_redis),
):
    """Statistik: received, unique_processed, duplicate_dropped, topics, uptime."""
    service = EventService(db, redis)
    stats = await service.get_stats()
    return StatsResponse(**stats)


@app.get("/")
async def root():
    return {
        "service": "Pub-Sub Log Aggregator",
        "version": "1.0.0",
        "endpoints": ["/publish", "/events", "/stats", "/health"],
    }
