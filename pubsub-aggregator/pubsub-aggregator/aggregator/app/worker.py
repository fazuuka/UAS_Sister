"""
Consumer Worker: membaca event dari Redis Stream dan memprosesnya.

Menggunakan Redis Streams (XREADGROUP) sebagai message broker:
- Consumer group untuk distribusi beban ke multiple worker
- ACK setelah proses berhasil (at-least-once delivery)
- Claim pending messages (PEL) untuk crash recovery
"""

import asyncio
import json
import logging
import os

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models import Event
from app.service import EventService

logger = logging.getLogger(__name__)
settings = get_settings()

WORKER_ID = f"consumer-{os.getpid()}-{id(asyncio.current_task) if asyncio.get_event_loop().is_running() else 0}"


async def ensure_consumer_group(redis: Redis) -> None:
    """Buat consumer group jika belum ada (idempotent)."""
    try:
        await redis.xgroup_create(
            settings.redis_stream_key,
            settings.redis_consumer_group,
            id="0",      # Mulai dari awal stream
            mkstream=True,  # Buat stream jika belum ada
        )
        logger.info(f"Consumer group '{settings.redis_consumer_group}' created")
    except Exception as e:
        if "BUSYGROUP" in str(e):
            logger.debug("Consumer group already exists")
        else:
            logger.error(f"Failed to create consumer group: {e}")


async def claim_pending_messages(redis: Redis, worker_name: str) -> list:
    """
    Claim pending messages (PEL) yang belum di-ACK lebih dari 30 detik.
    Ini adalah mekanisme crash recovery: jika worker crash sebelum ACK,
    worker lain akan mengambil alih message tersebut.
    """
    try:
        pending = await redis.xautoclaim(
            settings.redis_stream_key,
            settings.redis_consumer_group,
            worker_name,
            min_idle_time=30000,  # 30 detik
            start_id="0-0",
            count=10,
        )
        messages = pending[1] if pending and len(pending) > 1 else []
        if messages:
            logger.info(f"[{worker_name}] Claimed {len(messages)} pending messages")
        return messages
    except Exception as e:
        logger.warning(f"Failed to claim pending messages: {e}")
        return []


async def run_worker(worker_id: int, redis: Redis) -> None:
    """
    Satu consumer worker async.
    - Baca dari Redis Stream via XREADGROUP (blocking, 2 detik timeout)
    - Proses event dengan EventService (idempotent)
    - ACK message setelah proses berhasil
    """
    worker_name = f"worker-{os.getpid()}-{worker_id}"
    logger.info(f"[{worker_name}] Consumer worker started")

    while True:
        try:
            # Pertama: cek pending messages (crash recovery)
            pending = await claim_pending_messages(redis, worker_name)
            if pending:
                for msg_id, data in pending:
                    await _process_message(redis, msg_id, data, worker_name)

            # Baca message baru dari stream (blocking 2 detik)
            messages = await redis.xreadgroup(
                groupname=settings.redis_consumer_group,
                consumername=worker_name,
                streams={settings.redis_stream_key: ">"},
                count=10,
                block=2000,  # 2 detik block timeout
            )

            if not messages:
                continue

            for stream_key, stream_messages in messages:
                for msg_id, data in stream_messages:
                    await _process_message(redis, msg_id, data, worker_name)

        except asyncio.CancelledError:
            logger.info(f"[{worker_name}] Worker cancelled, shutting down")
            break
        except Exception as e:
            logger.error(f"[{worker_name}] Worker error: {e}")
            await asyncio.sleep(1)  # Backoff sebelum retry


async def _process_message(
    redis: Redis, msg_id: bytes | str, data: dict, worker_name: str
) -> None:
    """Proses satu message dari stream."""
    try:
        # Decode data dari Redis Stream
        raw = data.get(b"event") or data.get("event")
        if isinstance(raw, bytes):
            raw = raw.decode()

        event_dict = json.loads(raw)
        event = Event(**event_dict)

        # Proses dengan EventService (idempotent, transaksional)
        async with AsyncSessionLocal() as db:
            service = EventService(db, redis)
            result = await service.process_event(event)

        # ACK hanya jika proses berhasil (accepted atau duplicate — keduanya valid)
        await redis.xack(
            settings.redis_stream_key,
            settings.redis_consumer_group,
            msg_id,
        )
        logger.debug(f"[{worker_name}] ACK msg_id={msg_id} status={result['status']}")

    except Exception as e:
        logger.error(f"[{worker_name}] Failed to process msg_id={msg_id}: {e}")
        # Tidak ACK → message tetap di PEL → akan di-claim ulang
        # Ini adalah at-least-once delivery semantics


async def start_workers(redis: Redis, count: int) -> list[asyncio.Task]:
    """Start multiple consumer workers sebagai asyncio tasks."""
    await ensure_consumer_group(redis)
    tasks = [
        asyncio.create_task(run_worker(i, redis), name=f"consumer-{i}")
        for i in range(count)
    ]
    logger.info(f"Started {count} consumer workers")
    return tasks
