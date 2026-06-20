"""
EventService: Inti logika idempotency, deduplication, dan transaksi.

Strategi dedup:
  - Level 1 (Cache): Redis SETNX sebagai fast-path filter (bloom filter sederhana)
  - Level 2 (DB): INSERT ... ON CONFLICT DO NOTHING dengan UNIQUE(topic, event_id)
  - Transaksi: READ COMMITTED dengan unique constraint — race condition diatasi
    karena DB menjamin atomicity pada INSERT + UPDATE stats

Isolation Level: READ COMMITTED (default Postgres)
  - Cukup untuk use-case ini karena dedup bergantung pada unique constraint
  - Constraint unik mencegah phantom insert dari concurrent workers
  - Tidak perlu SERIALIZABLE (overhead tinggi, tidak diperlukan untuk idempotent upsert)
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.models import Event

logger = logging.getLogger(__name__)
settings = get_settings()

WORKER_ID = f"worker-{os.getpid()}"


class EventService:
    def __init__(self, db: AsyncSession, redis: Redis):
        self.db = db
        self.redis = redis

    async def _is_duplicate_in_cache(self, topic: str, event_id: str) -> bool:
        """
        Level 1 dedup: Redis SETNX sebagai fast-path.
        TTL 24 jam — cukup untuk window dedup normal.
        Ini optimisasi, bukan satu-satunya garis pertahanan.
        """
        cache_key = f"dedup:{topic}:{event_id}"
        # SET NX EX: Set if Not eXists, dengan Expiry
        result = await self.redis.set(cache_key, "1", nx=True, ex=86400)
        # result = True jika SET berhasil (event baru), None jika sudah ada
        return result is None

    async def process_event(self, event: Event) -> dict[str, str]:
        """
        Proses satu event dengan idempotency penuh.

        Flow:
        1. Increment received (audit)
        2. Cek cache Redis (fast-path dedup)
        3. Jika cache miss, coba INSERT ke DB dalam transaksi
        4. Jika UNIQUE conflict → duplicate, update stats
        5. Jika berhasil → update stats unique_processed
        6. Catat audit log

        Catatan konkurensi: method ini membuka session DB sendiri
        (bukan memakai self.db milik request) agar AMAN dipanggil
        bersamaan (concurrent) lewat asyncio.gather pada process_batch.
        SQLAlchemy AsyncSession TIDAK thread/task-safe jika dipakai
        bersama oleh banyak coroutine sekaligus.

        Return: {"status": "accepted"|"duplicate"}
        """
        # Atomically increment received count
        await self._increment_stat("received")

        # Fast-path: cek Redis cache
        if await self._is_duplicate_in_cache(event.topic, event.event_id):
            logger.info(
                f"[{WORKER_ID}] DUPLICATE (cache) event_id={event.event_id} topic={event.topic}"
            )
            await self._increment_stat("duplicate_dropped")
            await self._write_audit(event.event_id, event.topic, "duplicate_dropped")
            return {"status": "duplicate"}

        # DB-level dedup dengan transaksi — session terpisah per event
        try:
            async with AsyncSessionLocal() as session:
                # INSERT ... ON CONFLICT DO NOTHING
                # Transaksi READ COMMITTED: unique constraint memastikan atomicity
                result = await session.execute(
                    text("""
                        INSERT INTO processed_events
                            (topic, event_id, source, timestamp, payload, received_at)
                        VALUES
                            (:topic, :event_id, :source, :timestamp, CAST(:payload AS jsonb), NOW())
                        ON CONFLICT (topic, event_id) DO NOTHING
                        RETURNING id
                    """),
                    {
                        "topic": event.topic,
                        "event_id": event.event_id,
                        "source": event.source,
                        "timestamp": event.timestamp,
                        "payload": json.dumps(event.payload),
                    },
                )
                row = result.fetchone()

                if row is None:
                    # Conflict: event sudah ada di DB (race condition antar worker)
                    logger.info(
                        f"[{WORKER_ID}] DUPLICATE (db) event_id={event.event_id} topic={event.topic}"
                    )
                    await session.execute(
                        text("""
                            UPDATE aggregator_stats
                                SET duplicate_dropped = duplicate_dropped + 1,
                                updated_at = NOW()
                            WHERE id = 1
                        """)
                    )
                    await session.commit()
                    await self._write_audit(event.event_id, event.topic, "duplicate_dropped")
                    return {"status": "duplicate"}

                # Event baru berhasil diproses
                await session.execute(
                    text("""
                        UPDATE aggregator_stats
                        SET unique_processed = unique_processed + 1,
                            updated_at = NOW()
                        WHERE id = 1
                    """)
                )
                await session.commit()
                await self._write_audit(event.event_id, event.topic, "accepted")

                logger.info(
                    f"[{WORKER_ID}] ACCEPTED event_id={event.event_id} topic={event.topic}"
                )
                return {"status": "accepted"}

        except Exception as e:
            logger.error(f"[{WORKER_ID}] Error processing event {event.event_id}: {e}")
            raise

    async def _increment_stat(self, field: str) -> None:
        """
        Update statistik secara transaksional.
        SQL UPDATE ... SET count = count + 1 mencegah lost-update
        di bawah konkurensi multi-worker.
        """
        valid_fields = {"received", "unique_processed", "duplicate_dropped"}
        if field not in valid_fields:
            return
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(
                    text(f"""
                        UPDATE aggregator_stats
                        SET {field} = {field} + 1,
                            updated_at = NOW()
                        WHERE id = 1
                    """)
                )
                await session.commit()
        except Exception as e:
            logger.warning(f"Failed to increment stat {field}: {e}")

    async def _write_audit(self, event_id: str, topic: str, action: str) -> None:
        """Tulis audit log non-blocking (fire and forget, tidak blocking main flow)."""
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(
                    text("""
                        INSERT INTO audit_log (event_id, topic, action, worker_id)
                        VALUES (:event_id, :topic, :action, :worker_id)
                    """),
                    {
                        "event_id": event_id,
                        "topic": topic,
                        "action": action,
                        "worker_id": WORKER_ID,
                    },
                )
                await session.commit()
        except Exception as e:
            logger.warning(f"Audit log write failed: {e}")

    async def process_batch(self, events: list[Event]) -> dict[str, Any]:
        """
        Batch processing dengan handling duplikat sinkron intra-batch 
        untuk mencegah race condition di level cache/asyncio.gather.
        """
        accepted = 0
        duplicated = 0
        errors = 0
        accepted_ids = []

        # 1. Filter duplikat yang berada di dalam BATCH yang sama secara sinkron
        unique_events_in_batch = []
        seen_keys = set()

        for event in events:
            composite_key = (event.topic, event.event_id)
            if composite_key in seen_keys:
                # Duplikat terdeteksi di dalam batch yang sama!
                duplicated += 1
                await self._increment_stat("received")
                await self._increment_stat("duplicate_dropped")
                await self._write_audit(event.event_id, event.topic, "duplicate_dropped")
            else:
                seen_keys.add(composite_key)
                unique_events_in_batch.append(event)

        # 2. Proses event yang benar-benar unik secara konkuren
        if unique_events_in_batch:
            tasks = [self.process_event(event) for event in unique_events_in_batch]
            results = await asyncio.gather(*tasks, return_exceptions=False)

            for event, result in zip(unique_events_in_batch, results):
                if isinstance(result, Exception):
                    errors += 1
                    logger.error(f"Batch error for event {event.event_id}: {result}")
                elif result["status"] == "accepted":
                    accepted += 1
                    accepted_ids.append(event.event_id)
                else:
                    duplicated += 1

        return {
            "accepted": accepted,
            "duplicate_dropped": duplicated,
            "invalid": errors,
            "event_ids": accepted_ids,
        }

    async def get_events(
        self,
        topic: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Ambil event yang sudah diproses, filter by topic opsional."""
        if topic:
            result = await self.db.execute(
                text("""
                    SELECT topic, event_id, source, timestamp, payload, received_at
                    FROM processed_events
                    WHERE topic = :topic
                    ORDER BY received_at DESC
                    LIMIT :limit OFFSET :offset
                """),
                {"topic": topic, "limit": limit, "offset": offset},
            )
        else:
            result = await self.db.execute(
                text("""
                    SELECT topic, event_id, source, timestamp, payload, received_at
                    FROM processed_events
                    ORDER BY received_at DESC
                    LIMIT :limit OFFSET :offset
                """),
                {"limit": limit, "offset": offset},
            )
        rows = result.fetchall()
        return [
            {
                "topic": r.topic,
                "event_id": r.event_id,
                "source": r.source,
                "timestamp": r.timestamp,
                "payload": r.payload,
                "received_at": r.received_at,
            }
            for r in rows
        ]

    async def get_stats(self) -> dict:
        """Ambil statistik agregat dari DB."""
        result = await self.db.execute(
            text("SELECT received, unique_processed, duplicate_dropped, started_at FROM aggregator_stats WHERE id = 1")
        )
        row = result.fetchone()

        # Ambil daftar topics unik
        topics_result = await self.db.execute(
            text("SELECT DISTINCT topic FROM processed_events ORDER BY topic")
        )
        topics = [r.topic for r in topics_result.fetchall()]

        now = datetime.now(timezone.utc)
        started_at = row.started_at if row and row.started_at.tzinfo else row.started_at.replace(tzinfo=timezone.utc)
        uptime = (now - started_at).total_seconds() if row else 0

        return {
            "received": row.received if row else 0,
            "unique_processed": row.unique_processed if row else 0,
            "duplicate_dropped": row.duplicate_dropped if row else 0,
            "topics": topics,
            "uptime_seconds": uptime,
            "started_at": row.started_at if row else now,
        }
