"""
Publisher: Simulator event generator.

Tujuan:
  - Mengirim >= 20.000 event ke aggregator
  - Sengaja menduplikasi >= 30% event (sesuai spesifikasi tugas)
  - Mensimulasikan at-least-once delivery semantics (retry, network blip)
  - Mengukur throughput & latency dasar

Strategi duplikasi:
  - Setiap event punya event_id unik (UUID v4)
  - Sebagian event_id "dipakai ulang" secara sengaja (klon event sebelumnya)
    dengan payload yang sama persis, mensimulasikan retry/at-least-once
"""

import asyncio
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("publisher")

TARGET_URL = os.environ.get("TARGET_URL", "http://aggregator:8080/publish")
TOTAL_EVENTS = int(os.environ.get("TOTAL_EVENTS", "25000"))
DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.35"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "10"))

TOPICS = ["orders", "payments", "inventory", "user-activity", "shipping"]
SOURCES = ["service-a", "service-b", "service-c", "mobile-app", "web-app"]


def make_event(topic: str, event_id: str, source: str) -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": {
            "amount": round(random.uniform(1, 1000), 2),
            "status": random.choice(["created", "processed", "completed"]),
            "trace": str(uuid.uuid4())[:8],
        },
    }


def generate_event_pool(total: int, dup_rate: float) -> list[dict]:
    """
    Generate pool event dengan persentase duplikasi yang ditentukan.
    unique_count event unik, lalu sisanya adalah duplikat dari pool unik tersebut.
    """
    unique_count = int(total * (1 - dup_rate))
    duplicate_count = total - unique_count

    unique_events = []
    for _ in range(unique_count):
        topic = random.choice(TOPICS)
        event_id = str(uuid.uuid4())
        source = random.choice(SOURCES)
        unique_events.append(make_event(topic, event_id, source))

    # Duplikat: ambil event acak dari unique_events dan kirim ulang
    # (payload sama, event_id sama -> harus dideteksi sebagai duplicate)
    duplicates = [random.choice(unique_events).copy() for _ in range(duplicate_count)]

    pool = unique_events + duplicates
    random.shuffle(pool)
    return pool


async def send_batch(client: httpx.AsyncClient, events: list[dict], semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        body = {"events": events} if len(events) > 1 else events[0]
        try:
            resp = await client.post(TARGET_URL, json=body, timeout=30.0)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError as e:
            logger.warning(f"Batch send failed: {e}")
            return {"accepted": 0, "duplicate_dropped": 0, "invalid": len(events)}


async def main():
    logger.info(f"Publisher starting: total={TOTAL_EVENTS} dup_rate={DUPLICATE_RATE} batch_size={BATCH_SIZE}")

    # Tunggu aggregator siap
    async with httpx.AsyncClient() as client:
        health_url = TARGET_URL.replace("/publish", "/health")
        for attempt in range(30):
            try:
                resp = await client.get(health_url, timeout=5.0)
                if resp.status_code == 200:
                    logger.info("Aggregator is healthy, starting publish")
                    break
            except Exception:
                pass
            logger.info(f"Waiting for aggregator... attempt {attempt + 1}/30")
            await asyncio.sleep(2)

    pool = generate_event_pool(TOTAL_EVENTS, DUPLICATE_RATE)
    logger.info(f"Generated {len(pool)} events ({DUPLICATE_RATE * 100:.0f}% duplication)")

    batches = [pool[i : i + BATCH_SIZE] for i in range(0, len(pool), BATCH_SIZE)]
    semaphore = asyncio.Semaphore(CONCURRENCY)

    total_accepted = 0
    total_duplicate = 0
    total_invalid = 0

    start_time = time.monotonic()

    async with httpx.AsyncClient() as client:
        tasks = [send_batch(client, batch, semaphore) for batch in batches]
        results = await asyncio.gather(*tasks)

    elapsed = time.monotonic() - start_time

    for r in results:
        total_accepted += r.get("accepted", 0)
        total_duplicate += r.get("duplicate_dropped", 0)
        total_invalid += r.get("invalid", 0)

    throughput = len(pool) / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("PUBLISH SUMMARY")
    logger.info(f"  Total events sent     : {len(pool)}")
    logger.info(f"  Accepted (unique)     : {total_accepted}")
    logger.info(f"  Duplicate dropped     : {total_duplicate}")
    logger.info(f"  Invalid               : {total_invalid}")
    logger.info(f"  Elapsed time          : {elapsed:.2f}s")
    logger.info(f"  Throughput            : {throughput:.2f} events/sec")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
