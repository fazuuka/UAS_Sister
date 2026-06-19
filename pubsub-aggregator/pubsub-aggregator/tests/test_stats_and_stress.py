"""Test 17-19: GET /stats konsistensi dan stress test kecil."""

import asyncio
import time
import uuid

import pytest


@pytest.mark.asyncio
async def test_stats_endpoint_structure(client):
    """Test 17: GET /stats mengembalikan semua field yang disyaratkan spesifikasi."""
    resp = await client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()

    required_fields = {"received", "unique_processed", "duplicate_dropped", "topics", "uptime_seconds"}
    assert required_fields.issubset(data.keys())
    assert isinstance(data["topics"], list)
    assert data["uptime_seconds"] >= 0


@pytest.mark.asyncio
async def test_stats_matches_events_relationship(client, event_factory):
    """
    Test 18: Invariant matematis — received == unique_processed + duplicate_dropped
    (secara global, di seluruh sistem) harus selalu terjaga.
    """
    stats = (await client.get("/stats")).json()
    assert stats["received"] == stats["unique_processed"] + stats["duplicate_dropped"]


@pytest.mark.asyncio
async def test_small_batch_stress_execution_time(client, event_factory):
    """
    Test 19: Stress kecil — kirim batch 500 event unik dan ukur waktu eksekusi.
    Sistem harus tetap responsif (selesai dalam waktu wajar, < 15 detik untuk 500 event).
    """
    events = [
        event_factory(topic="stress-small", event_id=f"evt-stress-{uuid.uuid4()}")
        for _ in range(500)
    ]
    batch = {"events": events}

    start = time.monotonic()
    resp = await client.post("/publish", json=batch, timeout=20.0)
    elapsed = time.monotonic() - start

    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] == 500
    assert elapsed < 15.0, f"Batch 500 event terlalu lambat: {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_get_events_pagination(client, event_factory):
    """Test 20: GET /events mendukung pagination (limit & offset) dengan benar."""
    topic = f"pagination-test-{uuid.uuid4()}"
    events = [event_factory(topic=topic, event_id=f"evt-page-{i}") for i in range(15)]
    await asyncio.gather(*[client.post("/publish", json=e) for e in events])

    page1 = await client.get("/events", params={"topic": topic, "limit": 10, "offset": 0})
    page2 = await client.get("/events", params={"topic": topic, "limit": 10, "offset": 10})

    assert len(page1.json()) == 10
    assert len(page2.json()) == 5

    ids_page1 = {e["event_id"] for e in page1.json()}
    ids_page2 = {e["event_id"] for e in page2.json()}
    assert ids_page1.isdisjoint(ids_page2), "Pagination tidak boleh tumpang tindih"
