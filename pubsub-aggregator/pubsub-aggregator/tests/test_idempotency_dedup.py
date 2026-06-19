"""Test 5-9: Idempotency dan Deduplication — inti dari spesifikasi tugas."""

import asyncio

import pytest


@pytest.mark.asyncio
async def test_duplicate_event_id_processed_once(client, event_factory):
    """
    Test 5 (KRITIS): Mengirim event yang sama (topic+event_id identik) dua kali
    secara berurutan -> hanya diproses SEKALI. Kiriman kedua harus duplicate_dropped.
    """
    event = event_factory(topic="payments", event_id="evt-dedup-seq-001")

    resp1 = await client.post("/publish", json=event)
    resp2 = await client.post("/publish", json=event)

    data1 = resp1.json()
    data2 = resp2.json()

    # Kiriman pertama diterima sebagai unik
    assert data1["accepted"] == 1
    assert data1["duplicate_dropped"] == 0

    # Kiriman kedua (identik) harus terdeteksi duplikat
    assert data2["accepted"] == 0
    assert data2["duplicate_dropped"] == 1


@pytest.mark.asyncio
async def test_duplicate_within_same_batch_detected(client, event_factory):
    """Test 6: Duplikat di DALAM satu batch yang sama juga harus terdeteksi."""
    event = event_factory(topic="inventory", event_id="evt-dedup-batch-001")
    batch = {"events": [event, event.copy(), event.copy()]}  # 3x event identik

    resp = await client.post("/publish", json=batch)
    data = resp.json()

    assert data["accepted"] == 1
    assert data["duplicate_dropped"] == 2


@pytest.mark.asyncio
async def test_get_events_contains_only_unique_events(client, event_factory):
    """Test 7: GET /events tidak menampilkan duplikat — hanya satu entri per (topic, event_id)."""
    event_id = "evt-get-events-uniq-001"
    event = event_factory(topic="shipping", event_id=event_id)

    # Kirim 3 kali
    for _ in range(3):
        await client.post("/publish", json=event)

    resp = await client.get("/events", params={"topic": "shipping", "limit": 1000})
    assert resp.status_code == 200
    events = resp.json()

    matching = [e for e in events if e["event_id"] == event_id]
    assert len(matching) == 1, "Event seharusnya muncul tepat satu kali di /events"


@pytest.mark.asyncio
async def test_different_topics_same_event_id_both_processed(client, event_factory):
    """
    Test 8: event_id sama tetapi topic BERBEDA dianggap event berbeda
    (constraint unik adalah composite: topic + event_id).
    """
    shared_id = "evt-shared-id-cross-topic"
    event_a = event_factory(topic="topic-a", event_id=shared_id)
    event_b = event_factory(topic="topic-b", event_id=shared_id)

    resp_a = await client.post("/publish", json=event_a)
    resp_b = await client.post("/publish", json=event_b)

    assert resp_a.json()["accepted"] == 1
    assert resp_b.json()["accepted"] == 1  # bukan duplikat karena topic berbeda


@pytest.mark.asyncio
async def test_high_volume_duplicate_burst(client, event_factory):
    """
    Test 9: Kirim 1 event asli + 19 duplikat secara CONCURRENT (paralel).
    Hanya 1 yang boleh accepted, 19 lainnya duplicate_dropped — membuktikan
    dedup tetap akurat di bawah beban konkuren tinggi dari sisi client.
    """
    event = event_factory(topic="stress-dedup", event_id="evt-burst-001")
    requests = [client.post("/publish", json=event.copy()) for _ in range(20)]
    responses = await asyncio.gather(*requests)

    total_accepted = sum(r.json()["accepted"] for r in responses)
    total_duplicate = sum(r.json()["duplicate_dropped"] for r in responses)

    assert total_accepted == 1
    assert total_duplicate == 19
