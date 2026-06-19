"""
Test 14-16: Persistensi data via named volumes.

Test ini memerlukan kontrol manual container (docker compose restart/recreate)
sehingga sebagian bersifat semi-manual — dijalankan dengan urutan:
  1. test_persist_publish_event_before_restart  -> publish event, simpan event_id
  2. (manual) docker compose restart storage
  3. test_persist_event_survives_restart         -> GET event masih ada
  4. test_persist_dedup_prevents_reprocess_after_restart -> kirim ulang -> tetap duplicate

Untuk otomatisasi penuh di CI, gunakan script run_persistence_test.sh yang
menjalankan restart container di antara dua proses pytest.
"""

import json
import os
import uuid

import pytest

STATE_FILE = "/tmp/persistence_test_state.json"


@pytest.mark.asyncio
async def test_persist_publish_event_before_restart(client, event_factory):
    """
    Test 14: Publish event dan simpan event_id ke file state, untuk
    diverifikasi kembali oleh test_persist_event_survives_restart
    SETELAH container storage di-restart/recreate secara manual.
    """
    event_id = f"evt-persist-{uuid.uuid4()}"
    event = event_factory(topic="persistence-check", event_id=event_id)

    resp = await client.post("/publish", json=event)
    assert resp.json()["accepted"] == 1

    with open(STATE_FILE, "w") as f:
        json.dump({"event_id": event_id, "topic": "persistence-check"}, f)


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.path.exists(STATE_FILE),
    reason="Jalankan test_persist_publish_event_before_restart dan restart container dahulu",
)
async def test_persist_event_survives_restart(client):
    """
    Test 15 (KRITIS): Setelah container storage di-restart/recreate,
    event yang sebelumnya disimpan harus TETAP ADA (dibaca dari named volume).
    """
    if not os.path.exists(STATE_FILE):
        pytest.skip("State file tidak ditemukan; jalankan test sebelumnya dahulu")

    with open(STATE_FILE) as f:
        state = json.load(f)

    resp = await client.get("/events", params={"topic": state["topic"], "limit": 1000})
    events = resp.json()
    matching = [e for e in events if e["event_id"] == state["event_id"]]

    assert len(matching) == 1, "Data hilang setelah restart! Volume tidak persisten."


@pytest.mark.asyncio
async def test_persist_dedup_prevents_reprocess_after_restart(client):
    """
    Test 16: Setelah restart, mengirim ULANG event yang sama (dari sebelum restart)
    harus tetap terdeteksi sebagai DUPLICATE — membuktikan dedup store
    (constraint unique di Postgres) bertahan melewati siklus hidup container.
    """
    if not os.path.exists(STATE_FILE):
        pytest.skip("State file tidak ditemukan; jalankan test sebelumnya dahulu")

    with open(STATE_FILE) as f:
        state = json.load(f)

    from datetime import datetime, timezone
    replay_event = {
        "topic": state["topic"],
        "event_id": state["event_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "pytest-replay",
        "payload": {"replay": True},
    }

    import httpx
    async with httpx.AsyncClient(base_url=os.environ.get("TEST_BASE_URL", "http://localhost:8080")) as c:
        resp = await c.post("/publish", json=replay_event)

    data = resp.json()
    assert data["duplicate_dropped"] == 1, "Event seharusnya terdeteksi duplikat setelah restart"
    assert data["accepted"] == 0
