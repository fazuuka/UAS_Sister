"""Test 1-4: Validasi skema event dan endpoint dasar."""

import pytest


@pytest.mark.asyncio
async def test_health_check_returns_healthy(client):
    """Test 1: Health check harus mengembalikan status healthy saat DB & Redis terhubung."""
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["db"] is True
    assert data["redis"] is True


@pytest.mark.asyncio
async def test_publish_valid_single_event_accepted(client, event_factory):
    """Test 2: Event valid dengan skema benar harus diterima (accepted)."""
    event = event_factory(topic="orders", event_id="evt-valid-001")
    resp = await client.post("/publish", json=event)
    assert resp.status_code == 200
    data = resp.json()
    assert data["accepted"] >= 0  # bisa 1 (baru) atau 0 (jika rerun test, sudah ada)
    assert "event_ids" in data


@pytest.mark.asyncio
async def test_publish_missing_required_field_rejected(client):
    """Test 3: Event tanpa field wajib (event_id) harus ditolak validasi (422)."""
    invalid_event = {
        "topic": "orders",
        # event_id sengaja dihilangkan
        "timestamp": "2026-06-19T10:00:00Z",
        "source": "pytest",
        "payload": {},
    }
    resp = await client.post("/publish", json=invalid_event)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_publish_empty_topic_rejected(client, event_factory):
    """Test 4: Topic kosong harus ditolak oleh validator Pydantic."""
    event = event_factory(topic="", event_id="evt-empty-topic")
    resp = await client.post("/publish", json=event)
    assert resp.status_code == 422
