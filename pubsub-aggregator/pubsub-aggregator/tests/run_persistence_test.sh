#!/usr/bin/env bash
# Script otomatisasi test persistensi: publish -> restart container -> verifikasi data tetap ada
set -e

echo "=== [1/4] Menjalankan test publish event sebelum restart ==="
pytest tests/test_persistence.py::test_persist_publish_event_before_restart -v

echo "=== [2/4] Restart container storage (Postgres) — mensimulasikan crash/recreate ==="
docker compose restart storage

echo "    Menunggu Postgres siap kembali..."
sleep 8
until docker compose exec -T storage pg_isready -U user -d logdb > /dev/null 2>&1; do
  echo "    Postgres belum ready, menunggu..."
  sleep 2
done
echo "    Postgres ready."

echo "=== [3/4] Verifikasi data masih ada setelah restart ==="
pytest tests/test_persistence.py::test_persist_event_survives_restart -v

echo "=== [4/4] Verifikasi dedup tetap mencegah reprocessing setelah restart ==="
pytest tests/test_persistence.py::test_persist_dedup_prevents_reprocess_after_restart -v

echo "=== SELESAI: Semua test persistensi LULUS ==="
