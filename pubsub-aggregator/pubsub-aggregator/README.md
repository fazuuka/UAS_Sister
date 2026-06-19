# Pub-Sub Log Aggregator Terdistribusi

Sistem log aggregator multi-service dengan **idempotent consumer**, **deduplication persisten**, dan **kontrol transaksi/konkurensi**, berjalan penuh di atas Docker Compose pada jaringan lokal (tanpa dependensi layanan eksternal publik).


---

## 1. Arsitektur

```
                    ┌─────────────┐
   ┌───────────────▶│  publisher  │  (generator event + duplikasi sengaja)
   │                └──────┬──────┘
   │                       │ HTTP POST /publish
   │                       ▼
   │                ┌─────────────┐        ┌─────────┐
   │                │ aggregator  │◀──────▶│ broker  │  (Redis Stream)
   │                │  (FastAPI)  │        │ (redis) │
   │                └──────┬──────┘        └─────────┘
   │                       │ SQL (asyncpg/SQLAlchemy)
   │                       ▼
   │                ┌─────────────┐
   └────GET─────────│   storage   │  (Postgres 16, named volume)
                     └─────────────┘
```

| Service      | Peran                                                                 |
|--------------|------------------------------------------------------------------------|
| `aggregator` | FastAPI app: terima publish, jalankan consumer worker async, expose `/events`, `/stats`, `/health` |
| `publisher`  | Simulator yang mengirim ≥20.000 event dengan ≥30% duplikasi sengaja  |
| `broker`     | Redis 7 (Stream + consumer group) — message broker internal          |
| `storage`    | Postgres 16 — dedup store persisten (`UNIQUE(topic, event_id)`)      |

**Dedup berlapis:**
1. **Redis SETNX** (fast-path cache, TTL 24 jam) — saringan cepat sebelum menyentuh DB.
2. **Postgres `UNIQUE(topic, event_id)` + `INSERT ... ON CONFLICT DO NOTHING`** — garis pertahanan utama, atomik, tahan restart.

**Isolation level:** READ COMMITTED (default Postgres) — cukup karena dedup tidak bergantung pada snapshot read, melainkan pada *unique constraint* yang dijamin atomik oleh DB itu sendiri saat terjadi konflik INSERT konkuren.

---

## 2. Struktur Folder

```
pubsub-aggregator/
├── docker-compose.yml              # Orkestrasi 4 service (wajib)
├── README.md                       # File ini
├── report.md                       # Laporan teori Bab 1-13 + analisis performa
│
├── aggregator/                     # Service utama (API + consumer worker)
│   ├── Dockerfile                  # python:3.11-slim, non-root user
│   ├── requirements.txt
│   ├── init.sql                    # Schema Postgres (tabel + unique constraint)
│   └── app/
│       ├── __init__.py
│       ├── main.py                 # FastAPI routes: /publish /events /stats /health
│       ├── config.py                # Pydantic settings (env vars)
│       ├── models.py               # Skema validasi Event/Batch (Pydantic)
│       ├── database.py             # SQLAlchemy async engine & session
│       ├── service.py               # EventService: idempotency, dedup, transaksi
│       └── worker.py                # Consumer worker Redis Stream (crash recovery)
│
├── publisher/                      # Simulator generator event
│   ├── Dockerfile
│   ├── requirements.txt
│   └── publisher.py                # Generate 25.000 event, 35% duplikasi
│
└── tests/                          # 20 test (unit + integration)
    ├── requirements.txt
    ├── conftest.py                  # Fixture HTTP client + event factory
    ├── test_validation.py           # 4 test: skema event
    ├── test_idempotency_dedup.py    # 5 test: dedup inti
    ├── test_concurrency_transactions.py  # 4 test: race condition, transaksi
    ├── test_persistence.py          # 3 test: data tahan restart container
    ├── test_stats_and_stress.py     # 4 test: /stats, pagination, stress kecil
    └── run_persistence_test.sh      # Script otomatisasi test restart container
```

---

## 3. Cara Menjalankan

### Prasyarat
- Docker & Docker Compose v2 (`docker compose version`)
- Python 3.11+ (hanya untuk menjalankan test dari host, opsional)

### Build & Jalankan Sistem

```bash
git clone https://github.com/fazuuka/IPPL.git
cd IPPL

# Build semua image dan jalankan seluruh stack
docker compose up --build
```

Tunggu hingga log menunjukkan:
```
storage      | database system is ready to accept connections
broker       | Ready to accept connections tcp
aggregator   | Started 4 consumer workers
publisher    | Publisher starting: total=25000 dup_rate=0.35
```

Aggregator dapat diakses di **http://localhost:8080**

### Menjalankan di Background

```bash
docker compose up --build -d
docker compose logs -f aggregator    # ikuti log aggregator
docker compose logs -f publisher     # ikuti progres publisher
```

### Endpoint API

```bash
# Cek kesehatan service
curl http://localhost:8080/health

# Publish 1 event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"orders","event_id":"evt-001","timestamp":"2026-06-19T10:00:00Z","source":"manual-test","payload":{"amount":100}}'

# Kirim event YANG SAMA lagi -> harus duplicate_dropped
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"orders","event_id":"evt-001","timestamp":"2026-06-19T10:00:01Z","source":"manual-test","payload":{"amount":100}}'

# Lihat event per topic
curl "http://localhost:8080/events?topic=orders&limit=50"

# Lihat statistik
curl http://localhost:8080/stats
```

### Menjalankan Tests

```bash
# Pastikan stack sudah up dan publisher sudah selesai mengirim event
cd tests
pip install -r requirements.txt --break-system-packages

# Jalankan semua test fungsional (tidak termasuk test persistensi manual)
pytest -v --ignore=test_persistence.py

# Jalankan test persistensi (otomatis melakukan restart container storage)
chmod +x run_persistence_test.sh
./run_persistence_test.sh
```

### Membuktikan Persistensi Data Secara Manual

```bash
# 1. Publish event
curl -X POST http://localhost:8080/publish -H "Content-Type: application/json" \
  -d '{"topic":"demo","event_id":"persist-demo-1","timestamp":"2026-06-19T10:00:00Z","source":"demo","payload":{}}'

# 2. Hapus dan buat ulang container storage (simulasi crash total)
docker compose stop storage
docker compose rm -f storage
docker compose up -d storage

# 3. Tunggu Postgres siap, lalu cek event masih ada (volume named pg_data tidak terhapus)
sleep 8
curl "http://localhost:8080/events?topic=demo"
# -> event persist-demo-1 tetap muncul

# 4. Kirim ulang event yang sama -> tetap terdeteksi duplicate
curl -X POST http://localhost:8080/publish -H "Content-Type: application/json" \
  -d '{"topic":"demo","event_id":"persist-demo-1","timestamp":"2026-06-19T10:05:00Z","source":"demo","payload":{}}'
# -> {"accepted":0,"duplicate_dropped":1,...}
```

### Menghentikan & Membersihkan

```bash
docker compose down            # Stop semua service (volume tetap ada)
docker compose down -v         # Stop + hapus semua volume (reset total)
```

---

## 4. Asumsi & Keputusan Desain

- **Bahasa:** Python (FastAPI + asyncio) — dipilih karena ekosistem async matang untuk I/O-bound workload (HTTP + DB + Redis).
- **Broker:** Redis Stream (bukan Pub/Sub biasa) — dipilih karena mendukung consumer group, acknowledgement (XACK), dan pending entry list (PEL) untuk crash recovery, yang tidak dimiliki Redis Pub/Sub murni.
- **Dedup ganda (cache + DB):** Redis sebagai *fast path* mengurangi beban query ke Postgres pada kasus duplikat masif, namun Postgres tetap menjadi *source of truth* karena persisten dan punya constraint atomik.
- **Isolation level READ COMMITTED:** dipilih karena race condition pada INSERT dicegah oleh unique constraint (bukan oleh isolation level transaksi itu sendiri), sehingga SERIALIZABLE tidak diperlukan dan hanya menambah overhead/retry.
- **Partial commit pada batch:** setiap event dalam batch diproses sebagai transaksi independen, sehingga satu event gagal tidak membatalkan event lain dalam batch yang sama (lebih sesuai semantik idempotent at-least-once daripada all-or-nothing).
- **Jaringan:** semua service berada dalam satu Docker network internal (`internal`); hanya `aggregator` yang mem-publish port ke host (`8080:8080`) untuk keperluan demo lokal — `broker` dan `storage` tidak memiliki port exposed ke luar Compose.

---

## 5. Link Video Demo

 `https://youtu.be/XXXXXXXXXXX`

