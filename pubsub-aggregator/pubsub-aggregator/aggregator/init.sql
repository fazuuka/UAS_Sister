-- Schema untuk Pub-Sub Log Aggregator
-- Idempotency & Deduplication via UNIQUE constraint (topic, event_id)

CREATE TABLE IF NOT EXISTS processed_events (
    id          BIGSERIAL PRIMARY KEY,
    topic       VARCHAR(255)    NOT NULL,
    event_id    VARCHAR(255)    NOT NULL,
    source      VARCHAR(255)    NOT NULL,
    timestamp   TIMESTAMPTZ     NOT NULL,
    payload     JSONB           NOT NULL DEFAULT '{}',
    received_at TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- Constraint unik: dedup atomik, mencegah double-insert
    CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
);

-- Index untuk query GET /events?topic=...
CREATE INDEX IF NOT EXISTS idx_processed_events_topic
    ON processed_events (topic, received_at DESC);

-- Index untuk ordering by timestamp
CREATE INDEX IF NOT EXISTS idx_processed_events_timestamp
    ON processed_events (timestamp DESC);

-- Tabel statistik transaksional (bebas lost-update via UPDATE ... SET count = count + 1)
CREATE TABLE IF NOT EXISTS aggregator_stats (
    id                  INTEGER PRIMARY KEY DEFAULT 1,
    received            BIGINT NOT NULL DEFAULT 0,
    unique_processed    BIGINT NOT NULL DEFAULT 0,
    duplicate_dropped   BIGINT NOT NULL DEFAULT 0,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Pastikan hanya satu row statistik
    CONSTRAINT single_row CHECK (id = 1)
);

-- Seed row statistik
INSERT INTO aggregator_stats (id, received, unique_processed, duplicate_dropped)
VALUES (1, 0, 0, 0)
ON CONFLICT (id) DO NOTHING;

-- Tabel outbox untuk pattern outbox (opsional, bonus)
CREATE TABLE IF NOT EXISTS outbox (
    id          BIGSERIAL PRIMARY KEY,
    event_id    VARCHAR(255)    NOT NULL UNIQUE,
    topic       VARCHAR(255)    NOT NULL,
    payload     JSONB           NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    processed   BOOLEAN         NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
    ON outbox (processed, created_at)
    WHERE processed = FALSE;

-- Audit log: rekam setiap attempt (termasuk duplikat)
CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    event_id    VARCHAR(255)    NOT NULL,
    topic       VARCHAR(255)    NOT NULL,
    action      VARCHAR(50)     NOT NULL,  -- 'accepted' | 'duplicate_dropped' | 'invalid'
    worker_id   VARCHAR(50),
    logged_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_event
    ON audit_log (event_id, topic);
