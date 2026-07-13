-- =============================================================
-- schema.sql
-- Star schema for DE Assessment event data
-- Target: PostgreSQL 15
-- =============================================================

-- -------------------------------------------------------------
-- 1. DIMENSION TABLES
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_event_type (
    id   SMALLINT PRIMARY KEY,
    name TEXT     NOT NULL UNIQUE
);

INSERT INTO dim_event_type (id, name) VALUES
    (1, 'standard'),
    (2, 'express'),
    (3, 'premium'),
    (4, 'bulk'),
    (5, 'scheduled')
ON CONFLICT DO NOTHING;


CREATE TABLE IF NOT EXISTS dim_payment_method (
    id   SMALLINT PRIMARY KEY,
    name TEXT     NOT NULL UNIQUE
);

INSERT INTO dim_payment_method (id, name) VALUES
    (1, 'card'),
    (2, 'cash'),
    (3, 'account'),
    (4, 'voucher')
ON CONFLICT DO NOTHING;


-- dim_vendor — SCD Type 1
-- Simple lookup table. Name changes overwrite the existing record.
CREATE TABLE IF NOT EXISTS dim_vendor (
    id   SMALLINT PRIMARY KEY,
    name TEXT     NOT NULL
);

INSERT INTO dim_vendor (id, name) VALUES
    (1, 'Vendor A'),
    (2, 'Vendor B')
ON CONFLICT DO NOTHING;


-- Zone dimension — populated dynamically from fact data
CREATE TABLE IF NOT EXISTS dim_zone (
    zone_id INTEGER PRIMARY KEY,
    zone_name TEXT  -- can be enriched later
);


-- -------------------------------------------------------------
-- 2. RAW STAGING TABLE
-- Mirrors CSV exactly; all text to avoid parse errors on load
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw_events (
    event_id         TEXT,
    event_timestamp  TEXT,
    entity_id        TEXT,
    zone_id          TEXT,
    destination_id   TEXT,
    vendor_id        TEXT,
    event_type       TEXT,
    rate_type        TEXT,
    duration         TEXT,
    passenger_count  TEXT,
    value            TEXT,
    sub_value        TEXT,
    total_value      TEXT,
    payment_method   TEXT,
    loaded_at        TIMESTAMPTZ DEFAULT NOW()
);


-- -------------------------------------------------------------
-- 3. FACT TABLE
-- Cleaned, typed, normalized analytical table
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_events (
    event_id          UUID          PRIMARY KEY,
    event_timestamp   TIMESTAMPTZ   NOT NULL,
    entity_id         INTEGER       NOT NULL,
    zone_id           INTEGER       REFERENCES dim_zone(zone_id),
    destination_id    INTEGER,
    vendor_id         SMALLINT      REFERENCES dim_vendor(id),
    event_type_id     SMALLINT      REFERENCES dim_event_type(id),
    rate_type         SMALLINT      CHECK (rate_type BETWEEN 1 AND 6),
    duration_seconds  INTEGER       CHECK (duration_seconds >= 0),
    passenger_count   SMALLINT,
    value             NUMERIC(10,2),
    sub_value         NUMERIC(10,2),
    total_value       NUMERIC(10,2),
    payment_method_id SMALLINT      REFERENCES dim_payment_method(id),
    is_anomaly        BOOLEAN       NOT NULL DEFAULT FALSE,
    -- anomaly = negative total_value
    ingested_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------
-- 4. INDEXES for analytical query performance
-- -------------------------------------------------------------

-- Time-series queries (most common)
CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp
    ON fact_events (event_timestamp);

-- Note: monthly rollup queries use idx_fact_events_timestamp above.
-- DATE_TRUNC index on TIMESTAMPTZ is not IMMUTABLE — not supported by PostgreSQL.

-- Filtering by entity
CREATE INDEX IF NOT EXISTS idx_fact_events_entity
    ON fact_events (entity_id);

-- Filtering by zone
CREATE INDEX IF NOT EXISTS idx_fact_events_zone
    ON fact_events (zone_id);

-- Filtering by event type
CREATE INDEX IF NOT EXISTS idx_fact_events_event_type
    ON fact_events (event_type_id);

-- Filtering anomalies
CREATE INDEX IF NOT EXISTS idx_fact_events_anomaly
    ON fact_events (is_anomaly) WHERE is_anomaly = TRUE;
