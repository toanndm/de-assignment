-- =============================================================
-- load.sql
-- Load data from CSV into raw staging, then transform to fact
-- Idempotent: safe to re-run multiple times without duplicates
-- Target: PostgreSQL 15
-- =============================================================

-- -------------------------------------------------------------
-- STEP 1: Truncate staging (fresh load each run)
-- -------------------------------------------------------------

TRUNCATE TABLE raw_events;


-- -------------------------------------------------------------
-- STEP 2: COPY CSV into staging
-- Path inside the postgres container (mounted via docker-compose)
-- -------------------------------------------------------------

COPY raw_events (
    event_id,
    event_timestamp,
    entity_id,
    zone_id,
    destination_id,
    vendor_id,
    event_type,
    rate_type,
    duration,
    passenger_count,
    value,
    sub_value,
    total_value,
    payment_method
)
FROM '/docker-entrypoint-initdb.d/de_assessment_data.csv'
WITH (
    FORMAT CSV,
    HEADER TRUE,
    NULL ''
);


-- -------------------------------------------------------------
-- STEP 3: Populate dim_zone from staging data
-- (zone_id values are discovered from the data itself)
-- -------------------------------------------------------------

INSERT INTO dim_zone (zone_id)
SELECT DISTINCT zone_id::INTEGER
FROM   raw_events
WHERE  zone_id IS NOT NULL
  AND  zone_id ~ '^\d+$'
ON CONFLICT DO NOTHING;


-- -------------------------------------------------------------
-- STEP 3b: Auto-register new vendors (SCD1)
--
-- If a vendor_id appears in the data but does not yet exist
-- in dim_vendor, insert it with a placeholder name.
-- Uses ON CONFLICT DO NOTHING for idempotency.
-- -------------------------------------------------------------

INSERT INTO dim_vendor (id, name)
SELECT DISTINCT r.vendor_id::SMALLINT, 'Vendor ' || r.vendor_id
FROM raw_events r
WHERE r.vendor_id IS NOT NULL
  AND r.vendor_id ~ '^\d+$'
ON CONFLICT DO NOTHING;


-- -------------------------------------------------------------
-- STEP 4: Transform raw_events → fact_events
-- - Cast all types
-- - Resolve FK lookups (event_type, payment_method)
-- - Flag anomalies (negative total_value)
-- - ON CONFLICT DO NOTHING ensures idempotency
-- -------------------------------------------------------------

INSERT INTO fact_events (
    event_id,
    event_timestamp,
    entity_id,
    zone_id,
    destination_id,
    vendor_id,
    event_type_id,
    rate_type,
    duration_seconds,
    passenger_count,
    value,
    sub_value,
    total_value,
    payment_method_id,
    is_anomaly
)
SELECT
    r.event_id::UUID,
    r.event_timestamp::TIMESTAMPTZ,
    r.entity_id::INTEGER,
    r.zone_id::INTEGER,
    r.destination_id::INTEGER,
    r.vendor_id::SMALLINT          AS vendor_id,
    et.id                          AS event_type_id,
    r.rate_type::SMALLINT,
    r.duration::INTEGER            AS duration_seconds,
    r.passenger_count::NUMERIC::SMALLINT,
    r.value::NUMERIC(10,2),
    r.sub_value::NUMERIC(10,2),
    r.total_value::NUMERIC(10,2),
    pm.id                          AS payment_method_id,
    -- flag negative total_value as anomaly
    (r.total_value::NUMERIC < 0)   AS is_anomaly
FROM raw_events r
-- resolve event_type name → id
LEFT JOIN dim_event_type et
    ON  LOWER(TRIM(r.event_type)) = et.name
-- resolve payment_method name → id
LEFT JOIN dim_payment_method pm
    ON  LOWER(TRIM(r.payment_method)) = pm.name
-- skip rows with unparseable event_id
WHERE r.event_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
ON CONFLICT (event_id) DO NOTHING;


-- -------------------------------------------------------------
-- STEP 5: Quick validation report
-- -------------------------------------------------------------

SELECT
    'raw_events'  AS table_name,
    COUNT(*)      AS row_count
FROM raw_events

UNION ALL

SELECT
    'fact_events',
    COUNT(*)
FROM fact_events

UNION ALL

SELECT
    'anomalies (negative total_value)',
    COUNT(*)
FROM fact_events
WHERE is_anomaly = TRUE;
