"""
pipeline.py
===========
Airflow DAG — DE Assessment ingestion & transformation pipeline.

Schedule : daily  (@daily)
Idempotent: yes — uses ON CONFLICT DO NOTHING + TRUNCATE staging
Re-run    : safe at any time

Task flow:
    create_schema  →  ingest_to_staging  →  transform_to_fact  →  validate_and_log
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# DAG default args
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "de_assessment",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# Airflow Connection ID pointing to the assessment Postgres
# Configure via: Admin → Connections → postgres_assessment
#   host=postgres, port=5432, schema=assessment, login=de, password=de
POSTGRES_CONN_ID = "postgres_assessment"

# Path to the CSV inside the Airflow container (mounted read-only)
CSV_PATH = "/opt/airflow/data/de_assessment_data.csv"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL helpers (kept inline so the DAG is self-contained)
# ---------------------------------------------------------------------------

SQL_CREATE_SCHEMA = dedent("""
    -- Dimension: event types
    CREATE TABLE IF NOT EXISTS dim_event_type (
        id   SMALLINT PRIMARY KEY,
        name TEXT     NOT NULL UNIQUE
    );
    INSERT INTO dim_event_type (id, name) VALUES
        (1,'standard'),(2,'express'),(3,'premium'),(4,'bulk'),(5,'scheduled')
    ON CONFLICT DO NOTHING;

    -- Dimension: payment methods
    CREATE TABLE IF NOT EXISTS dim_payment_method (
        id   SMALLINT PRIMARY KEY,
        name TEXT     NOT NULL UNIQUE
    );
    INSERT INTO dim_payment_method (id, name) VALUES
        (1,'card'),(2,'cash'),(3,'account'),(4,'voucher')
    ON CONFLICT DO NOTHING;

    -- Dimension: vendors (SCD Type 1)
    CREATE TABLE IF NOT EXISTS dim_vendor (
        id   SMALLINT PRIMARY KEY,
        name TEXT     NOT NULL
    );
    INSERT INTO dim_vendor (id, name) VALUES
        (1,'Vendor A'),(2,'Vendor B')
    ON CONFLICT DO NOTHING;

    -- Dimension: zones (populated from data)
    CREATE TABLE IF NOT EXISTS dim_zone (
        zone_id   INTEGER PRIMARY KEY,
        zone_name TEXT
    );

    -- Raw staging (all text, mirrors CSV)
    CREATE TABLE IF NOT EXISTS raw_events (
        event_id        TEXT,
        event_timestamp TEXT,
        entity_id       TEXT,
        zone_id         TEXT,
        destination_id  TEXT,
        vendor_id       TEXT,
        event_type      TEXT,
        rate_type       TEXT,
        duration        TEXT,
        passenger_count TEXT,
        value           TEXT,
        sub_value       TEXT,
        total_value     TEXT,
        payment_method  TEXT,
        loaded_at       TIMESTAMPTZ DEFAULT NOW()
    );

    -- Fact table (clean, typed, normalized)
    CREATE TABLE IF NOT EXISTS fact_events (
        event_id          UUID        PRIMARY KEY,
        event_timestamp   TIMESTAMPTZ NOT NULL,
        entity_id         INTEGER     NOT NULL,
        zone_id           INTEGER     REFERENCES dim_zone(zone_id),
        destination_id    INTEGER,
        vendor_id         SMALLINT    REFERENCES dim_vendor(id),
        event_type_id     SMALLINT    REFERENCES dim_event_type(id),
        rate_type         SMALLINT    CHECK (rate_type BETWEEN 1 AND 6),
        duration_seconds  INTEGER     CHECK (duration_seconds >= 0),
        passenger_count   SMALLINT,
        value             NUMERIC(10,2),
        sub_value         NUMERIC(10,2),
        total_value       NUMERIC(10,2),
        payment_method_id SMALLINT    REFERENCES dim_payment_method(id),
        is_anomaly        BOOLEAN     NOT NULL DEFAULT FALSE,
        ingested_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_fact_events_timestamp
        ON fact_events (event_timestamp);

    -- Note: monthly rollup queries use idx_fact_events_timestamp above.
    -- DATE_TRUNC index on TIMESTAMPTZ is not allowed (not IMMUTABLE).
    CREATE INDEX IF NOT EXISTS idx_fact_events_entity
        ON fact_events (entity_id);
    CREATE INDEX IF NOT EXISTS idx_fact_events_zone
        ON fact_events (zone_id);
    CREATE INDEX IF NOT EXISTS idx_fact_events_event_type
        ON fact_events (event_type_id);
    CREATE INDEX IF NOT EXISTS idx_fact_events_anomaly
        ON fact_events (is_anomaly) WHERE is_anomaly = TRUE;
""")

SQL_POPULATE_ZONES = dedent("""
    INSERT INTO dim_zone (zone_id)
    SELECT DISTINCT zone_id::INTEGER
    FROM   raw_events
    WHERE  zone_id IS NOT NULL
      AND  zone_id ~ '^\\d+$'
    ON CONFLICT DO NOTHING;
""")

SQL_UPSERT_VENDORS = dedent("""
    -- Auto-register new vendors found in staging (SCD1).
    -- ON CONFLICT DO NOTHING ensures idempotency.
    INSERT INTO dim_vendor (id, name)
    SELECT DISTINCT r.vendor_id::SMALLINT, 'Vendor ' || r.vendor_id
    FROM raw_events r
    WHERE r.vendor_id IS NOT NULL
      AND r.vendor_id ~ '^\\d+$'
    ON CONFLICT DO NOTHING;
""")

SQL_TRANSFORM = dedent("""
    INSERT INTO fact_events (
        event_id, event_timestamp, entity_id, zone_id, destination_id,
        vendor_id, event_type_id, rate_type, duration_seconds,
        passenger_count, value, sub_value, total_value,
        payment_method_id, is_anomaly
    )
    SELECT
        r.event_id::UUID,
        r.event_timestamp::TIMESTAMPTZ,
        r.entity_id::INTEGER,
        r.zone_id::INTEGER,
        r.destination_id::INTEGER,
        r.vendor_id::SMALLINT,
        et.id,
        r.rate_type::SMALLINT,
        r.duration::INTEGER,
        r.passenger_count::NUMERIC::SMALLINT,
        r.value::NUMERIC(10,2),
        r.sub_value::NUMERIC(10,2),
        r.total_value::NUMERIC(10,2),
        pm.id,
        (r.total_value::NUMERIC < 0)
    FROM raw_events r
    LEFT JOIN dim_event_type et
        ON LOWER(TRIM(r.event_type)) = et.name
    LEFT JOIN dim_payment_method pm
        ON LOWER(TRIM(r.payment_method)) = pm.name
    WHERE r.event_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    ON CONFLICT (event_id) DO NOTHING;
""")

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def create_schema(**context) -> None:
    """Create all tables and indexes if they don't already exist."""
    log.info("Creating schema (idempotent)...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(SQL_CREATE_SCHEMA)
    log.info("Schema creation complete.")


def ingest_to_staging(**context) -> None:
    """
    Truncate raw_events and load the CSV into it.
    Uses psycopg2 copy_expert to stream the file from the Airflow
    container to Postgres — avoids server-side COPY path issues.
    """
    log.info("Starting CSV ingestion to raw_events staging table...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Truncate staging for a clean load
    hook.run("TRUNCATE TABLE raw_events;")
    log.info("raw_events truncated.")

    # Use copy_expert: file is read by the Python process (Airflow container)
    # and streamed to Postgres — no need for Postgres to access the file directly
    copy_sql = """
        COPY raw_events (
            event_id, event_timestamp, entity_id, zone_id, destination_id,
            vendor_id, event_type, rate_type, duration, passenger_count,
            value, sub_value, total_value, payment_method
        )
        FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '');
    """
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            with open(CSV_PATH, "r", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)
        conn.commit()
        log.info("CSV streamed into raw_events via copy_expert.")
    except Exception:
        conn.rollback()
        raise

    # Count rows for logging
    row_count = hook.get_first("SELECT COUNT(*) FROM raw_events;")[0]
    log.info("Ingested %d rows into raw_events.", row_count)

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="raw_row_count", value=row_count)


def transform_to_fact(**context) -> None:
    """
    1. Populate dim_zone from staging.
    2. Auto-register any new vendor_ids found in staging (SCD1).
    3. Transform raw_events → fact_events (ON CONFLICT DO NOTHING).
    """
    log.info("Starting transformation: raw_events → fact_events...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Populate zone dimension
    hook.run(SQL_POPULATE_ZONES)
    zone_count = hook.get_first("SELECT COUNT(*) FROM dim_zone;")[0]
    log.info("dim_zone populated: %d zones.", zone_count)

    # Auto-register new vendors (SCD1 — ON CONFLICT DO NOTHING)
    hook.run(SQL_UPSERT_VENDORS)
    vendor_count = hook.get_first(
        "SELECT COUNT(*) FROM dim_vendor;"
    )[0]
    log.info("dim_vendor records: %d.", vendor_count)

    # Transform
    hook.run(SQL_TRANSFORM)

    fact_count = hook.get_first("SELECT COUNT(*) FROM fact_events;")[0]
    log.info("fact_events total rows: %d.", fact_count)
    context["ti"].xcom_push(key="fact_row_count", value=fact_count)


def validate_and_log(**context) -> None:
    """
    Run post-load checks and emit a summary log.
    Raises ValueError if critical checks fail.
    """
    log.info("Running post-load validation...")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ti = context["ti"]

    raw_count  = ti.xcom_pull(task_ids="ingest_to_staging",  key="raw_row_count")
    fact_count = ti.xcom_pull(task_ids="transform_to_fact",  key="fact_row_count")

    # Anomaly count
    anomaly_count = hook.get_first(
        "SELECT COUNT(*) FROM fact_events WHERE is_anomaly = TRUE;"
    )[0]

    # Null event_type check (failed FK lookup)
    null_event_type = hook.get_first(
        "SELECT COUNT(*) FROM fact_events WHERE event_type_id IS NULL;"
    )[0]

    # Null payment_method check
    null_payment = hook.get_first(
        "SELECT COUNT(*) FROM fact_events WHERE payment_method_id IS NULL;"
    )[0]

    log.info(
        "Validation summary:\n"
        "  raw_events rows    : %d\n"
        "  fact_events rows   : %d\n"
        "  anomalies flagged  : %d\n"
        "  null event_type_id : %d\n"
        "  null payment_method: %d",
        raw_count or 0,
        fact_count or 0,
        anomaly_count,
        null_event_type,
        null_payment,
    )

    # Fail the DAG if no data was loaded
    if not fact_count or fact_count == 0:
        raise ValueError("fact_events is empty after transform — pipeline failed.")

    # Warn (but don't fail) if FK lookups have unexpected nulls
    if null_event_type > 0:
        log.warning("%d rows have NULL event_type_id — check dim_event_type values.", null_event_type)
    if null_payment > 0:
        log.warning("%d rows have NULL payment_method_id — check dim_payment_method values.", null_payment)

    log.info("Validation passed. Pipeline run complete.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="de_assessment_pipeline",
    description="Ingest de_assessment_data.csv → raw staging → fact_events",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 10, 1),
    schedule="@daily",
    catchup=False,          # don't backfill historical runs
    max_active_runs=1,      # prevent concurrent runs causing duplicates
    tags=["assessment", "ingestion"],
) as dag:

    t_create_schema = PythonOperator(
        task_id="create_schema",
        python_callable=create_schema,
    )

    t_ingest = PythonOperator(
        task_id="ingest_to_staging",
        python_callable=ingest_to_staging,
    )

    t_transform = PythonOperator(
        task_id="transform_to_fact",
        python_callable=transform_to_fact,
    )

    t_validate = PythonOperator(
        task_id="validate_and_log",
        python_callable=validate_and_log,
    )

    # Task dependency chain
    t_create_schema >> t_ingest >> t_transform >> t_validate
