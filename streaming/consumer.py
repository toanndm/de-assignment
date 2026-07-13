"""
consumer.py
===========
Reads from the Kafka topic 'events' and writes processed records
into PostgreSQL in near-real-time.

Processing steps per record:
  1. Parse & type-cast all fields (string → proper Python types)
  2. Flag is_anomaly = True when total_value < 0
  3. Batch-insert into fact_events via UPSERT (ON CONFLICT DO NOTHING)
  4. Commit Kafka offset only after successful DB write (at-least-once)

Usage:
    python streaming/consumer.py [--topic TOPIC] [--batch-size N] [--group GROUP]

Defaults:
    --topic       events
    --batch-size  100
    --group       de_assessment_consumer
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC     = "events"
DEFAULT_GROUP     = "de_assessment_consumer"
DEFAULT_BATCH     = 100

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "assessment",
    "user":     "de",
    "password": "de",
}

# ---------------------------------------------------------------------------
# Target table DDL (created if not exists)
# Consumer writes to its own table to avoid conflicting with the batch DAG
# ---------------------------------------------------------------------------

DDL_STREAMING_TABLE = """
CREATE TABLE IF NOT EXISTS streaming_events (
    event_id          UUID        PRIMARY KEY,
    event_timestamp   TIMESTAMPTZ NOT NULL,
    entity_id         INTEGER     NOT NULL,
    zone_id           INTEGER,
    destination_id    INTEGER,
    vendor_id         SMALLINT,
    event_type        TEXT,
    rate_type         SMALLINT,
    duration_seconds  INTEGER,
    passenger_count   SMALLINT,
    value             NUMERIC(10,2),
    sub_value         NUMERIC(10,2),
    total_value       NUMERIC(10,2),
    payment_method    TEXT,
    is_anomaly        BOOLEAN     NOT NULL DEFAULT FALSE,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_streaming_events_ts
    ON streaming_events (event_timestamp);
"""

UPSERT_SQL = """
INSERT INTO streaming_events (
    event_id, event_timestamp, entity_id, zone_id, destination_id,
    vendor_id, event_type, rate_type, duration_seconds,
    passenger_count, value, sub_value, total_value,
    payment_method, is_anomaly
) VALUES %s
ON CONFLICT (event_id) DO NOTHING;
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_int(val: Any, default: int | None = None) -> int | None:
    try:
        return int(float(val)) if val not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def safe_float(val: Any, default: float | None = None) -> float | None:
    try:
        return round(float(val), 2) if val not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def parse_record(raw: dict) -> tuple | None:
    """
    Parse and type-cast a raw Kafka message dict.
    Returns a tuple ready for psycopg2 execute_values, or None if invalid.
    """
    event_id = raw.get("event_id", "").strip()
    if not event_id:
        return None

    try:
        event_timestamp = datetime.fromisoformat(
            raw.get("event_timestamp", "").replace(" ", "T")
        ).replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        log.warning("Cannot parse timestamp for event_id=%s, skipping.", event_id)
        return None

    total_value = safe_float(raw.get("total_value"))
    is_anomaly  = bool(total_value is not None and total_value < 0)

    return (
        event_id,
        event_timestamp,
        safe_int(raw.get("entity_id")),
        safe_int(raw.get("zone_id")),
        safe_int(raw.get("destination_id")),
        safe_int(raw.get("vendor_id")),
        raw.get("event_type", "").strip().lower() or None,
        safe_int(raw.get("rate_type")),
        safe_int(raw.get("duration")),
        safe_int(raw.get("passenger_count")),
        safe_float(raw.get("value")),
        safe_float(raw.get("sub_value")),
        total_value,
        raw.get("payment_method", "").strip().lower() or None,
        is_anomaly,
    )


def connect_db() -> psycopg2.extensions.connection:
    """Connect to PostgreSQL with retry logic."""
    for attempt in range(1, 6):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            log.info("Connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("DB connection attempt %d failed: %s", attempt, exc)
            time.sleep(3 * attempt)
    log.error("Could not connect to PostgreSQL after 5 attempts.")
    sys.exit(1)


def ensure_table(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_STREAMING_TABLE)
    conn.commit()
    log.info("streaming_events table ready.")


def insert_batch(
    conn: psycopg2.extensions.connection,
    batch: list[tuple],
) -> int:
    """Batch-insert records into streaming_events. Returns rows inserted."""
    if not batch:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, batch, page_size=200)
        inserted = cur.rowcount
    conn.commit()
    return max(inserted, 0)


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown = False

def _handle_signal(signum, frame) -> None:
    global _shutdown
    log.info("Shutdown signal received (%s), draining current batch...", signum)
    _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def run(topic: str, group: str, batch_size: int, bootstrap: str) -> None:
    log.info("Starting consumer | topic=%s group=%s batch=%d", topic, group, batch_size)

    # --- DB setup ---
    conn = connect_db()
    ensure_table(conn)

    # --- Kafka setup ---
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id=group,
            auto_offset_reset="earliest",   # start from beginning if no offset stored
            enable_auto_commit=False,        # manual commit after successful DB write
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            # Poll settings
            max_poll_records=batch_size * 2,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
            fetch_max_wait_ms=500,
        )
    except NoBrokersAvailable as exc:
        log.error("Cannot connect to Kafka: %s", exc)
        conn.close()
        sys.exit(1)

    log.info("Consumer subscribed to topic '%s'. Waiting for messages...", topic)

    batch: list[tuple] = []
    total_inserted  = 0
    total_skipped   = 0
    total_anomalies = 0
    last_log_ts     = time.monotonic()

    try:
        while not _shutdown:
            # Poll with 1-second timeout so shutdown signal is checked regularly
            records = consumer.poll(timeout_ms=1000)

            for _tp, messages in records.items():
                for msg in messages:
                    record = parse_record(msg.value)
                    if record is None:
                        total_skipped += 1
                        continue
                    batch.append(record)
                    if record[-1]:   # is_anomaly flag (last element)
                        total_anomalies += 1

                    # Flush batch when it reaches target size
                    if len(batch) >= batch_size:
                        inserted = insert_batch(conn, batch)
                        total_inserted += inserted
                        consumer.commit()   # commit AFTER successful DB write
                        batch.clear()

            # Progress log every 30 seconds
            now = time.monotonic()
            if now - last_log_ts >= 30:
                log.info(
                    "Progress: inserted=%d skipped=%d anomalies=%d",
                    total_inserted, total_skipped, total_anomalies,
                )
                last_log_ts = now

    except KafkaError as exc:
        log.error("Kafka error: %s", exc)
    except Exception as exc:
        log.exception("Unexpected error: %s", exc)
    finally:
        # Drain remaining batch before exit
        if batch:
            log.info("Draining remaining %d records...", len(batch))
            inserted = insert_batch(conn, batch)
            total_inserted += inserted
            consumer.commit()

        consumer.close()
        conn.close()
        log.info(
            "Consumer stopped. total_inserted=%d total_skipped=%d total_anomalies=%d",
            total_inserted, total_skipped, total_anomalies,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka event consumer for DE Assessment")
    parser.add_argument("--topic",      default=DEFAULT_TOPIC,           help="Kafka topic name")
    parser.add_argument("--group",      default=DEFAULT_GROUP,           help="Kafka consumer group ID")
    parser.add_argument("--batch-size", default=DEFAULT_BATCH, type=int, help="DB insert batch size")
    parser.add_argument("--bootstrap",  default=DEFAULT_BOOTSTRAP,       help="Kafka bootstrap server(s)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        topic      = args.topic,
        group      = args.group,
        batch_size = args.batch_size,
        bootstrap  = args.bootstrap,
    )
