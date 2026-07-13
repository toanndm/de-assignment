"""
producer.py
===========
Reads rows from de_assessment_data.csv and publishes them
to the Kafka topic 'events' one by one with a configurable delay.

Usage:
    python streaming/producer.py [--csv PATH] [--delay SECONDS] [--topic TOPIC]

Defaults:
    --csv    de_assessment_data.csv
    --delay  0.01  (100 messages/sec)
    --topic  events
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC     = "events"
DEFAULT_DELAY     = 0.01   # seconds between messages
DEFAULT_CSV       = Path(__file__).parent.parent / "de_assessment_data.csv"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create a KafkaProducer with JSON serialization and retry logic."""
    log.info("Connecting to Kafka at %s ...", bootstrap_servers)
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # Reliability settings
            acks="all",               # wait for leader + replicas to ack
            retries=5,
            retry_backoff_ms=500,
            # Throughput / latency balance
            linger_ms=5,              # small batch window
            compression_type="gzip",  # reduce network load
        )
        log.info("Kafka producer connected.")
        return producer
    except NoBrokersAvailable as exc:
        log.error("Cannot connect to Kafka: %s", exc)
        sys.exit(1)


def on_send_success(record_metadata) -> None:
    """Callback fired when a message is successfully acknowledged."""
    log.debug(
        "Delivered → topic=%s partition=%d offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(exc: Exception) -> None:
    """Callback fired on delivery failure."""
    log.error("Failed to deliver message: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(csv_path: Path, topic: str, delay: float, bootstrap: str) -> None:
    if not csv_path.exists():
        log.error("CSV file not found: %s", csv_path)
        sys.exit(1)

    producer = build_producer(bootstrap)

    sent      = 0
    errors    = 0
    start_ts  = time.monotonic()

    log.info("Starting to publish '%s' → topic '%s' (delay=%.3fs)", csv_path.name, topic, delay)

    try:
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # Send asynchronously with callbacks
                future = producer.send(topic, value=row)
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)

                sent += 1

                # Progress log every 10,000 messages
                if sent % 10_000 == 0:
                    elapsed = time.monotonic() - start_ts
                    rate    = sent / elapsed if elapsed > 0 else 0
                    log.info("Sent %d messages | %.1f msg/s", sent, rate)

                time.sleep(delay)

    except KeyboardInterrupt:
        log.info("Interrupted by user after %d messages.", sent)
    except Exception as exc:
        log.exception("Unexpected error after %d messages: %s", sent, exc)
        errors += 1
    finally:
        # Flush ensures all buffered messages are sent before exit
        log.info("Flushing producer buffer...")
        producer.flush()
        producer.close()

        elapsed = time.monotonic() - start_ts
        log.info(
            "Producer finished. sent=%d errors=%d elapsed=%.1fs",
            sent, errors, elapsed,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka event producer for DE Assessment")
    parser.add_argument("--csv",       default=str(DEFAULT_CSV),       help="Path to the CSV file")
    parser.add_argument("--topic",     default=DEFAULT_TOPIC,          help="Kafka topic name")
    parser.add_argument("--delay",     default=DEFAULT_DELAY, type=float, help="Delay between messages (seconds)")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP,      help="Kafka bootstrap server(s)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        csv_path  = Path(args.csv),
        topic     = args.topic,
        delay     = args.delay,
        bootstrap = args.bootstrap,
    )
