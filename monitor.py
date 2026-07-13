"""
monitor.py
==========
Quick health check for the DE Assessment pipeline.
Connects to PostgreSQL and prints a status summary.

Usage:
    python monitor.py

Run this after the pipeline to verify everything loaded correctly.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "assessment",
    "user":     "de",
    "password": "de",
}

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"  {GREEN}✓{RESET}  {msg}")
def warn(msg):  print(f"  {YELLOW}⚠{RESET}  {msg}")
def fail(msg):  print(f"  {RED}✗{RESET}  {msg}")
def header(msg):print(f"\n{BOLD}{msg}{RESET}")


def run_checks():
    print(f"\n{BOLD}=== DE Assessment Pipeline Monitor ==={RESET}")
    print(f"    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")

    issues = 0

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        ok("Connected to PostgreSQL")
    except psycopg2.OperationalError as e:
        fail(f"Cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # ------------------------------------------------------------------
    header("1. Schema checks")
    # ------------------------------------------------------------------
    tables = [
        "dim_event_type", "dim_payment_method", "dim_vendor",
        "dim_zone", "raw_events", "fact_events", "streaming_events"
    ]
    for table in tables:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_name = %s)", (table,)
        )
        exists = cur.fetchone()["exists"]
        if exists:
            ok(f"Table '{table}' exists")
        else:
            # streaming_events only exists after consumer runs
            if table == "streaming_events":
                warn(f"Table '{table}' not found (consumer not run yet?)")
            else:
                fail(f"Table '{table}' MISSING")
                issues += 1

    # ------------------------------------------------------------------
    header("2. Row counts")
    # ------------------------------------------------------------------
    count_checks = [
        ("raw_events",   287_000, "rows in staging"),
        ("fact_events",  287_000, "rows in fact table"),
    ]
    for table, minimum, label in count_checks:
        cur.execute(f"SELECT COUNT(*) AS n FROM {table}")
        n = cur.fetchone()["n"]
        if n >= minimum:
            ok(f"{table}: {n:,} {label}")
        elif n > 0:
            warn(f"{table}: {n:,} {label} (expected ~287,924)")
            issues += 1
        else:
            fail(f"{table}: 0 rows — pipeline did not load data")
            issues += 1

    # streaming_events (optional)
    try:
        cur.execute("SELECT COUNT(*) AS n FROM streaming_events")
        n = cur.fetchone()["n"]
        if n > 0:
            ok(f"streaming_events: {n:,} rows (Kafka consumer has run)")
        else:
            warn("streaming_events: 0 rows (run producer + consumer to populate)")
    except psycopg2.errors.UndefinedTable:
        warn("streaming_events: table not created yet")

    # ------------------------------------------------------------------
    header("3. Data quality")
    # ------------------------------------------------------------------

    # Anomaly count
    cur.execute("SELECT COUNT(*) AS n FROM fact_events WHERE is_anomaly = TRUE")
    anomalies = cur.fetchone()["n"]
    if anomalies > 0:
        ok(f"Anomalies flagged (negative total_value): {anomalies:,}")
    else:
        warn("No anomalies flagged — expected ~863")

    # Duplicate check
    cur.execute("""
        SELECT COUNT(*) AS dupes FROM (
            SELECT event_id, COUNT(*) c
            FROM fact_events
            GROUP BY event_id
            HAVING COUNT(*) > 1
        ) t
    """)
    dupes = cur.fetchone()["dupes"]
    if dupes == 0:
        ok("No duplicate event_ids in fact_events")
    else:
        fail(f"{dupes:,} duplicate event_ids found — idempotency issue")
        issues += 1

    # NULL event_type_id
    cur.execute("SELECT COUNT(*) AS n FROM fact_events WHERE event_type_id IS NULL")
    null_et = cur.fetchone()["n"]
    if null_et == 0:
        ok("No NULL event_type_id")
    else:
        warn(f"{null_et:,} rows with NULL event_type_id (unresolved FK)")
        issues += 1

    # NULL payment_method_id
    cur.execute("SELECT COUNT(*) AS n FROM fact_events WHERE payment_method_id IS NULL")
    null_pm = cur.fetchone()["n"]
    if null_pm == 0:
        ok("No NULL payment_method_id")
    else:
        warn(f"{null_pm:,} rows with NULL payment_method_id")
        issues += 1

    # ------------------------------------------------------------------
    header("4. Date range")
    # ------------------------------------------------------------------
    cur.execute("""
        SELECT
            MIN(event_timestamp) AS earliest,
            MAX(event_timestamp) AS latest,
            COUNT(DISTINCT DATE_TRUNC('month', event_timestamp)) AS months
        FROM fact_events
    """)
    row = cur.fetchone()
    if row["earliest"]:
        ok(f"Earliest event : {row['earliest']}")
        ok(f"Latest event   : {row['latest']}")
        ok(f"Months covered : {row['months']} (expected 3 — Oct/Nov/Dec 2024)")
    else:
        fail("Cannot read date range — fact_events may be empty")
        issues += 1

    # ------------------------------------------------------------------
    header("5. Dimension tables")
    # ------------------------------------------------------------------
    for dim, expected in [("dim_event_type", 5), ("dim_payment_method", 4), ("dim_vendor", 2)]:
        cur.execute(f"SELECT COUNT(*) AS n FROM {dim}")
        n = cur.fetchone()["n"]
        if n == expected:
            ok(f"{dim}: {n} rows")
        else:
            warn(f"{dim}: {n} rows (expected {expected})")
            issues += 1

    cur.execute("SELECT COUNT(*) AS n FROM dim_zone")
    n = cur.fetchone()["n"]
    ok(f"dim_zone: {n:,} distinct zones")

    # ------------------------------------------------------------------
    header("6. Quick revenue sanity check")
    # ------------------------------------------------------------------
    cur.execute("""
        SELECT
            ROUND(SUM(total_value), 2)  AS total_revenue,
            ROUND(AVG(total_value), 2)  AS avg_per_event,
            ROUND(MIN(total_value), 2)  AS min_value,
            ROUND(MAX(total_value), 2)  AS max_value
        FROM fact_events
        WHERE is_anomaly = FALSE
    """)
    row = cur.fetchone()
    ok(f"Total revenue  : ${row['total_revenue']:,.2f}")
    ok(f"Avg per event  : ${row['avg_per_event']:,.2f}")
    ok(f"Value range    : ${row['min_value']:,.2f} → ${row['max_value']:,.2f}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'─'*40}")
    if issues == 0:
        print(f"{GREEN}{BOLD}All checks passed ✓{RESET}")
    else:
        print(f"{RED}{BOLD}{issues} issue(s) found — review warnings above{RESET}")

    cur.close()
    conn.close()
    return issues


if __name__ == "__main__":
    issues = run_checks()
    sys.exit(0 if issues == 0 else 1)
