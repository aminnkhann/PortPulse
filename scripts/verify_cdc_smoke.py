"""Verify real PostgreSQL-to-Debezium-to-Kafka-to-Airflow CDC for one scenario."""

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer


TOPIC = "portpulse_stream.stream_demo.port_eta_state"
SCENARIO_ID = os.environ.get("CDC_SMOKE_SCENARIO_ID", "cdc-smoke-2026-11-13")
DATABASE_URL = os.environ.get("DATABASE_URL")
EXPECTED_OPERATIONS = {"SIM-001": ["c", "u", "u"], "SIM-002": ["c", "d"]}


def verify_kafka_capture() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
            ),
            "group.id": f"portpulse-smoke-{SCENARIO_ID}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    received = {"SIM-001": [], "SIM-002": []}
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline and any(
            received[key] != expected for key, expected in EXPECTED_OPERATIONS.items()
        ):
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Kafka consumer error: {message.error()}")
            if message.value() is None:
                continue
            envelope = json.loads(message.value())
            operation = envelope.get("op")
            row = envelope.get("after") or envelope.get("before") or {}
            if row.get("scenario_id") == SCENARIO_ID:
                received[row["port_call_id"]].append(operation)
                print(
                    f"Kafka CDC topic={message.topic()} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} op={operation} "
                    f"port_call={row['port_call_id']}"
                )
    finally:
        consumer.close()
    if received != EXPECTED_OPERATIONS:
        raise RuntimeError(
            f"Kafka did not contain expected CDC operations: {received}; "
            f"expected {EXPECTED_OPERATIONS}"
        )


def trigger_airflow() -> None:
    subprocess.run(
        ["airflow", "dags", "unpause", "portpulse_streaming_cdc"], check=True
    )
    subprocess.run(
        ["airflow", "dags", "trigger", "portpulse_streaming_cdc"], check=True
    )


def verify_airflow_history_and_dbt() -> None:
    deadline = time.monotonic() + 180
    expected_eta = datetime(2026, 11, 13, 14, 50, tzinfo=timezone.utc)
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(DATABASE_URL) as connection:
                operations = connection.execute(
                    """SELECT port_call_id, operation
                       FROM stream_demo.port_eta_cdc_event
                       WHERE scenario_id = %s
                       ORDER BY event_id""",
                    (SCENARIO_ID,),
                ).fetchall()
                latest = connection.execute(
                    """SELECT port_call_id, eta_utc
                       FROM analytics.mart_stream_latest_eta
                       WHERE scenario_id = %s ORDER BY port_call_id""",
                    (SCENARIO_ID,),
                ).fetchall()
            by_call = {}
            for call_id, operation in operations:
                by_call.setdefault(call_id, []).append(operation)
            expected_history = {"SIM-001": ["c", "u", "u"], "SIM-002": ["c", "d"]}
            if by_call == expected_history and latest == [("SIM-001", expected_eta)]:
                print(f"CDC history verified: {operations}")
                print(f"dbt current-state result verified: {latest}")
                return
        except psycopg.errors.UndefinedTable:
            pass
        time.sleep(3)
    raise RuntimeError(
        "Airflow did not store all five CDC events and build the expected mart "
        "within 180 seconds; inspect the streaming DAG task logs"
    )


def main() -> None:
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is required")
    verify_kafka_capture()
    trigger_airflow()
    verify_airflow_history_and_dbt()


if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, RuntimeError, ValueError) as error:
        print(f"CDC smoke test failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
