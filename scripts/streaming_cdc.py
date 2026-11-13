"""Validate Debezium messages and store them idempotently in PostgreSQL."""

import os
import json
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb


ALLOWED_OPERATIONS = {"c", "u", "d", "r"}


def _message_value(message):
    error = message.error()
    if error:
        raise ValueError(f"Kafka message error: {error}")
    raw_value = message.value()
    if raw_value is None:
        return None
    try:
        if isinstance(raw_value, bytes):
            raw_value = raw_value.decode("utf-8")
        envelope = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("CDC message value must be valid UTF-8 JSON") from error
    return envelope


def _row_from_envelope(envelope):
    if not isinstance(envelope, dict):
        raise ValueError("Debezium message must be a JSON object")
    if isinstance(envelope.get("payload"), dict):
        envelope = envelope["payload"]

    operation = envelope.get("op")
    if operation not in ALLOWED_OPERATIONS:
        raise ValueError(f"unsupported Debezium operation: {operation!r}")
    before = envelope.get("before")
    after = envelope.get("after")
    row = before if operation == "d" else after
    if not isinstance(row, dict):
        raise ValueError(f"operation {operation!r} requires its row values")
    required = ("scenario_id", "port_call_id", "port_area_code", "updated_at_utc")
    missing = [field for field in required if row.get(field) is None]
    if missing:
        raise ValueError(
            f"Debezium row is missing required fields: {', '.join(missing)}"
        )
    if any(
        not isinstance(row[field], str) or not row[field].strip()
        for field in required[:3]
    ):
        raise ValueError(
            "scenario, call, and area identities must be non-empty strings"
        )

    timestamp = row["updated_at_utc"]
    if isinstance(timestamp, (int, float)):
        source_time = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    else:
        source_time = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    if source_time.utcoffset() is None:
        raise ValueError("updated_at_utc must include a timezone")
    return operation, before, after, row, source_time


def _kafka_identity(message):
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    if (
        type(partition) is not int
        or partition < 0
        or type(offset) is not int
        or offset < 0
    ):
        raise ValueError("Kafka partition and offset must be non-negative integers")
    return topic, partition, offset


def decode_debezium_message(message) -> dict | None:
    """Return a normalized CDC event, or None for a Kafka tombstone."""
    envelope = _message_value(message)
    if envelope is None:
        return None
    operation, before, after, row, source_time = _row_from_envelope(envelope)
    topic, partition, offset = _kafka_identity(message)
    return {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "operation": operation,
        "scenario_id": row["scenario_id"],
        "port_call_id": row["port_call_id"],
        "port_area_code": row["port_area_code"],
        "before": before,
        "after": after,
        "source_updated_at": source_time,
    }


def store_cdc_messages(messages, database_url: str) -> int:
    events = [decode_debezium_message(message) for message in messages]
    events = [event for event in events if event is not None]
    if not events:
        return 0
    with psycopg.connect(database_url) as connection:
        connection.cursor().executemany(
            """INSERT INTO stream_demo.port_eta_cdc_event
               (kafka_topic, kafka_partition, kafka_offset, operation, scenario_id,
                port_call_id, port_area_code, before_values, after_values,
                source_updated_at_utc)
               VALUES (%(topic)s, %(partition)s, %(offset)s, %(operation)s,
                       %(scenario_id)s, %(port_call_id)s, %(port_area_code)s,
                       %(before)s, %(after)s, %(source_updated_at)s)
               ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING""",
            [
                {
                    **event,
                    "before": Jsonb(event["before"]) if event["before"] else None,
                    "after": Jsonb(event["after"]) if event["after"] else None,
                }
                for event in events
            ],
        )
    return len(events)


def process_kafka_batch(messages) -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set for streaming CDC ingestion")
    stored = store_cdc_messages(messages, database_url)
    print(f"Processed Kafka batch; non-tombstone messages={stored}")
