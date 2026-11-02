"""Validate and load the v1 fixture into the local PostgreSQL database."""

import hashlib
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import psycopg
from jsonschema import Draft202012Validator, FormatChecker
from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures" / "port_call_v1.jsonl"
SCHEMA = ROOT / "contracts" / "port_call_event_v1.schema.json"
DEFAULT_DATABASE_URL = "postgresql://portpulse:portpulse_local_only@127.0.0.1:5432/portpulse"
HASH_FIELDS = (
    "event_id",
    "port_call_id",
    "source_id",
    "event_time_utc",
    "eta_utc",
    "schema_version",
)


def text_field(payload, name):
    value = payload.get(name) if isinstance(payload, dict) else None
    return value if isinstance(value, str) else None


def received_time(payload):
    return timestamp(payload, "received_at_utc")


def timestamp(payload, field):
    value = text_field(payload, field)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None
    except ValueError:
        return None


def validation_reason(error, schema):
    field = str(error.absolute_path[-1]) if error.absolute_path else None
    if error.validator == "required":
        missing = sorted(set(error.validator_value) - set(error.instance))[0]
        return "MISSING_REQUIRED_FIELD", missing
    if error.validator == "const" and field == "schema_version":
        return "INVALID_SCHEMA_VERSION", field
    if error.validator == "enum" and field == "source_id":
        return "UNKNOWN_SOURCE", field
    if error.validator in {"format", "pattern"} and field and field.endswith("_utc"):
        return "INVALID_TIMESTAMP", field
    if error.validator == "additionalProperties":
        extra = sorted(set(error.instance) - set(schema["properties"]))[0]
        return "UNKNOWN_FIELD", extra
    if error.validator == "type":
        return "INVALID_FIELD_TYPE", field
    return "INVALID_EVENT", field


def read_fixture():
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    valid = []
    rejected = []

    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            rejected.append({
                "event_id": None,
                "port_call_id": None,
                "received_at_utc": None,
                "reason_code": "INVALID_JSON",
                "raw_payload": line,
                "details": {"message": error.msg},
            })
            continue

        errors = sorted(
            validator.iter_errors(payload),
            key=lambda error: (tuple(map(str, error.absolute_path)), error.validator or ""),
        )
        if errors:
            reason, field = validation_reason(errors[0], schema)
            rejected.append({
                "event_id": text_field(payload, "event_id"),
                "port_call_id": text_field(payload, "port_call_id"),
                "received_at_utc": received_time(payload),
                "reason_code": reason,
                "raw_payload": payload,
                "details": {"field": field, "message": errors[0].message},
            })
            continue

        valid.append(payload)

    return valid, rejected


def canonical_hash(event):
    fields = {name: event[name] for name in HASH_FIELDS}
    value = json.dumps(fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def group_events(events):
    groups = defaultdict(lambda: defaultdict(list))
    for event in events:
        groups[event["event_id"]][canonical_hash(event)].append(event)

    unique_events = []
    duplicate_attempts = []
    collisions = {}
    for event_id, variants in sorted(groups.items()):
        if len(variants) > 1:
            collisions[event_id] = sorted(variants)
        for payload_hash, attempts in sorted(variants.items()):
            attempts.sort(key=lambda event: (event["received_at_utc"], json.dumps(event, sort_keys=True)))
            unique_events.append((attempts[0], payload_hash))
            duplicate_attempts.extend(
                (event, payload_hash, number)
                for number, event in enumerate(attempts[1:], start=1)
            )

    return unique_events, duplicate_attempts, collisions


def insert_rejection(cursor, item):
    cursor.execute(
        """
        INSERT INTO rejected_event
            (event_id, port_call_id, received_at_utc, reason_code, raw_payload, details)
        SELECT %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM rejected_event
            WHERE reason_code = %s
              AND event_id IS NOT DISTINCT FROM %s
              AND received_at_utc IS NOT DISTINCT FROM %s
              AND raw_payload = %s
              AND details = %s
        )
        """,
        (
            item["event_id"], item["port_call_id"], item["received_at_utc"],
            item["reason_code"], Jsonb(item["raw_payload"]), Jsonb(item["details"]),
            item["reason_code"], item["event_id"], item["received_at_utc"],
            Jsonb(item["raw_payload"]), Jsonb(item["details"]),
        ),
    )


def insert_issue(cursor, issue_code, event_id, received_at, raw_payload, details):
    cursor.execute(
        """
        INSERT INTO quality_issue
            (issue_code, event_id, port_call_id, received_at_utc, raw_payload, details)
        SELECT %s, %s, %s, %s, %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM quality_issue
            WHERE issue_code = %s
              AND event_id IS NOT DISTINCT FROM %s
              AND received_at_utc IS NOT DISTINCT FROM %s
              AND details = %s
        )
        """,
        (
            issue_code, event_id, details.get("port_call_id"), received_at,
            Jsonb(raw_payload) if raw_payload is not None else None, Jsonb(details),
            issue_code, event_id, received_at, Jsonb(details),
        ),
    )


def store_fixture(valid, rejected):
    unique_events, duplicate_attempts, collisions = group_events(valid)
    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            for item in rejected:
                insert_rejection(cursor, item)

            for event, _ in unique_events:
                cursor.execute(
                    "INSERT INTO port_call (port_call_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (event["port_call_id"],),
                )
                cursor.execute(
                    """
                    INSERT INTO raw_event
                        (event_id, port_call_id, source_id, event_time_utc,
                         received_at_utc, eta_utc, schema_version,
                         canonical_payload_sha256, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, canonical_payload_sha256) DO NOTHING
                    """,
                    (
                        event["event_id"], event["port_call_id"], event["source_id"],
                        timestamp(event, "event_time_utc"), received_time(event),
                        datetime.fromisoformat(event["eta_utc"].replace("Z", "+00:00")),
                        event["schema_version"], canonical_hash(event), Jsonb(event),
                    ),
                )

            for event, payload_hash, number in duplicate_attempts:
                insert_issue(
                    cursor,
                    "EXACT_DUPLICATE",
                    event["event_id"],
                    received_time(event),
                    event,
                    {
                        "canonical_payload_sha256": payload_hash,
                        "duplicate_number": number,
                        "port_call_id": event["port_call_id"],
                    },
                )

            for event_id, hashes in collisions.items():
                event = next(item for item, digest in unique_events if item["event_id"] == event_id)
                insert_issue(
                    cursor,
                    "ID_COLLISION",
                    event_id,
                    None,
                    None,
                    {
                        "payload_hashes": hashes,
                        "variant_count": len(hashes),
                        "port_call_id": event["port_call_id"],
                    },
                )

    return len(unique_events), len(duplicate_attempts), len(collisions)


def main():
    valid, rejected = read_fixture()
    unique_count, duplicate_count, collision_count = store_fixture(valid, rejected)
    print(f"Fixture occurrences: {len(valid) + len(rejected)}")
    print(f"Unique valid payloads stored: {unique_count}")
    print(f"Rejected occurrences: {len(rejected)}")
    print(f"Exact duplicate attempts: {duplicate_count}")
    print(f"ID collision groups: {collision_count}")


if __name__ == "__main__":
    main()
