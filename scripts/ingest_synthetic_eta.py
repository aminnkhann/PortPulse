"""Validate and load the synthetic ETA JSONL file into its isolated schema."""

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path

import psycopg
from pydantic import ValidationError
from psycopg import sql
from psycopg.types.json import Jsonb

from scripts.event_model import PortCallEvent
from scripts.generate_synthetic_eta import OUTPUT
from scripts.ingest_fixture import (
    group_events,
    text_field,
    timestamp,
    validation_reason,
)


SCHEMA = "synthetic_demo"


def read_events(path):
    valid = []
    rejected = []

    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            rejected.append(
                {
                    "event_id": None,
                    "port_call_id": None,
                    "source_id": None,
                    "event_time_utc": None,
                    "received_at_utc": None,
                    "reason_code": "INVALID_JSON",
                    "raw_payload": line,
                    "details": {"message": error.msg},
                }
            )
            continue

        try:
            PortCallEvent.model_validate(payload)
        except ValidationError as error:
            reason, field = validation_reason(error)
            rejected.append(
                {
                    "event_id": text_field(payload, "event_id"),
                    "port_call_id": text_field(payload, "port_call_id"),
                    "source_id": text_field(payload, "source_id"),
                    "event_time_utc": timestamp(payload, "event_time_utc"),
                    "received_at_utc": timestamp(payload, "received_at_utc"),
                    "reason_code": reason,
                    "raw_payload": payload,
                    "details": {
                        "field": field,
                        "message": error.errors()[0]["msg"],
                    },
                }
            )
            continue

        valid.append(payload)

    return valid, rejected


def store_events(valid, rejected, database_url, schema=SCHEMA):
    unique_events, duplicate_attempts, collisions = group_events(valid)
    event_ids = sorted({event["event_id"] for event, _ in unique_events})
    inserted = 0

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    "SELECT event_id, canonical_payload_sha256 "
                    "FROM {} WHERE event_id = ANY(%s)"
                ).format(sql.Identifier(schema, "eta_observation")),
                (event_ids,),
            )
            prior_hashes = defaultdict(set)
            for event_id, payload_hash in cursor.fetchall():
                prior_hashes[event_id].add(payload_hash.strip())

            for item in rejected:
                cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (event_id, port_call_id, source_id, "
                        "event_time_utc, received_at_utc, reason_code, "
                        "raw_payload, details) VALUES (%s, %s, %s, %s, %s, %s, "
                        "%s, %s) ON CONFLICT DO NOTHING"
                    ).format(sql.Identifier(schema, "eta_rejected_event")),
                    (
                        item["event_id"],
                        item["port_call_id"],
                        item["source_id"],
                        item["event_time_utc"],
                        item["received_at_utc"],
                        item["reason_code"],
                        Jsonb(item["raw_payload"]),
                        Jsonb(item["details"]),
                    ),
                )

            for event, payload_hash in unique_events:
                known_hashes = prior_hashes[event["event_id"]]
                if known_hashes and payload_hash not in known_hashes:
                    collisions[event["event_id"]] = sorted(
                        {*known_hashes, payload_hash}
                    )
                cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (event_id, port_call_id, source_id, "
                        "event_time_utc, received_at_utc, eta_utc, schema_version, "
                        "canonical_payload_sha256, raw_payload) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (event_id, canonical_payload_sha256) DO NOTHING"
                    ).format(sql.Identifier(schema, "eta_observation")),
                    (
                        event["event_id"],
                        event["port_call_id"],
                        event["source_id"],
                        timestamp(event, "event_time_utc"),
                        timestamp(event, "received_at_utc"),
                        timestamp(event, "eta_utc"),
                        event["schema_version"],
                        payload_hash,
                        Jsonb(event),
                    ),
                )
                inserted += cursor.rowcount
                known_hashes.add(payload_hash)

            for event, payload_hash, number in duplicate_attempts:
                cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (issue_code, event_id, port_call_id, "
                        "received_at_utc, raw_payload, details) "
                        "VALUES ('EXACT_DUPLICATE', %s, %s, %s, %s, %s) "
                        "ON CONFLICT DO NOTHING"
                    ).format(sql.Identifier(schema, "eta_quality_issue")),
                    (
                        event["event_id"],
                        event["port_call_id"],
                        timestamp(event, "received_at_utc"),
                        Jsonb(event),
                        Jsonb(
                            {
                                "canonical_payload_sha256": payload_hash,
                                "duplicate_number": number,
                            }
                        ),
                    ),
                )

            for event_id, hashes in collisions.items():
                event = next(
                    event for event, _ in unique_events if event["event_id"] == event_id
                )
                cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (issue_code, event_id, port_call_id, "
                        "details) VALUES ('ID_COLLISION', %s, %s, %s) "
                        "ON CONFLICT DO NOTHING"
                    ).format(sql.Identifier(schema, "eta_quality_issue")),
                    (
                        event_id,
                        event["port_call_id"],
                        Jsonb(
                            {
                                "payload_hashes": hashes,
                                "variant_count": len(hashes),
                            }
                        ),
                    ),
                )

    return {
        "accepted": len(unique_events),
        "rejected": len(rejected),
        "duplicates": len(duplicate_attempts),
        "collisions": len(collisions),
        "inserted": inserted,
    }


def ingest_file(path, database_url, schema=SCHEMA):
    valid, rejected = read_events(path)
    return store_events(valid, rejected, database_url, schema)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=OUTPUT)
    return parser.parse_args()


def main():
    args = parse_args()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("Set DATABASE_URL to the PortPulse PostgreSQL database")
    try:
        counts = ingest_file(args.input, database_url)
    except OSError as error:
        raise SystemExit(f"Could not read synthetic ETA input {args.input}: {error}")
    except psycopg.Error as error:
        raise SystemExit(f"Could not ingest synthetic ETA data: {error}")

    print(f"Input file: {args.input}")
    for name, count in counts.items():
        print(f"{name.capitalize()}: {count}")


if __name__ == "__main__":
    main()
