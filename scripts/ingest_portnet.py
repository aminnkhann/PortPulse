"""Fetch, validate, store, and report Portnet V1 ETA observations."""

import argparse
import gzip
import json
import os
from collections import Counter
from datetime import date, datetime, time, timezone
from urllib.error import URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import psycopg
from pydantic import ValidationError
from psycopg.types.json import Jsonb

from scripts import ingest_fixture
from scripts.event_model import MarineEtaEvent, PortnetEtaPayload
from scripts.event_storage import (
    existing_hashes,
    insert_quality_issue,
    record_payload_hash,
)


PORTNET_URL = "https://meri.digitraffic.fi/api/port-call/v1/port-calls"
USER_AGENT = "PortPulse/0.1"


def fetch_json(url):
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "Connection": "close",
            "Digitraffic-User": USER_AGENT,
        },
    )
    try:
        with urlopen(request, timeout=20) as response:
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return json.loads(body)
    except (URLError, OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Portnet request failed: {error}") from error


def fetch_port_calls(port, day):
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    filters = {"date": start.isoformat().replace("+00:00", "Z")}
    if port:
        filters["locode"] = port
    response = fetch_json(f"{PORTNET_URL}?{urlencode(filters)}")
    if not isinstance(response, dict) or not isinstance(
        response.get("portCalls"), list
    ):
        raise RuntimeError("Portnet response must contain a portCalls array")
    return response["portCalls"]


def _utc_timestamp(value):
    if not isinstance(value, str) or not value:
        raise ValueError("timestamp must be a non-empty string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("timestamp must be ISO-8601") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("timestamp must include a UTC timezone")
    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _safe_call(call):
    if not isinstance(call, dict):
        return call
    details = call.get("portAreaDetails")
    safe_details = (
        [
            {
                key: item.get(key)
                for key in (
                    "portAreaCode",
                    "eta",
                    "etaSource",
                    "etaTimestamp",
                    "ata",
                    "atd",
                )
            }
            if isinstance(item, dict)
            else item
            for item in details
        ]
        if isinstance(details, list)
        else details
    )
    return {"portCallId": call.get("portCallId"), "portAreaDetails": safe_details}


def _rejection(reason_code, payload, received_at, message, call_id=None, event_id=None):
    return {
        "event_id": event_id,
        "port_call_id": f"portnet:{call_id}" if call_id is not None else None,
        "received_at_utc": received_at,
        "reason_code": reason_code,
        "raw_payload": payload,
        "details": {"message": message},
    }


def _make_event(call_id, detail, received_at):
    area = detail.get("portAreaCode")
    if area is not None and not isinstance(area, str):
        raise ValueError("portAreaCode must be a string or null")
    area = area.strip() if area else None
    update_time = _utc_timestamp(detail.get("etaTimestamp"))
    eta = _utc_timestamp(detail.get("eta"))
    area_key = quote(area or "unspecified", safe="")
    event = MarineEtaEvent.model_validate(
        {
            "event_id": f"portnet:{call_id}:{area_key}:{update_time}",
            "port_call_id": f"portnet:{call_id}",
            "source_id": "portnet",
            "event_time_utc": update_time,
            "received_at_utc": received_at,
            "eta_utc": eta,
            "schema_version": "marine-v1",
        }
    ).model_dump()
    event["source_payload"] = PortnetEtaPayload.model_validate(
        {
            "port_call_id": call_id,
            "port_area_code": area,
            "eta_utc": eta,
            "eta_source": detail.get("etaSource"),
            "eta_timestamp_utc": update_time,
        }
    ).model_dump()
    return event


def _validate_call(call, received_at):
    if not isinstance(call, dict):
        return None, _rejection(
            "INVALID_PORTNET_CALL",
            call,
            received_at,
            "port call must be a JSON object",
        )
    call_id = call.get("portCallId")
    details = call.get("portAreaDetails")
    if (
        type(call_id) is not int
        or call_id <= 0
        or (details is not None and not isinstance(details, list))
    ):
        return None, _rejection(
            "INVALID_PORTNET_CALL",
            _safe_call(call),
            received_at,
            "positive portCallId and an array of portAreaDetails are required",
            call_id=call_id if type(call_id) is int else None,
        )
    return (call_id, details or []), None


def _active_eta_details(call_id, call, details, received_at):
    objects = []
    rejected = []
    for detail in details:
        if isinstance(detail, dict):
            objects.append(detail)
        else:
            rejected.append(
                _rejection(
                    "INVALID_PORTNET_ETA",
                    _safe_call(call),
                    received_at,
                    "port-area detail must be a JSON object",
                    call_id=call_id,
                )
            )

    eta_details = [item for item in objects if item.get("eta") is not None]
    active = [
        item for item in eta_details if not item.get("ata") and not item.get("atd")
    ]
    if eta_details and not active:
        return [], rejected, "completed"
    if not eta_details:
        return [], rejected, "without_eta"

    missing_area = [
        item
        for item in active
        if not isinstance(item.get("portAreaCode"), str)
        or not item.get("portAreaCode", "").strip()
    ]
    if len(active) > 1 and missing_area:
        for _ in missing_area:
            rejected.append(
                _rejection(
                    "AMBIGUOUS_PORT_AREA",
                    _safe_call(call),
                    received_at,
                    (
                        "portAreaCode is required when a call has "
                        "multiple active ETA details"
                    ),
                    call_id=call_id,
                )
            )
        active = [item for item in active if item not in missing_area]
    return active, rejected, "active"


def _normalize_eta_details(call_id, call, details, received_at):
    events = []
    rejected = []
    for detail in details:
        try:
            events.append(_make_event(call_id, detail, received_at))
        except (ValueError, ValidationError) as error:
            rejected.append(
                _rejection(
                    "INVALID_PORTNET_ETA",
                    _safe_call(call),
                    received_at,
                    str(error),
                    call_id=call_id,
                )
            )
    return events, rejected


def normalize_data(port_calls, received_at_utc):
    events = []
    rejected = []
    stats = Counter(
        portnet_call_records_received=len(port_calls),
        rejected_portnet_call_records=0,
        rejected_portnet_eta_details=0,
        completed_portnet_calls_skipped=0,
        portnet_calls_without_eta_skipped=0,
        active_portnet_calls=0,
        accepted_portnet_observations=0,
    )

    for call in port_calls:
        identity, error = _validate_call(call, received_at_utc)
        if error:
            rejected.append(error)
            stats["rejected_portnet_call_records"] += 1
            continue
        call_id, details = identity
        active, detail_rejections, status = _active_eta_details(
            call_id,
            call,
            details,
            received_at_utc,
        )
        rejected.extend(detail_rejections)
        stats["rejected_portnet_eta_details"] += len(detail_rejections)
        if status == "completed":
            stats["completed_portnet_calls_skipped"] += 1
            continue
        if status == "without_eta":
            stats["portnet_calls_without_eta_skipped"] += 1
            continue
        stats["active_portnet_calls"] += 1
        accepted, invalid = _normalize_eta_details(
            call_id,
            call,
            active,
            received_at_utc,
        )
        events.extend(accepted)
        rejected.extend(invalid)
        stats["accepted_portnet_observations"] += len(accepted)
        stats["rejected_portnet_eta_details"] += len(invalid)

    return events, rejected, dict(stats)


def store_observations(events, rejected, database_url, grouped_events=None):
    unique_events, duplicate_attempts, collisions = (
        grouped_events or ingest_fixture.group_events(events)
    )
    event_ids = sorted({event["event_id"] for event, _ in unique_events})
    duplicate_count = len(duplicate_attempts)
    inserted_count = 0

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            existing = existing_hashes(cursor, "portnet_eta_observation", event_ids)

            for item in rejected:
                cursor.execute(
                    """INSERT INTO portnet_eta_rejected_event
                       (event_id, port_call_id, received_at_utc, reason_code,
                        raw_payload, details)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        item.get("event_id"),
                        item.get("port_call_id"),
                        ingest_fixture.timestamp(item, "received_at_utc"),
                        item["reason_code"],
                        Jsonb(item["raw_payload"]),
                        Jsonb(item["details"]),
                    ),
                )

            for event, _ in unique_events:
                payload_hash = ingest_fixture.canonical_hash(event)
                identity = record_payload_hash(
                    existing, event["event_id"], payload_hash
                )
                if identity == "duplicate":
                    duplicate_count += 1
                    insert_quality_issue(
                        cursor,
                        "portnet_eta_quality_issue",
                        "EXACT_DUPLICATE",
                        event["event_id"],
                        event["port_call_id"],
                        ingest_fixture.timestamp(event, "received_at_utc"),
                        event,
                        {"payload_sha256": payload_hash},
                    )
                elif identity == "collision":
                    collisions[event["event_id"]] = sorted(existing[event["event_id"]])

                cursor.execute(
                    """INSERT INTO portnet_eta_observation
                       (event_id, port_call_id, port_area_code, event_time_utc,
                        received_at_utc, eta_utc, canonical_payload_sha256, raw_payload)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (event_id, canonical_payload_sha256) DO NOTHING""",
                    (
                        event["event_id"],
                        event["port_call_id"],
                        event["source_payload"]["port_area_code"],
                        ingest_fixture.timestamp(event, "event_time_utc"),
                        ingest_fixture.timestamp(event, "received_at_utc"),
                        ingest_fixture.timestamp(event, "eta_utc"),
                        payload_hash,
                        Jsonb(event),
                    ),
                )
                inserted_count += cursor.rowcount

            for event, payload_hash, _ in duplicate_attempts:
                insert_quality_issue(
                    cursor,
                    "portnet_eta_quality_issue",
                    "EXACT_DUPLICATE",
                    event["event_id"],
                    event["port_call_id"],
                    ingest_fixture.timestamp(event, "received_at_utc"),
                    event,
                    {"payload_sha256": payload_hash},
                )

            for event_id, hashes in collisions.items():
                event = next(
                    item for item, _ in unique_events if item["event_id"] == event_id
                )
                insert_quality_issue(
                    cursor,
                    "portnet_eta_quality_issue",
                    "ID_COLLISION",
                    event_id,
                    event["port_call_id"],
                    None,
                    None,
                    {"payload_hashes": hashes, "variant_count": len(hashes)},
                )

    return len(unique_events), duplicate_count, len(collisions), inserted_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date", required=True, type=date.fromisoformat, help="UTC date, YYYY-MM-DD"
    )
    parser.add_argument("--port", help="Optional port LOCODE, for example FIHEL")
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DATABASE_URL",
            ingest_fixture.DEFAULT_DATABASE_URL,
        ),
    )
    args = parser.parse_args()

    received_at = (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace(
            "+00:00",
            "Z",
        )
    )
    port_calls = fetch_port_calls(args.port, args.date)
    events, rejected, metrics = normalize_data(port_calls, received_at)
    grouped_events = ingest_fixture.group_events(events)
    _, duplicate_events, _ = grouped_events
    duplicate_calls = len(port_calls) - len(
        {json.dumps(call, sort_keys=True, separators=(",", ":")) for call in port_calls}
    )
    unique, duplicates, collisions, inserted = store_observations(
        events,
        rejected,
        args.database_url,
        grouped_events,
    )
    print(
        json.dumps(
            {
                "record_origin": "live_api",
                "source": "Portnet V1",
                "date": args.date.isoformat(),
                "port": args.port.upper() if args.port else "ALL",
                **metrics,
                "rejected_observations": len(rejected),
                "rejected_by_reason": dict(
                    sorted(Counter(item["reason_code"] for item in rejected).items())
                ),
                "duplicate_port_call_records_in_response": duplicate_calls,
                "unique_normalized_observations_in_response": unique,
                "duplicate_normalized_event_attempts_in_response": len(
                    duplicate_events
                ),
                "unchanged_observations_on_repeat_poll": duplicates
                - len(duplicate_events),
                "id_collision_groups": collisions,
                "new_observation_rows_inserted": inserted,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
