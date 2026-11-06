"""Compare Portnet schedules with vessel-reported AIS ETAs for one UTC day."""

import argparse
import gzip
import json
import os
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pydantic import ValidationError

from scripts import ingest_fixture
from scripts.event_model import MarineEtaEvent
from scripts.reconcile import reconcile_database


PORTNET_URL = "https://meri.digitraffic.fi/api/port-call/v1/port-calls"
AIS_URL = "https://meri.digitraffic.fi/api/ais/v1/vessels"
USER_AGENT = "PortPulse/0.1"


def fetch_json(url):
    request = Request(url, headers={
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
        "Connection": "close",
        "Digitraffic-User": USER_AGENT,
    })
    try:
        with urlopen(request, timeout=20) as response:
            body = response.read()
            if response.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
            return json.loads(body)
    except (URLError, OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Digitraffic request failed: {error}") from error


def fetch_day(port, day):
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    port_filters = {"date": start.isoformat().replace("+00:00", "Z")}
    if port:
        port_filters["locode"] = port
    port_params = urlencode(port_filters)
    ais_params = urlencode({
        "from": int(start.timestamp() * 1000),
        "to": int(end.timestamp() * 1000),
    })
    calls = fetch_json(f"{PORTNET_URL}?{port_params}")
    ais = fetch_json(f"{AIS_URL}?{ais_params}")
    if not isinstance(calls, dict) or not isinstance(calls.get("portCalls"), list):
        raise RuntimeError("Portnet response must contain a portCalls array")
    if not isinstance(ais, list):
        raise RuntimeError("AIS vessel response must be a JSON array")
    return calls["portCalls"], ais


def decode_ais_eta(value, year):
    if type(value) is not int or value < 0:
        return None
    month = (value >> 16) & 0x0F
    day = (value >> 11) & 0x1F
    hour = (value >> 6) & 0x1F
    minute = value & 0x3F
    if not month or not day or hour == 24 or minute == 60:
        return None
    try:
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except ValueError:
        return None


def _rejection(reason, payload, received_at, message, event_id=None, call_id=None):
    return {
        "event_id": event_id,
        "port_call_id": call_id,
        "received_at_utc": received_at,
        "reason_code": reason,
        "raw_payload": payload,
        "details": {"message": message},
    }


def _safe_portnet_payload(call):
    if not isinstance(call, dict):
        return call
    details = call.get("portAreaDetails")
    return {
        key: call.get(key)
        for key in ("portCallId", "mmsi", "portToVisit", "portCallTimestamp")
    } | {
        "portAreaDetails": [
            {
                key: detail.get(key)
                for key in ("portAreaCode", "eta", "etaSource", "etaTimestamp")
            }
            for detail in details
            if isinstance(detail, dict)
        ] if isinstance(details, list) else details
    }


def _safe_ais_payload(record):
    if not isinstance(record, dict):
        return record
    return {key: record.get(key) for key in ("mmsi", "destination", "eta", "timestamp")}


def _validated_event(event, payload, received_at, rejected):
    try:
        event = MarineEtaEvent.model_validate(event).model_dump()
    except ValidationError as error:
        rejected.append(_rejection(
            "INVALID_EVENT", payload, received_at, error.errors()[0]["msg"],
            event["event_id"], event["port_call_id"],
        ))
        return None
    event["source_payload"] = payload
    return event


def _portnet_identity(call, received_at, rejected):
    if not isinstance(call, dict):
        rejected.append(_rejection(
            "INVALID_PORTNET_EVENT", call, received_at,
            "port call must be a JSON object",
        ))
        return None
    call_id = call.get("portCallId")
    if type(call_id) is not int or call_id <= 0:
        rejected.append(_rejection(
            "INVALID_PORTNET_EVENT", _safe_portnet_payload(call), received_at,
            "a positive integer portCallId is required",
        ))
        return None
    mmsi = call.get("mmsi")
    if type(mmsi) is not int or mmsi <= 0:
        mmsi = None
    port = call.get("portToVisit")
    if not isinstance(port, str) or len(port) != 5:
        port = None
    details = call.get("portAreaDetails")
    if details is None:
        details = []
    if not isinstance(details, list):
        rejected.append(_rejection(
            "INVALID_PORTNET_EVENT", _safe_portnet_payload(call), received_at,
            "portAreaDetails must be an array", call_id=str(call_id),
        ))
        return None
    return call_id, mmsi, port, details


def _select_active_eta(details):
    eta_details = [
        item for item in details
        if isinstance(item, dict) and item.get("eta")
    ]
    active_details = [
        item for item in eta_details
        if not item.get("ata") and not item.get("atd")
    ]
    if eta_details and not active_details:
        return None, "completed"
    if len(active_details) != 1:
        return None, "ambiguous"
    return active_details[0], None


def _portnet_eta_event(call, identity, detail, received_at, rejected):
    call_id, mmsi, port, _ = identity
    eta_time = detail.get("etaTimestamp")
    area = detail.get("portAreaCode")
    source = detail.get("etaSource")
    if eta_time is None:
        return None, "missing_update_time"
    try:
        if not isinstance(eta_time, str) or not eta_time:
            raise ValueError("ETA update timestamp is required")
        event = {
            "event_id": f"portnet:{call_id}:{eta_time}",
            "port_call_id": f"maritime:{call_id}",
            "source_id": "portnet",
            "event_time_utc": eta_time,
            "received_at_utc": received_at,
            "eta_utc": detail["eta"],
            "schema_version": "marine-v1",
        }
        audit = {
            "port_call_id": call_id,
            "mmsi": mmsi,
            "port": port,
            "port_area": area,
            "eta": detail["eta"],
            "eta_source": source,
            "eta_timestamp": eta_time,
        }
    except (KeyError, TypeError, ValueError) as error:
        rejected.append(_rejection(
            "INVALID_PORTNET_ETA", _safe_portnet_payload(call), received_at,
            str(error), call_id=str(call_id),
        ))
        return None, None
    normalized = _validated_event(event, audit, received_at, rejected)
    if normalized is None:
        return None, None
    eta = datetime.fromisoformat(normalized["eta_utc"].replace("Z", "+00:00"))
    match_key = (mmsi, port.upper(), eta.month, eta.day)
    return (match_key, normalized), None


def _ais_matches(record, portnet_by_key):
    mmsi = record["mmsi"]
    destination = record["destination"].strip().upper()
    eta_matches = []
    for key, values in portnet_by_key.items():
        if key[0] != mmsi or key[1] != destination:
            continue
        eta = decode_ais_eta(record.get("eta"), values[0][1].year)
        if eta is not None and (eta.month, eta.day) == (key[2], key[3]):
            eta_matches.extend((event, eta) for event, _ in values)
    return eta_matches


def _normalize_ais_record(record, portnet_by_key, received_at, rejected):
    if not isinstance(record, dict):
        rejected.append(_rejection(
            "INVALID_AIS_EVENT", record, received_at,
            "AIS vessel record must be a JSON object",
        ))
        return None, None, False
    mmsi = record.get("mmsi")
    destination = record.get("destination")
    stamp = record.get("timestamp")
    if (
        type(mmsi) is not int
        or mmsi <= 0
        or not isinstance(destination, str)
        or type(stamp) is not int
        or type(record.get("eta")) is not int
    ):
        rejected.append(_rejection(
            "INVALID_AIS_EVENT", _safe_ais_payload(record), received_at,
            "MMSI, destination, and epoch-millisecond timestamp are required",
        ))
        return None, None, False
    try:
        event_time = datetime.fromtimestamp(stamp / 1000, timezone.utc)
    except (OverflowError, OSError, ValueError):
        rejected.append(_rejection(
            "INVALID_AIS_TIMESTAMP", _safe_ais_payload(record), received_at,
            "AIS timestamp must be epoch milliseconds",
        ))
        return None, None, False
    matches = _ais_matches(record, portnet_by_key)
    if len(matches) != 1:
        return None, None, True

    portnet_event, ais_eta = matches[0]
    call_id = portnet_event["port_call_id"]
    audit = {
        "mmsi": mmsi,
        "destination": destination.strip().upper(),
        "eta_raw": record.get("eta"),
        "eta": ais_eta.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "metadata_timestamp": stamp,
        "matched_port_call_id": call_id,
    }
    event = {
        "event_id": f"ais:{mmsi}:{stamp}",
        "port_call_id": call_id,
        "source_id": "ais",
        "event_time_utc": event_time.isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z"),
        "received_at_utc": received_at,
        "eta_utc": audit["eta"],
        "schema_version": "marine-v1",
    }
    return _validated_event(event, audit, received_at, rejected), portnet_event, False


def _add_portnet_call(call, received_at, rejected, portnet_by_key, stats):
    identity = _portnet_identity(call, received_at, rejected)
    if identity is None:
        return
    if identity[1] is None or identity[2] is None:
        stats["unmatchable_portnet"] += 1
        return
    detail, reason = _select_active_eta(identity[3])
    if reason == "completed":
        stats["completed_portnet"] += 1
        return
    if reason:
        stats["skipped_portnet"] += 1
        return
    result, reason = _portnet_eta_event(
        call, identity, detail, received_at, rejected,
    )
    if reason == "missing_update_time":
        stats["missing_portnet_eta_timestamp"] += 1
        return
    if result is not None:
        key, event = result
        eta = datetime.fromisoformat(event["eta_utc"].replace("Z", "+00:00"))
        portnet_by_key.setdefault(key, []).append((event, eta))


def normalize_data(port_calls, ais_records, received_at_utc):
    events = []
    rejected = []
    portnet_by_key = {}
    stats = Counter()

    for call in port_calls:
        _add_portnet_call(
            call, received_at_utc, rejected, portnet_by_key, stats,
        )

    matched_portnet_ids = set()
    for record in ais_records:
        event, portnet_event, unmatched = _normalize_ais_record(
            record, portnet_by_key, received_at_utc, rejected,
        )
        if unmatched:
            stats["unmatched_ais"] += 1
            continue
        if event is None:
            continue
        if portnet_event["event_id"] not in matched_portnet_ids:
            events.append(portnet_event)
            matched_portnet_ids.add(portnet_event["event_id"])
        events.append(event)
        stats["matched_ais"] += 1
    return events, rejected, {
        "matched_ais": stats["matched_ais"],
        "unmatched_ais": stats["unmatched_ais"],
        "skipped_portnet": stats["skipped_portnet"],
        "completed_portnet": stats["completed_portnet"],
        "unmatchable_portnet": stats["unmatchable_portnet"],
        "missing_portnet_eta_timestamp": stats["missing_portnet_eta_timestamp"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", help="Optional port LOCODE filter, for example FIHEL")
    parser.add_argument(
        "--date", required=True, type=date.fromisoformat,
        help="UTC date, YYYY-MM-DD",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ingest_fixture.DEFAULT_DATABASE_URL),
    )
    args = parser.parse_args()

    received_at = datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")
    port_calls, ais_records = fetch_day(args.port, args.date)
    events, rejected, match_counts = normalize_data(
        port_calls, ais_records, received_at,
    )
    counts = ingest_fixture.store_fixture(
        events, rejected, record_existing_duplicates=True,
        database_url=args.database_url,
    )
    call_ids = {event["port_call_id"] for event in events}
    results = [
        result for result in reconcile_database(
            args.database_url, as_of_utc=received_at,
        )
        if result["port_call_id"] in call_ids
    ] if events else []
    event_by_id = {event["event_id"]: event for event in events}
    comparisons = [
        {
            "port_call_id": result["port_call_id"],
            "candidates": [
                {
                    "source_id": state["source_id"],
                    "reported_by": (
                        event_by_id.get(state["event_id"], {})
                        .get("source_payload", {})
                        .get("eta_source")
                        or ("AIS vessel" if state["source_id"] == "ais" else "Portnet")
                    ),
                    "status": state["status"],
                    "eta_utc": state.get("eta_utc"),
                }
                for state in result["source_states"]
            ],
            "winner": result["trusted_state"]["source_id"],
            "winner_eta_utc": result["trusted_state"]["eta_utc"],
            "issues": sorted({issue["code"] for issue in result["issues"]}),
        }
        for result in results
        if len(result["source_states"]) > 1
    ]
    print(json.dumps({
        "port": args.port.upper() if args.port else "ALL",
        "date": args.date.isoformat(),
        "port_calls_received": len(port_calls),
        "ais_records_received": len(ais_records),
        "ais_matches": match_counts["matched_ais"],
        "ais_unmatched_or_ambiguous": match_counts["unmatched_ais"],
        "portnet_without_one_eta_area": match_counts["skipped_portnet"],
        "portnet_with_actual_arrival_departure": match_counts["completed_portnet"],
        "portnet_unmatchable_without_vessel_or_port": match_counts[
            "unmatchable_portnet"
        ],
        "portnet_without_eta_update_time": match_counts[
            "missing_portnet_eta_timestamp"
        ],
        "comparable_calls": len(comparisons),
        "conflicting_calls": sum("CONFLICT" in item["issues"] for item in comparisons),
        "unique_payloads": counts[0],
        "duplicate_attempts": counts[1],
        "collision_groups": counts[2],
        "rejected_records": len(rejected),
        "rejected_by_reason": dict(sorted(Counter(
            item["reason_code"] for item in rejected
        ).items())),
        "comparisons": comparisons,
    }, indent=2))


if __name__ == "__main__":
    main()
