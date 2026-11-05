"""Fetch, normalize, and load Digitraffic Port Call V2 visits."""

import argparse
import gzip
import json
import os
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pydantic import ValidationError

from scripts import ingest_fixture
from scripts.event_model import DigitrafficEvent
from scripts.reconcile import reconcile_database


API_URL = "https://meri.digitraffic.fi/api/port-call/v2/visits"
USER_AGENT = "PortPulse/0.1"
VALID_STATUSES = {"Expected to Arrive", "Arrived", "Departed", "Cancelled"}


def fetch_visits(port_of_call=None, imo=None):
    params = {}
    if port_of_call:
        params["portOfCall"] = port_of_call
    if imo:
        params["imo"] = imo
    url = f"{API_URL}?{urlencode(params)}" if params else API_URL
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
            visits = json.loads(body)
    except (URLError, OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Digitraffic request failed: {error}") from error
    if not isinstance(visits, list):
        raise RuntimeError("Digitraffic visits response must be a JSON array")
    return visits


def normalize_visits(visits, received_at_utc):
    valid = []
    rejected = []
    for visit in visits:
        if not isinstance(visit, dict):
            rejected.append({
                "event_id": None,
                "port_call_id": None,
                "received_at_utc": received_at_utc,
                "reason_code": "INVALID_EVENT",
                "raw_payload": visit,
                "details": {"message": "visit must be a JSON object"},
            })
            continue

        visit_id = visit.get("visitId")
        event_time = visit.get("updateTime")
        has_identity = isinstance(visit_id, str) and isinstance(event_time, str)
        event_id = f"digitraffic:{visit_id}:{event_time}" if has_identity else None
        port_call_id = f"digitraffic:{visit_id}"
        event = {
            "event_id": event_id,
            "port_call_id": port_call_id,
            "source_id": "digitraffic",
            "event_time_utc": event_time,
            "received_at_utc": received_at_utc,
            "eta_utc": visit.get("eta"),
            "schema_version": "v2",
        }
        try:
            if not isinstance(visit_id, str) or not 1 <= len(visit_id) <= 35:
                raise ValueError("visitId must contain 1 to 35 characters")
            port_code = visit.get("portOfCall")
            if not isinstance(port_code, str) or len(port_code) != 5:
                raise ValueError("portOfCall must be a five-character LOCODE")
            status = visit.get("status")
            if not isinstance(status, str) or status not in VALID_STATUSES:
                raise ValueError("status must be a documented visit status")
            event = DigitrafficEvent.model_validate(event).model_dump()
        except (ValidationError, ValueError) as error:
            details = error.errors()[0] if isinstance(error, ValidationError) else {}
            rejected.append({
                "event_id": event_id,
                "port_call_id": port_call_id if isinstance(visit_id, str) else None,
                "received_at_utc": received_at_utc,
                "reason_code": "INVALID_TIMESTAMP" if details else "INVALID_EVENT",
                "raw_payload": visit,
                "details": {"message": str(error)},
            })
            continue
        event["source_payload"] = visit
        valid.append(event)
    return valid, rejected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", dest="port_of_call", help="Filter by port LOCODE")
    parser.add_argument("--imo", help="Filter by vessel IMO number")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ingest_fixture.DEFAULT_DATABASE_URL),
    )
    args = parser.parse_args()

    received_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    visits = fetch_visits(args.port_of_call, args.imo)
    valid, rejected = normalize_visits(visits, received_at)
    counts = ingest_fixture.store_fixture(
        valid,
        rejected,
        record_existing_duplicates=True,
        database_url=args.database_url,
    )
    call_ids = {event["port_call_id"] for event in valid}
    results = [
        result
        for result in reconcile_database(args.database_url, as_of_utc=received_at)
        if result["port_call_id"] in call_ids
    ] if valid else []
    print(json.dumps({
        "visits_received": len(visits),
        "unique_payloads_in_response": counts[0],
        "duplicate_attempts": counts[1],
        "collision_groups": counts[2],
        "rejected_visits": len(rejected),
        "reconciliation": results,
    }, indent=2))


if __name__ == "__main__":
    main()
