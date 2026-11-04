"""Print a port call's trusted state, event history, and recorded issues."""

import argparse
import json
import os

import psycopg
from psycopg.rows import dict_row

from scripts import ingest_fixture


def query_port_call(database_url, port_call_id):
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        state = connection.execute(
            """SELECT port_call_id, status, trusted_eta_utc, source_id,
                      winning_event_id, conflict_flag, as_of_utc, rules_version
               FROM port_call_state WHERE port_call_id = %s""",
            (port_call_id,),
        ).fetchone()
        if state is None:
            raise ValueError(f"no reconciled state for port_call_id {port_call_id!r}")

        history = connection.execute(
            """SELECT event_id, source_id, event_time_utc, received_at_utc,
                      ingested_at_utc, eta_utc, schema_version
               FROM raw_event WHERE port_call_id = %s
               ORDER BY event_time_utc, event_id, raw_event_id""",
            (port_call_id,),
        ).fetchall()
        issues = connection.execute(
            """SELECT issue_code, event_id, received_at_utc, details
               FROM quality_issue WHERE port_call_id = %s
               ORDER BY issue_code, event_id, received_at_utc, quality_issue_id""",
            (port_call_id,),
        ).fetchall()
        rejected = connection.execute(
            """SELECT event_id, reason_code, received_at_utc, raw_payload, details
               FROM rejected_event WHERE port_call_id = %s
               ORDER BY reason_code, event_id, rejected_event_id""",
            (port_call_id,),
        ).fetchall()

    return {
        "state": state,
        "event_history": history,
        "quality_issues": issues,
        "rejected_events": rejected,
    }


def json_default(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(f"cannot encode {type(value).__name__}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("port_call_id")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ingest_fixture.DEFAULT_DATABASE_URL),
    )
    args = parser.parse_args()
    result = query_port_call(args.database_url, args.port_call_id)
    print(json.dumps(result, indent=2, default=json_default))


if __name__ == "__main__":
    main()
