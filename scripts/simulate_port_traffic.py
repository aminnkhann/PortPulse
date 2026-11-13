"""Write a fixed sequence of simulated vessel changes to PostgreSQL."""

import argparse
import math
import os
import time
from datetime import datetime

import psycopg


EVENTS = (
    ("INSERT", "SIM-001", "VUOS", "2026-11-13T15:00:00Z", "2026-11-13T10:00:00Z"),
    ("UPDATE", "SIM-001", "VUOS", "2026-11-13T15:30:00Z", "2026-11-13T10:02:00Z"),
    ("UPDATE", "SIM-001", "VUOS", "2026-11-13T14:50:00Z", "2026-11-13T10:04:00Z"),
    ("INSERT", "SIM-002", "KATA", "2026-11-13T17:00:00Z", "2026-11-13T10:06:00Z"),
    ("DELETE", "SIM-002", "KATA", None, "2026-11-13T10:08:00Z"),
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario-id", default="demo-2026-11-13")
    parser.add_argument("--delay-seconds", type=float, default=2)
    args = parser.parse_args()
    if not args.scenario_id.strip():
        parser.error("--scenario-id must not be empty")
    if not math.isfinite(args.delay_seconds) or args.delay_seconds < 0:
        parser.error("--delay-seconds must be finite and zero or greater")
    return args


def run_scenario(database_url: str, scenario_id: str, delay_seconds: float) -> None:
    with psycopg.connect(database_url) as connection:
        for index, (action, call_id, area, eta, updated_at) in enumerate(EVENTS):
            update_time = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            if action == "INSERT":
                connection.execute(
                    """INSERT INTO stream_demo.port_eta_state
                       (scenario_id, port_call_id, port_area_code, eta_utc,
                        updated_at_utc)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (scenario_id, call_id, area, eta, update_time),
                )
            elif action == "UPDATE":
                result = connection.execute(
                    """UPDATE stream_demo.port_eta_state
                       SET eta_utc = %s, updated_at_utc = %s
                       WHERE scenario_id = %s AND port_call_id = %s
                         AND port_area_code = %s""",
                    (eta, update_time, scenario_id, call_id, area),
                )
                if result.rowcount != 1:
                    raise RuntimeError(
                        f"Expected one source row to update for {call_id}"
                    )
            else:
                result = connection.execute(
                    """DELETE FROM stream_demo.port_eta_state
                       WHERE scenario_id = %s AND port_call_id = %s
                         AND port_area_code = %s""",
                    (scenario_id, call_id, area),
                )
                if result.rowcount != 1:
                    raise RuntimeError(
                        f"Expected one source row to delete for {call_id}"
                    )
            connection.commit()
            print(f"{update_time.isoformat()} {action:6} {call_id} ETA={eta or '—'}")
            if delay_seconds and index < len(EVENTS) - 1:
                time.sleep(delay_seconds)


def main():
    args = parse_args()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("Set DATABASE_URL to the PortPulse PostgreSQL database")
    try:
        run_scenario(database_url, args.scenario_id, args.delay_seconds)
    except psycopg.errors.UniqueViolation as error:
        raise SystemExit(
            f"Scenario {args.scenario_id!r} already has source rows. Choose a new "
            "--scenario-id; reusing an ID would turn its INSERTs into collisions."
        ) from error
    except psycopg.Error as error:
        raise SystemExit(
            f"Could not write simulated port traffic: {error}. "
            "Check that migration 008 was applied and PostgreSQL is healthy."
        ) from error
    except RuntimeError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
