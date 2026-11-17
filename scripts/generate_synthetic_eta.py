"""Generate a reproducible, fictional 30-day maritime ETA dataset."""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.event_model import PortCallEvent


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "fixtures" / "synthetic_eta_v1.jsonl"
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
SOURCES = ("carrier_sim", "coordinator_sim", "terminal_sim")


def utc_text(value):
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def build_dataset(port_calls=50, seed=42):
    if not 1 <= port_calls <= 1000:
        raise ValueError("port_calls must be between 1 and 1000")

    randomizer = random.Random(seed)
    events = []
    eta_updates = 0
    revisions = 0
    delayed_events = 0
    late_arrivals = 0
    duplicates = 0
    invalid_events = 0

    for call_number in range(1, port_calls + 1):
        call_id = f"SYNTH-{seed}-PC-{call_number:04}"
        update_count = randomizer.randint(2, 5)
        call_start = START + timedelta(days=randomizer.randint(0, 24))
        event_times = [call_start]
        for _ in range(1, update_count):
            event_times.append(
                event_times[-1] + timedelta(hours=randomizer.randint(6, 24))
            )

        eta = call_start + timedelta(hours=randomizer.randint(24, 72))
        call_events = []
        for update_number, event_time in enumerate(event_times, start=1):
            if update_number > 1:
                revisions += 1
                eta += timedelta(
                    minutes=randomizer.choice((-1, 1)) * randomizer.randint(30, 360)
                )
            received_at = event_time + timedelta(minutes=randomizer.randint(2, 20))
            if call_number % 8 == 0 and update_number == update_count:
                delay_minutes = randomizer.randint(45, 150)
                received_at = event_time + timedelta(minutes=delay_minutes)
                delayed_events += 1

            event = {
                "event_id": f"SYNTH-{seed}-{call_number:04}-{update_number:02}",
                "port_call_id": call_id,
                "source_id": randomizer.choice(SOURCES),
                "event_time_utc": utc_text(event_time),
                "received_at_utc": utc_text(received_at),
                "eta_utc": utc_text(eta),
                "schema_version": "v1",
            }
            call_events.append(event)
            eta_updates += 1

        if call_number % 10 == 0:
            late_event = call_events[0]
            later_receive = datetime.fromisoformat(
                call_events[-1]["received_at_utc"].replace("Z", "+00:00")
            ) + timedelta(minutes=5)
            late_event["received_at_utc"] = utc_text(later_receive)
            late_arrivals += 1

        events.extend(call_events)

        if call_number % 7 == 0:
            duplicate = call_events[-1].copy()
            duplicate["received_at_utc"] = utc_text(
                datetime.fromisoformat(
                    duplicate["received_at_utc"].replace("Z", "+00:00")
                )
                + timedelta(minutes=5)
            )
            events.append(duplicate)
            duplicates += 1

        if call_number % 11 == 0:
            invalid = call_events[0].copy()
            invalid["event_id"] = f"SYNTH-{seed}-{call_number:04}-INVALID"
            invalid["schema_version"] = "v99"
            events.append(invalid)
            invalid_events += 1

    events.sort(key=lambda event: (event["received_at_utc"], event["event_id"]))
    summary = {
        "port_calls": port_calls,
        "eta_updates": eta_updates,
        "event_occurrences": len(events),
        "scenario_categories": {
            "eta_revisions": revisions,
            "delayed_events": delayed_events,
            "late_arrivals": late_arrivals,
            "duplicate_deliveries": duplicates,
            "invalid_events": invalid_events,
        },
    }
    return events, summary


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port-calls", type=int, default=50, choices=range(1, 1001))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    return parser.parse_args()


def main():
    args = parse_args()
    events, summary = build_dataset(args.port_calls, args.seed)
    for event in events:
        if event["schema_version"] == "v1":
            PortCallEvent.model_validate(event)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        "".join(json.dumps(event, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )
    print(f"Wrote {summary['event_occurrences']} event occurrences to {args.output}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
