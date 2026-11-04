"""Generate the fixed synthetic PortPulse v1 scenario."""

import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "fixtures" / "port_call_v1.jsonl"
PORT_CALL_ID = "PC-DEMO-001"
DAY = "2026-01-15"


def stamp(hour: int, minute: int) -> str:
    value = datetime.fromisoformat(f"{DAY}T{hour:02}:{minute:02}:00+00:00")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def event(
    event_id: str,
    source_id: str,
    event_time: tuple[int, int],
    received_at: tuple[int, int],
    eta: tuple[int, int],
    schema_version: str = "v1",
) -> dict[str, str]:
    return {
        "event_id": event_id,
        "port_call_id": PORT_CALL_ID,
        "source_id": source_id,
        "event_time_utc": stamp(*event_time),
        "received_at_utc": stamp(*received_at),
        "eta_utc": stamp(*eta),
        "schema_version": schema_version,
    }


def build_events() -> list[dict[str, str]]:
    events = [
        event(
            f"CARRIER-BASE-{number:02}",
            "carrier_sim",
            (8, number - 1),
            (8, number + 4),
            (17, number - 1),
        )
        for number in range(1, 19)
    ]
    events.extend(
        [
            event("TERMINAL-STALE-01", "terminal_sim", (8, 30), (8, 35), (17, 0)),
            event("CARRIER-REV-01", "carrier_sim", (9, 0), (9, 5), (16, 0)),
            event("CARRIER-REV-02", "carrier_sim", (9, 30), (9, 31), (16, 30)),
        ]
    )
    events.append({**events[-1], "received_at_utc": stamp(9, 32)})
    events.append({**events[-1], "received_at_utc": stamp(9, 33)})
    events.extend(
        [
            event("CARRIER-LATE-01", "carrier_sim", (8, 50), (9, 40), (15, 50)),
            event("CARRIER-REV-03", "carrier_sim", (10, 0), (10, 2), (16, 25)),
            event("COORD-REV-01", "coordinator_sim", (10, 0), (10, 5), (15, 45)),
            event("COLLISION-01", "coordinator_sim", (10, 20), (10, 21), (15, 30)),
            event("COLLISION-01", "coordinator_sim", (10, 20), (10, 22), (16, 45)),
            event(
                "INVALID-VERSION-01", "carrier_sim", (10, 24), (10, 25), (16, 0), "v99"
            ),
            event("COORD-DELAYED-01", "coordinator_sim", (10, 15), (10, 55), (15, 40)),
            event("COORD-LATE-01", "coordinator_sim", (9, 50), (10, 58), (15, 50)),
        ]
    )

    events.sort(key=lambda item: item["received_at_utc"])
    return events


def main() -> None:
    events = build_events()
    assert len(events) == 31, f"expected 31 event occurrences, got {len(events)}"
    assert sum(item["event_id"] == "CARRIER-REV-02" for item in events) == 3
    assert sum(item["event_id"] == "COLLISION-01" for item in events) == 2
    assert all(
        events[index]["received_at_utc"] <= events[index + 1]["received_at_utc"]
        for index in range(len(events) - 1)
    )

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(
        "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in events),
        encoding="utf-8",
    )
    print(
        f"Wrote {len(events)} synthetic event occurrences to {OUTPUT.relative_to(ROOT)}"
    )


if __name__ == "__main__":
    main()
