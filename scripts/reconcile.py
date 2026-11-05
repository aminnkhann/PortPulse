"""Build and save the deterministic trusted ETA for a port call."""

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
import yaml

from scripts import ingest_fixture


ROOT = Path(__file__).resolve().parents[1]
RULES_FILE = ROOT / "rules" / "eta_rules_v1.yaml"
RULE_DESCRIPTION = (
    "highest-priority non-stale source; latest eligible event_time within source"
)
SAVED_ISSUE_CODES = {
    "CONFLICT",
    "DELAYED_DELIVERY",
    "OUT_OF_ORDER",
    "STALE_SOURCE",
}


def parse_utc(value):
    if value is None:
        raise ValueError("timestamp is required")
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC and include a timezone")
    return value.astimezone(timezone.utc)


def format_utc(value):
    return parse_utc(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_rules(path=RULES_FILE):
    rules = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(rules, dict):
        raise ValueError("rules file must contain a mapping")
    if not isinstance(rules.get("sources"), dict) or not rules["sources"]:
        raise ValueError("rules file must define at least one source")
    for name, source in rules["sources"].items():
        if (
            not isinstance(name, str)
            or not isinstance(source, dict)
            or type(source.get("priority")) is not int
            or source["priority"] <= 0
            or type(source.get("freshness_minutes")) is not int
            or source["freshness_minutes"] <= 0
        ):
            raise ValueError(f"invalid source settings for {name!r}")
    thresholds = rules.get("quality_thresholds", {})
    for name in ("conflict_minutes", "delayed_delivery_minutes"):
        if type(thresholds.get(name)) is not int or thresholds[name] < 0:
            raise ValueError(f"invalid quality threshold: {name}")
    if not isinstance(rules.get("rules_version"), str) or not rules["rules_version"]:
        raise ValueError("rules_version must be a non-empty string")
    rules["as_of"] = parse_utc(rules.get("as_of_utc"))
    return rules


def group_events(events, port_call_id):
    groups = defaultdict(lambda: defaultdict(list))
    for event in events:
        if event.get("port_call_id") == port_call_id:
            groups[event["event_id"]][ingest_fixture.canonical_hash(event)].append(event)

    unique_events = []
    duplicate_counts = {}
    collisions = {}
    for event_id, variants in sorted(groups.items()):
        if len(variants) > 1:
            collisions[event_id] = len(variants)
        for attempts in variants.values():
            attempts.sort(
                key=lambda event: (
                    parse_utc(event["received_at_utc"]),
                    json.dumps(event, sort_keys=True),
                )
            )
            unique_events.append(attempts[0])
            if len(attempts) > 1:
                duplicate_counts[event_id] = (
                    duplicate_counts.get(event_id, 0) + len(attempts) - 1
                )
    return unique_events, duplicate_counts, collisions


def latest_event(events):
    newest_time = max(parse_utc(event["event_time_utc"]) for event in events)
    newest = [
        event for event in events
        if parse_utc(event["event_time_utc"]) == newest_time
    ]
    newest_received = max(parse_utc(event["received_at_utc"]) for event in newest)
    newest = [
        event for event in newest
        if parse_utc(event["received_at_utc"]) == newest_received
    ]
    return min(newest, key=lambda event: event["event_id"])


def build_source_states(events, sources, as_of):
    by_source = defaultdict(list)
    for event in events:
        by_source[event["source_id"]].append(event)

    states = []
    winners = {}
    for source_id, source in sorted(sources.items()):
        source_events = by_source[source_id]
        if not source_events:
            continue
        event = latest_event(source_events)
        source_payload = event.get("source_payload")
        visit_status = (
            source_payload.get("status")
            if isinstance(source_payload, dict)
            else "Expected to Arrive"
        )
        if visit_status != "Expected to Arrive":
            states.append({
                "source_id": source_id,
                "status": "INELIGIBLE",
                "event_id": event["event_id"],
                "visit_status": visit_status,
            })
            continue
        received_at = parse_utc(event["received_at_utc"])
        age_minutes = int((as_of - received_at).total_seconds() // 60)
        status = "STALE" if age_minutes > source["freshness_minutes"] else "ACTIVE"
        states.append({
            "source_id": source_id,
            "status": status,
            "event_id": event["event_id"],
            "eta_utc": format_utc(event["eta_utc"]),
            "last_received_at_utc": format_utc(received_at),
            "age_minutes": age_minutes,
        })
        winners[source_id] = event
    return states, winners


def out_of_order_issues(events):
    by_source = defaultdict(list)
    for event in events:
        by_source[event["source_id"]].append(event)

    issues = []
    for source_id, source_events in by_source.items():
        source_events.sort(
            key=lambda event: (parse_utc(event["received_at_utc"]), event["event_id"])
        )
        newest_time = None
        newest_event = None
        for event in source_events:
            event_time = parse_utc(event["event_time_utc"])
            if newest_time is not None and event_time < newest_time:
                issues.append({
                    "code": "OUT_OF_ORDER",
                    "event_id": event["event_id"],
                    "source_id": source_id,
                    "newer_event_id": newest_event["event_id"],
                })
            elif newest_time is None or event_time > newest_time:
                newest_time = event_time
                newest_event = event
    return issues


def identity_issues(rejected, duplicate_counts, collisions, port_call_id):
    issues = []
    for event_id, count in duplicate_counts.items():
        issues.append({
            "code": "EXACT_DUPLICATE",
            "event_id": event_id,
            "duplicate_occurrences": count,
        })
    for event_id, count in collisions.items():
        issues.append({
            "code": "ID_COLLISION",
            "event_id": event_id,
            "variant_count": count,
            "action": "exclude_all_variants",
        })
    for item in rejected:
        if item.get("port_call_id") not in (None, port_call_id):
            continue
        issue = {"code": item["reason_code"], "event_id": item.get("event_id")}
        if item["reason_code"] == "INVALID_SCHEMA_VERSION":
            issue.update({
                "schema_version": item["raw_payload"].get("schema_version"),
                "expected": "v1",
            })
        issues.append(issue)
    return issues


def delivery_issues(candidates, threshold):
    issues = []
    for event in candidates:
        received_at = parse_utc(event["received_at_utc"])
        event_time = parse_utc(event["event_time_utc"])
        delay = int((received_at - event_time).total_seconds() // 60)
        if delay > threshold:
            issues.append({
                "code": "DELAYED_DELIVERY",
                "event_id": event["event_id"],
                "source_id": event["source_id"],
                "delay_minutes": delay,
                "threshold_minutes": threshold,
            })
    return issues


def stale_source_issues(source_states, sources):
    issues = []
    for state in source_states:
        if state["status"] == "STALE":
            issues.append({
                "code": "STALE_SOURCE",
                "source_id": state["source_id"],
                "last_received_at_utc": state["last_received_at_utc"],
                "age_minutes": state["age_minutes"],
                "threshold_minutes": sources[state["source_id"]][
                    "freshness_minutes"
                ],
            })
    return issues


def conflict_issues(source_states, winners, threshold, port_call_id):
    issues = []
    active = [state for state in source_states if state["status"] == "ACTIVE"]
    for index, left in enumerate(active):
        for right in active[index + 1:]:
            difference = abs(
                parse_utc(winners[left["source_id"]]["eta_utc"])
                - parse_utc(winners[right["source_id"]]["eta_utc"])
            )
            difference_minutes = int(difference.total_seconds() // 60)
            if difference_minutes > threshold:
                pair = sorted((left["source_id"], right["source_id"]))
                issues.append({
                    "code": "CONFLICT",
                    "port_call_id": port_call_id,
                    "sources": pair,
                    "eta_values_utc": [
                        format_utc(winners[source_id]["eta_utc"]) for source_id in pair
                    ],
                    "difference_minutes": difference_minutes,
                    "threshold_minutes": threshold,
                })
    return issues


def build_issues(rejected, duplicate_counts, collisions, candidates,
                 source_states, winners, rules, port_call_id):
    issues = identity_issues(rejected, duplicate_counts, collisions, port_call_id)
    issues.extend(delivery_issues(
        candidates, rules["quality_thresholds"]["delayed_delivery_minutes"]
    ))
    issues.extend(out_of_order_issues(candidates))
    issues.extend(stale_source_issues(source_states, rules["sources"]))
    issues.extend(conflict_issues(
        source_states, winners, rules["quality_thresholds"]["conflict_minutes"],
        port_call_id,
    ))
    return sorted(
        issues,
        key=lambda issue: (
            issue["code"], issue.get("event_id", ""), issue.get("source_id", "")
        ),
    )


def reconcile_port_call(events, rejected, sources, rules, port_call_id):
    events = [event for event in events if event.get("port_call_id") == port_call_id]
    rejected = [
        item for item in rejected
        if item.get("port_call_id") == port_call_id
    ]
    as_of = rules["as_of"]
    unique_events, duplicate_counts, collisions = group_events(events, port_call_id)
    candidates = [
        event for event in unique_events
        if event["event_id"] not in collisions
        and parse_utc(event["received_at_utc"]) <= as_of
    ]
    source_states, winners = build_source_states(candidates, sources, as_of)
    active_states = [state for state in source_states if state["status"] == "ACTIVE"]

    winning_state = None
    if active_states:
        highest_priority = max(
            sources[state["source_id"]]["priority"] for state in active_states
        )
        highest = [
            state for state in active_states
            if sources[state["source_id"]]["priority"] == highest_priority
        ]
        winning_state = min(
            highest,
            key=lambda state: (
                -parse_utc(winners[state["source_id"]]["event_time_utc"]).timestamp(),
                state["event_id"],
            ),
        )
        trusted_state = {
            "status": "TRUSTED",
            "eta_utc": format_utc(winners[winning_state["source_id"]]["eta_utc"]),
            "source_id": winning_state["source_id"],
            "event_id": winning_state["event_id"],
            "source_priority": highest_priority,
            "applied_rule": RULE_DESCRIPTION,
        }
    else:
        trusted_state = {
            "status": (
                "UNAVAILABLE"
                if any(state["status"] == "INELIGIBLE" for state in source_states)
                else "UNAVAILABLE_STALE"
            ),
            "eta_utc": None,
            "source_id": None,
            "event_id": None,
            "source_priority": None,
            "applied_rule": RULE_DESCRIPTION,
        }

    issues = build_issues(
        rejected, duplicate_counts, collisions, candidates, source_states,
        winners, rules, port_call_id,
    )
    unique_count = len(unique_events)
    collision_variants = sum(collisions.values())
    after_cutoff = sum(
        1 for event in unique_events
        if event["event_id"] not in collisions
        and parse_utc(event["received_at_utc"]) > as_of
    )
    return {
        "rules_version": rules["rules_version"],
        "as_of_utc": format_utc(as_of),
        "port_call_id": port_call_id,
        "trusted_state": trusted_state,
        "source_states": source_states,
        "counts": {
            "message_occurrences": len(events) + len(rejected),
            "valid_occurrences": len(events),
            "exact_duplicate_occurrences": sum(duplicate_counts.values()),
            "unique_valid_payloads_before_collision_filter": unique_count,
            "collision_groups": len(collisions),
            "collision_variants": collision_variants,
            "rejected_occurrences": len(rejected),
            "eligible_unique_events_after_filtering": len(candidates),
            "events_after_as_of_utc": after_cutoff,
        },
        "issues": issues,
    }


def database_inputs(cursor):
    cursor.execute(
        "SELECT raw_payload FROM raw_event "
        "ORDER BY port_call_id, received_at_utc, event_id"
    )
    events = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        """SELECT raw_payload FROM quality_issue
           WHERE issue_code = 'EXACT_DUPLICATE' AND raw_payload IS NOT NULL"""
    )
    events.extend(row[0] for row in cursor.fetchall())
    cursor.execute(
        "SELECT event_id, port_call_id, reason_code, raw_payload FROM rejected_event"
    )
    rejected = [
        {
            "event_id": event_id,
            "port_call_id": port_call_id,
            "reason_code": reason_code,
            "raw_payload": raw_payload,
        }
        for event_id, port_call_id, reason_code, raw_payload in cursor.fetchall()
    ]
    cursor.execute("SELECT source_id, priority, freshness_minutes FROM source_system")
    sources = {
        source_id: {"priority": priority, "freshness_minutes": freshness}
        for source_id, priority, freshness in cursor.fetchall()
    }
    return events, rejected, sources


def save_result(cursor, result, events):
    state = result["trusted_state"]
    cursor.execute(
        """INSERT INTO port_call_state
               (port_call_id, status, trusted_eta_utc, source_id, winning_event_id,
                conflict_flag, as_of_utc, rules_version)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (port_call_id) DO UPDATE SET
               status = EXCLUDED.status,
               trusted_eta_utc = EXCLUDED.trusted_eta_utc,
               source_id = EXCLUDED.source_id,
               winning_event_id = EXCLUDED.winning_event_id,
               conflict_flag = EXCLUDED.conflict_flag,
               as_of_utc = EXCLUDED.as_of_utc,
               rules_version = EXCLUDED.rules_version""",
        (
            result["port_call_id"], state["status"],
            parse_utc(state["eta_utc"]) if state["eta_utc"] else None,
            state["source_id"], state["event_id"],
            any(issue["code"] == "CONFLICT" for issue in result["issues"]),
            parse_utc(result["as_of_utc"]), result["rules_version"],
        ),
    )

    event_by_id = {
        event["event_id"]: event
        for event in events
        if event.get("port_call_id") == result["port_call_id"]
    }
    for issue in result["issues"]:
        if issue["code"] not in SAVED_ISSUE_CODES:
            continue
        event_id = issue.get("event_id")
        event = event_by_id.get(event_id)
        details = {
            key: value for key, value in issue.items()
            if key not in {"code", "event_id"}
        }
        details["port_call_id"] = result["port_call_id"]
        received_at = parse_utc(event["received_at_utc"]) if event else None
        ingest_fixture.insert_issue(
            cursor, issue["code"], event_id, received_at, event, details
        )


def reconcile_database(database_url, rules_path=RULES_FILE, as_of_utc=None):
    rules = load_rules(rules_path)
    if as_of_utc is not None:
        rules["as_of"] = parse_utc(as_of_utc)
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            events, rejected, sources = database_inputs(cursor)
            if not events:
                raise ValueError("raw_event is empty; ingest the fixture first")
            if sources != rules["sources"]:
                raise ValueError("database sources do not match the versioned rules")
            port_call_ids = sorted({event["port_call_id"] for event in events})
            results = []
            for port_call_id in port_call_ids:
                result = reconcile_port_call(
                    events, rejected, rules["sources"], rules, port_call_id
                )
                save_result(cursor, result, events)
                results.append(result)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ingest_fixture.DEFAULT_DATABASE_URL),
    )
    parser.add_argument(
        "--as-of",
        help="Optional ISO-8601 UTC evaluation time; defaults to the rules file",
    )
    args = parser.parse_args()
    results = reconcile_database(args.database_url, as_of_utc=args.as_of)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
