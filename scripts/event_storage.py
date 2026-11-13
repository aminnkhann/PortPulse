"""Shared PostgreSQL helpers for event identity and quality issues."""

from collections import defaultdict

from psycopg import sql
from psycopg.types.json import Jsonb


def existing_hashes(cursor, table_name, event_ids):
    hashes = defaultdict(set)
    if event_ids:
        cursor.execute(
            sql.SQL(
                "SELECT event_id, canonical_payload_sha256 FROM {} "
                "WHERE event_id = ANY(%s)"
            ).format(sql.Identifier(table_name)),
            (event_ids,),
        )
        for event_id, payload_hash in cursor.fetchall():
            hashes[event_id].add(payload_hash)
    return hashes


def insert_quality_issue(
    cursor,
    table_name,
    issue_code,
    event_id,
    port_call_id,
    received_at,
    raw_payload,
    details,
    deduplicate=False,
):
    columns = sql.SQL(
        "(issue_code, event_id, port_call_id, received_at_utc, raw_payload, details)"
    )
    if deduplicate:
        query = sql.SQL(
            "INSERT INTO {} {} SELECT %(issue_code)s, %(event_id)s, "
            "%(port_call_id)s, %(received_at)s, %(raw_payload)s, %(details)s "
            "WHERE NOT EXISTS (SELECT 1 FROM {} WHERE issue_code = %(issue_code)s "
            "AND event_id IS NOT DISTINCT FROM %(event_id)s "
            "AND received_at_utc IS NOT DISTINCT FROM %(received_at)s "
            "AND details = %(details)s)"
        ).format(sql.Identifier(table_name), columns, sql.Identifier(table_name))
    else:
        query = sql.SQL(
            "INSERT INTO {} {} VALUES (%(issue_code)s, %(event_id)s, "
            "%(port_call_id)s, %(received_at)s, %(raw_payload)s, %(details)s)"
        ).format(sql.Identifier(table_name), columns)
    cursor.execute(
        query,
        {
            "issue_code": issue_code,
            "event_id": event_id,
            "port_call_id": port_call_id,
            "received_at": received_at,
            "raw_payload": Jsonb(raw_payload) if raw_payload is not None else None,
            "details": Jsonb(details),
        },
    )
