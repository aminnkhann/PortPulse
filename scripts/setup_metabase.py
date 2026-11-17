"""Create the local Metabase analytics connection and PortPulse dashboards."""

import json
import os
import re
import urllib.error
import urllib.request

import psycopg
from psycopg import sql


BASE_URL = os.getenv("METABASE_URL", "http://metabase:3000").rstrip("/")
ROLE = "metabase_reader"
COLLECTION = "PortPulse"
DASHBOARDS = {
    "PortPulse — Portnet Overview": {
        "Accepted observations": (
            "scalar",
            "SELECT accepted_observations FROM analytics.mart_pipeline_quality",
            (0, 0, 5, 4),
        ),
        "Latest ETA records": (
            "scalar",
            "SELECT count(*) AS latest_eta_records "
            "FROM analytics.mart_portnet_latest_eta",
            (0, 5, 5, 4),
        ),
        "Rejected observations": (
            "scalar",
            "SELECT rejected_observations FROM analytics.mart_pipeline_quality",
            (0, 10, 5, 4),
        ),
        "Duplicate issues": (
            "scalar",
            "SELECT duplicate_issue_rows FROM analytics.mart_pipeline_quality",
            (0, 15, 5, 4),
        ),
        "Collision issues": (
            "scalar",
            "SELECT collision_issue_rows FROM analytics.mart_pipeline_quality",
            (0, 20, 4, 4),
        ),
        "Captured ETA Observations — Daily Trend": (
            "line",
            """SELECT
                (provider_updated_at_utc AT TIME ZONE 'UTC')::date
                    AS provider_update_date,
                count(*) AS observations
            FROM analytics.stg_portnet_eta
            GROUP BY 1
            ORDER BY 1""",
            (4, 0, 14, 9),
        ),
        "Calls by area": (
            "row",
            """SELECT
                coalesce(port_area_code, 'Unknown') AS port_area_code,
                count(*) AS port_calls
            FROM analytics.mart_portnet_latest_eta
            GROUP BY 1
            ORDER BY port_calls DESC, port_area_code
            LIMIT 10""",
            (4, 14, 10, 9),
        ),
        "ETA lead-time distribution": (
            "bar",
            """SELECT
                CASE
                    WHEN eta_utc < provider_updated_at_utc
                        THEN '00 Before provider update'
                    WHEN eta_utc < provider_updated_at_utc + interval '12 hours'
                        THEN '01 Under 12 hours'
                    WHEN eta_utc < provider_updated_at_utc + interval '1 day'
                        THEN '02 12–24 hours'
                    WHEN eta_utc < provider_updated_at_utc + interval '3 days'
                        THEN '03 1–3 days'
                    ELSE '04 Over 3 days'
                END AS lead_time_bucket,
                count(*) AS eta_records
            FROM analytics.mart_portnet_latest_eta
            GROUP BY 1
            ORDER BY 1""",
            (13, 0, 10, 9),
        ),
        "Current ETAs": (
            "table",
            """SELECT
                port_call_id,
                port_area_code,
                eta_utc,
                provider_updated_at_utc
            FROM analytics.mart_portnet_latest_eta
            ORDER BY eta_utc DESC
            LIMIT 50""",
            (13, 10, 14, 9),
        ),
        "Mart refreshed at (UTC)": (
            "table",
            """SELECT
                refreshed_at_utc
            FROM analytics.mart_pipeline_quality""",
            (22, 0, 10, 3),
        ),
    },
    "PortPulse — Synthetic ETA Analytics (SIMULATED DATA)": {
        "Synthetic calls (SIMULATED)": (
            "scalar",
            "SELECT count(*) AS synthetic_calls "
            "FROM analytics.mart_synthetic_eta_latest_eta",
            (0, 0, 6, 4),
        ),
        "Synthetic ETA updates (SIMULATED)": (
            "scalar",
            "SELECT count(*) AS eta_updates "
            "FROM analytics.mart_synthetic_eta_revision_history",
            (0, 6, 6, 4),
        ),
        "ETA revisions (SIMULATED)": (
            "scalar",
            "SELECT count(*) AS eta_revisions "
            "FROM analytics.mart_synthetic_eta_revision_history "
            "WHERE previous_eta_utc IS NOT NULL",
            (0, 12, 6, 4),
        ),
        "Rejected demo events (SIMULATED)": (
            "scalar",
            "SELECT rejected_events FROM analytics.mart_synthetic_eta_quality",
            (0, 18, 6, 4),
        ),
        "Synthetic ETA updates by event date": (
            "line",
            """SELECT
                event_date_utc,
                eta_updates,
                port_calls_updated
            FROM analytics.mart_synthetic_eta_daily_activity
            ORDER BY event_date_utc""",
            (4, 0, 14, 9),
        ),
        "Synthetic ETA source contributions": (
            "row",
            """SELECT
                source_id,
                count(*) AS eta_updates
            FROM analytics.mart_synthetic_eta_revision_history
            GROUP BY source_id
            ORDER BY eta_updates DESC, source_id""",
            (4, 14, 10, 9),
        ),
        "Synthetic ETA revision direction": (
            "bar",
            """SELECT
                CASE
                    WHEN eta_change_minutes < 0 THEN 'ETA moved earlier'
                    WHEN eta_change_minutes > 0 THEN 'ETA moved later'
                    ELSE 'ETA unchanged'
                END AS revision_direction,
                count(*) AS revisions
            FROM analytics.mart_synthetic_eta_revision_history
            WHERE previous_eta_utc IS NOT NULL
            GROUP BY 1
            ORDER BY 1""",
            (13, 0, 12, 8),
        ),
        "Synthetic ingestion quality": (
            "bar",
            """SELECT 'Rejected' AS issue_type, rejected_events AS event_count
            FROM analytics.mart_synthetic_eta_quality
            UNION ALL
            SELECT 'Duplicate', duplicate_issues
            FROM analytics.mart_synthetic_eta_quality
            UNION ALL
            SELECT 'ID collision', collision_issues
            FROM analytics.mart_synthetic_eta_quality""",
            (13, 12, 12, 8),
        ),
        "Largest synthetic ETA revisions": (
            "table",
            """SELECT
                port_call_id,
                source_id,
                event_time_utc,
                previous_eta_utc,
                eta_utc,
                eta_change_minutes
            FROM analytics.mart_synthetic_eta_revision_history
            WHERE previous_eta_utc IS NOT NULL
            ORDER BY abs(eta_change_minutes) DESC, port_call_id
            LIMIT 25""",
            (21, 0, 13, 9),
        ),
        "Latest synthetic ETAs": (
            "table",
            """SELECT
                port_call_id,
                source_id,
                eta_utc,
                event_time_utc
            FROM analytics.mart_synthetic_eta_latest_eta
            ORDER BY event_time_utc DESC
            LIMIT 25""",
            (21, 13, 11, 9),
        ),
    },
    "PortPulse — Kafka CDC Demo (SIMULATED DATA)": {
        "Active simulated calls": (
            "scalar",
            "SELECT count(*) AS active_simulated_calls "
            "FROM analytics.mart_stream_latest_eta",
            (0, 0, 6, 4),
        ),
        "CDC events captured (SIMULATED)": (
            "scalar",
            "SELECT count(*) AS cdc_events "
            "FROM analytics.stg_stream_eta_changes",
            (0, 6, 6, 4),
        ),
        "CDC changes by operation (SIMULATED)": (
            "bar",
            """SELECT
                CASE cdc_operation
                    WHEN 'c' THEN 'Create'
                    WHEN 'u' THEN 'Update'
                    WHEN 'd' THEN 'Delete'
                    WHEN 'r' THEN 'Snapshot'
                END AS operation,
                count(*) AS changes
            FROM analytics.stg_stream_eta_changes
            GROUP BY 1
            ORDER BY 1""",
            (4, 0, 12, 8),
        ),
        "Calls by area (simulated)": (
            "row",
            """SELECT
                port_area_code,
                count(*) AS active_simulated_calls
            FROM analytics.mart_stream_latest_eta
            GROUP BY port_area_code
            ORDER BY active_simulated_calls DESC, port_area_code
            LIMIT 10""",
            (4, 12, 12, 8),
        ),
        "Current simulated ETAs": (
            "table",
            """SELECT
                scenario_id,
                port_call_id,
                port_area_code,
                eta_utc,
                source_updated_at_utc,
                processed_at_utc
            FROM analytics.mart_stream_latest_eta
            ORDER BY scenario_id, port_call_id
            LIMIT 50""",
            (12, 0, 24, 9),
        ),
    },
}


def configure_reader(connection, password):
    exists = connection.execute(
        "SELECT 1 FROM pg_roles WHERE rolname = %s", (ROLE,)
    ).fetchone()
    if not exists:
        connection.execute("CREATE ROLE metabase_reader LOGIN")
    connection.execute(
        sql.SQL("ALTER ROLE metabase_reader WITH LOGIN PASSWORD {}").format(
            sql.Literal(password)
        )
    )
    privileges = connection.execute(
        "SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication "
        "OR rolbypassrls OR EXISTS (SELECT 1 FROM pg_auth_members m "
        "JOIN pg_roles r ON r.oid = m.member WHERE r.rolname = pg_roles.rolname) "
        "FROM pg_roles WHERE rolname = %s",
        (ROLE,),
    ).fetchone()[0]
    if privileges:
        raise RuntimeError(
            "metabase_reader has elevated role privileges or memberships"
        )

    database = connection.execute("SELECT current_database()").fetchone()[0]
    connection.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(database), sql.Identifier(ROLE)
        )
    )
    connection.execute("GRANT USAGE ON SCHEMA analytics TO metabase_reader")
    connection.execute(
        "GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO metabase_reader"
    )
    connection.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE portpulse IN SCHEMA analytics "
        "GRANT SELECT ON TABLES TO metabase_reader"
    )
    unsafe = connection.execute(
        """
        SELECT
            EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname IN ('public', 'stream_demo', 'synthetic_demo')
                  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
                  AND (
                    has_table_privilege(%s, c.oid, 'SELECT')
                    OR has_table_privilege(%s, c.oid, 'INSERT')
                    OR has_table_privilege(%s, c.oid, 'UPDATE')
                    OR has_table_privilege(%s, c.oid, 'DELETE')
                    OR has_table_privilege(%s, c.oid, 'TRUNCATE')
                  )
            )
            OR EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname IN ('public', 'stream_demo', 'synthetic_demo')
                  AND c.relkind = 'S'
                  AND (
                    has_sequence_privilege(%s, c.oid, 'USAGE')
                    OR has_sequence_privilege(%s, c.oid, 'SELECT')
                    OR has_sequence_privilege(%s, c.oid, 'UPDATE')
                  )
            )
            OR EXISTS (
                SELECT 1 FROM pg_namespace n
                WHERE n.nspname IN (
                    'public', 'stream_demo', 'synthetic_demo', 'analytics'
                )
                  AND has_schema_privilege(%s, n.oid, 'CREATE')
            )
        """,
        (ROLE, ROLE, ROLE, ROLE, ROLE, ROLE, ROLE, ROLE, ROLE),
    ).fetchone()[0]
    if unsafe:
        raise RuntimeError(
            "metabase_reader has unexpected source access or analytics write privileges"
        )


def request(path, method="GET", payload=None, session=None):
    headers = {"Content-Type": "application/json"}
    if session:
        headers["X-Metabase-Session"] = session
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE_URL + path, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        excerpt = error.read(512).decode("utf-8", "replace")
        excerpt = re.sub(
            r'(?i)(password|token|session)("?\s*:\s*"?)[^",} ]+',
            r"\1\2[redacted]",
            excerpt,
        )
        excerpt = re.sub(r'(?i)(received:\s*")[^"]+(")', r"\1[redacted]\2", excerpt)
        raise RuntimeError(
            f"Metabase {method} {path} failed with HTTP {error.code}: {excerpt}"
        ) from error


def find_or_create_collection(session):
    collections = request("/api/collection", session=session)
    return next(
        (item for item in collections if item.get("name") == COLLECTION),
        None,
    ) or request("/api/collection", "POST", {"name": COLLECTION}, session)


def collection_items(collection_id, session):
    result = request(f"/api/collection/{collection_id}/items", session=session)
    return result if isinstance(result, list) else result["data"]


def get_session(admin_email, admin_password):
    setup = request("/api/session/properties")
    if setup.get("setup-token"):
        result = request(
            "/api/setup",
            "POST",
            {
                "token": setup["setup-token"],
                "user": {
                    "first_name": "PortPulse",
                    "last_name": "Local Admin",
                    "email": admin_email,
                    "password": admin_password,
                },
                "prefs": {"site_name": "PortPulse"},
            },
        )
    else:
        result = request(
            "/api/session",
            "POST",
            {"username": admin_email, "password": admin_password},
        )
    return result["id"]


def setup_database(session, reader_password):
    databases = request("/api/database", session=session)["data"]
    database = next(
        (item for item in databases if item.get("name") == "PortPulse analytics"), None
    )
    details = {
        "host": "db",
        "port": 5432,
        "dbname": os.getenv("POSTGRES_DB", "portpulse"),
        "user": ROLE,
        "password": reader_password,
        "ssl": False,
        "schema-filters-type": "inclusion",
        "schema-filters": ["analytics"],
    }
    payload = {"name": "PortPulse analytics", "engine": "postgres", "details": details}
    if database:
        database_id = database["id"]
        request(f"/api/database/{database_id}", "PUT", payload, session)
    else:
        database_id = request("/api/database", "POST", payload, session)["id"]
    request(f"/api/database/{database_id}/sync_schema", "POST", {}, session)
    return database_id


def verify_card_queries(session, card_ids):
    for name, card_id in card_ids.items():
        result = request(f"/api/card/{card_id}/query", "POST", {}, session)
        if result.get("status") != "completed":
            detail = (
                result.get("error")
                or result.get("error_type")
                or result.get("message")
                or "no error details returned"
            )
            raise RuntimeError(
                f"Saved question {name!r} did not complete "
                f"(status={result.get('status')!r}): {detail}"
            )


def setup_cards_and_dashboards(session, database_id):
    collection = find_or_create_collection(session)
    items = collection_items(collection["id"], session)
    card_ids = {}
    for dashboard_name, questions in DASHBOARDS.items():
        for name, question in questions.items():
            display, query, layout = question
            old_names = {f"Portnet — {name}", f"CDC — {name}"}
            existing = next(
                (
                    item
                    for item in items
                    if item.get("name") in {name, *old_names}
                    and item.get("model") == "card"
                ),
                None,
            )
            card = {
                "name": name,
                "type": "question",
                "collection_id": collection["id"],
                "display": display,
                "visualization_settings": {},
                "dataset_query": {
                    "type": "native",
                    "native": {"query": query},
                    "database": database_id,
                },
            }
            result = (
                request(f"/api/card/{existing['id']}", "PUT", card, session)
                if existing
                else request("/api/card", "POST", card, session)
            )
            card_ids[name] = result["id"]

    dashboards = request("/api/dashboard", session=session)
    dashboards = dashboards if isinstance(dashboards, list) else dashboards["data"]
    for dashboard_name, questions in DASHBOARDS.items():
        dashboard = next(
            (item for item in dashboards if item.get("name") == dashboard_name), None
        )
        if dashboard:
            dashboard_id = dashboard["id"]
        else:
            dashboard_id = request(
                "/api/dashboard",
                "POST",
                {"name": dashboard_name, "collection_id": collection["id"]},
                session,
            )["id"]
        current = request(f"/api/dashboard/{dashboard_id}", session=session)
        dashcards = current.get("dashcards", [])
        for name, question in questions.items():
            card_id = card_ids[name]
            dashcard = next(
                (item for item in dashcards if item.get("card_id") == card_id), None
            )
            if dashcard is None:
                dashcard = {"id": -len(dashcards) - 1, "card_id": card_id}
                dashcards.append(dashcard)
            row, col, width, height = question[2]
            dashcard.update({"row": row, "col": col, "size_x": width, "size_y": height})
        request(
            f"/api/dashboard/{dashboard_id}", "PUT", {"dashcards": dashcards}, session
        )
        print(f"Dashboard ready: {BASE_URL}/dashboard/{dashboard_id}")
    verify_card_queries(session, card_ids)


def main():
    reader_password = os.getenv("METABASE_DB_PASSWORD", "portpulse-local-reader")
    admin_email = os.getenv("METABASE_ADMIN_EMAIL", "admin@example.test")
    admin_password = os.getenv("METABASE_ADMIN_PASSWORD", "portpulse-local-demo-2026")
    with psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        dbname=os.getenv("POSTGRES_DB", "portpulse"),
        user=os.getenv("POSTGRES_USER", "portpulse"),
        password=os.getenv("POSTGRES_PASSWORD", "admin"),
    ) as connection:
        configure_reader(connection, reader_password)
    session = get_session(admin_email, admin_password)
    database_id = setup_database(session, reader_password)
    setup_cards_and_dashboards(session, database_id)
    print("PortPulse analytics connection and dashboards are ready.")


if __name__ == "__main__":
    main()
