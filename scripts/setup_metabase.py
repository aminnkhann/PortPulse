"""Create the local Metabase analytics connection and PortPulse dashboards."""

import json
import os
import time
import urllib.error
import urllib.request

import psycopg
from psycopg import sql


BASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
ROLE = "metabase_reader"
COLLECTION = "PortPulse"

CARDS = {
    "Portnet — Latest ETA records": (
        "scalar",
        "SELECT count(*) AS latest_eta_records FROM analytics.mart_portnet_latest_eta",
    ),
    "Portnet — Calls by area": (
        "bar",
        "SELECT port_area_code, count(*) AS port_calls "
        "FROM analytics.mart_portnet_latest_eta "
        "GROUP BY port_area_code ORDER BY port_area_code",
    ),
    "Portnet — Current ETAs": (
        "table",
        "SELECT port_call_id, port_area_code, "
        "to_char(eta_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI \"UTC\"') "
        "AS eta_utc, to_char(provider_updated_at_utc AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD HH24:MI \"UTC\"') AS provider_updated_at_utc "
        "FROM analytics.mart_portnet_latest_eta ORDER BY eta_utc LIMIT 100",
    ),
    "Portnet — Accepted observations": (
        "scalar",
        "SELECT accepted_observations FROM analytics.mart_pipeline_quality",
    ),
    "Portnet — Rejected observations": (
        "scalar",
        "SELECT rejected_observations FROM analytics.mart_pipeline_quality",
    ),
    "Portnet — Duplicate issues": (
        "scalar",
        "SELECT duplicate_issue_rows FROM analytics.mart_pipeline_quality",
    ),
    "Portnet — Collision issues": (
        "scalar",
        "SELECT collision_issue_rows FROM analytics.mart_pipeline_quality",
    ),
    "Portnet — Mart refreshed at (UTC)": (
        "table",
        "SELECT to_char(refreshed_at_utc AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD HH24:MI \"UTC\"') AS refreshed_at_utc "
        "FROM analytics.mart_pipeline_quality",
    ),
    "CDC — Active simulated calls": (
        "scalar",
        "SELECT count(*) AS active_simulated_calls "
        "FROM analytics.mart_stream_latest_eta",
    ),
    "CDC — Current simulated ETAs": (
        "table",
        "SELECT scenario_id, port_call_id, port_area_code, "
        "to_char(eta_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI \"UTC\"') "
        "AS eta_utc, to_char(source_updated_at_utc AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD HH24:MI \"UTC\"') AS source_updated_at_utc, "
        "to_char(processed_at_utc AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD HH24:MI \"UTC\"') AS processed_at_utc "
        "FROM analytics.mart_stream_latest_eta ORDER BY scenario_id, port_call_id",
    ),
    "CDC — Calls by area (simulated)": (
        "bar",
        "SELECT port_area_code, count(*) AS active_simulated_calls "
        "FROM analytics.mart_stream_latest_eta "
        "GROUP BY port_area_code ORDER BY port_area_code",
    ),
}

DASHBOARDS = {
    "PortPulse — Portnet Overview": list(CARDS)[:8],
    "PortPulse — Kafka CDC Demo (SIMULATED DATA)": list(CARDS)[8:],
}

CARD_LAYOUT = {
    "Portnet — Latest ETA records": (0, 0, 4, 4),
    "Portnet — Accepted observations": (0, 4, 4, 4),
    "Portnet — Rejected observations": (0, 8, 4, 4),
    "Portnet — Duplicate issues": (0, 12, 4, 4),
    "Portnet — Collision issues": (0, 16, 4, 4),
    "Portnet — Calls by area": (1, 0, 12, 6),
    "Portnet — Current ETAs": (2, 0, 24, 8),
    "Portnet — Mart refreshed at (UTC)": (3, 0, 12, 4),
    "CDC — Active simulated calls": (0, 0, 6, 4),
    "CDC — Current simulated ETAs": (1, 0, 24, 8),
    "CDC — Calls by area (simulated)": (2, 0, 12, 6),
}


def configure_reader(connection, password):
    connection.execute("CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION portpulse")
    exists = connection.execute(
        "SELECT 1 FROM pg_roles WHERE rolname = %s", (ROLE,)
    ).fetchone()
    if not exists:
        connection.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(ROLE)))
    connection.execute(
        sql.SQL(
            "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION NOBYPASSRLS PASSWORD {}"
        ).format(sql.Identifier(ROLE), sql.Literal(password))
    )
    memberships = connection.execute(
        "SELECT parent.rolname FROM pg_auth_members membership "
        "JOIN pg_roles parent ON parent.oid = membership.roleid "
        "JOIN pg_roles member ON member.oid = membership.member "
        "WHERE member.rolname = %s",
        (ROLE,),
    ).fetchall()
    for (parent_role,) in memberships:
        connection.execute(
            sql.SQL("REVOKE {} FROM {}").format(
                sql.Identifier(parent_role), sql.Identifier(ROLE)
            )
        )
    connection.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(os.getenv("PGDATABASE", "portpulse")), sql.Identifier(ROLE)
        )
    )
    connection.execute("REVOKE ALL ON SCHEMA public, stream_demo FROM metabase_reader")
    connection.execute(
        "REVOKE ALL ON ALL TABLES IN SCHEMA public, stream_demo FROM metabase_reader"
    )
    connection.execute(
        "REVOKE ALL ON ALL SEQUENCES IN SCHEMA public, stream_demo FROM metabase_reader"
    )
    for schema_name in ("public", "stream_demo"):
        connection.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE portpulse IN SCHEMA {} "
                "REVOKE ALL ON TABLES FROM {}"
            ).format(sql.Identifier(schema_name), sql.Identifier(ROLE))
        )
        connection.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE portpulse IN SCHEMA {} "
                "REVOKE ALL ON SEQUENCES FROM {}"
            ).format(sql.Identifier(schema_name), sql.Identifier(ROLE))
        )
    connection.execute("REVOKE CREATE ON SCHEMA analytics FROM metabase_reader")
    connection.execute(
        "REVOKE ALL ON ALL TABLES IN SCHEMA analytics FROM metabase_reader"
    )
    connection.execute(
        "REVOKE ALL ON ALL SEQUENCES IN SCHEMA analytics FROM metabase_reader"
    )
    connection.execute("GRANT USAGE ON SCHEMA analytics TO metabase_reader")
    connection.execute(
        "GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO metabase_reader"
    )
    connection.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE portpulse IN SCHEMA analytics "
        "REVOKE ALL ON TABLES FROM metabase_reader"
    )
    connection.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE portpulse IN SCHEMA analytics "
        "GRANT SELECT ON TABLES TO metabase_reader"
    )
    connection.execute(
        "ALTER DEFAULT PRIVILEGES FOR ROLE portpulse IN SCHEMA analytics "
        "REVOKE ALL ON SEQUENCES FROM metabase_reader"
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
        raise RuntimeError(
            f"Metabase {method} {path} failed with HTTP {error.code}; "
            "check the Metabase API log for details."
        ) from error


def wait_until_ready():
    for _ in range(60):
        try:
            if request("/api/health").get("status") == "ok":
                return
        except (OSError, RuntimeError, ValueError):
            time.sleep(2)
    raise RuntimeError("Metabase did not become ready at " + BASE_URL)


def find_or_create_collection(session):
    collections = request("/api/collection", session=session)
    found = next((item for item in collections if item.get("name") == COLLECTION), None)
    return found or request("/api/collection", "POST", {"name": COLLECTION}, session)


def collection_items(collection_id, session):
    result = request(f"/api/collection/{collection_id}/items", session=session)
    return result.get("data", result if isinstance(result, list) else [])


def get_session(admin_email, admin_password):
    setup_token = request("/api/session/properties").get("setup-token")
    if setup_token:
        result = request(
            "/api/setup",
            "POST",
            {
                "token": setup_token,
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
    databases = request("/api/database", session=session).get("data", [])
    database = next(
        (item for item in databases if item.get("name") == "PortPulse analytics"),
        None,
    )
    details = {
        "host": "db",
        "port": 5432,
        "dbname": "portpulse",
        "user": ROLE,
        "password": reader_password,
        "ssl": False,
        "schema-filters-type": "inclusion",
        "schema-filters": ["analytics"],
    }
    if database:
        database_id = database["id"]
        request(
            f"/api/database/{database_id}",
            "PUT",
            {"name": "PortPulse analytics", "engine": "postgres", "details": details},
            session,
        )
    else:
        database = request(
            "/api/database",
            "POST",
            {"name": "PortPulse analytics", "engine": "postgres", "details": details},
            session,
        )
        database_id = database["id"]
    request(f"/api/database/{database_id}/sync_schema", "POST", {}, session)
    return database_id


def setup_cards_and_dashboards(session, database_id):
    collection = find_or_create_collection(session)
    items = collection_items(collection["id"], session)
    card_ids = {}
    for name, (display, query) in CARDS.items():
        existing = next(
            (
                item
                for item in items
                if item.get("name") == name and item.get("model") == "card"
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
    if isinstance(dashboards, dict):
        dashboards = dashboards.get("data", [])
    for dashboard_name, names in DASHBOARDS.items():
        dashboard = next(
            (item for item in dashboards if item.get("name") == dashboard_name), None
        )
        if dashboard:
            dashboard_id = dashboard["id"]
        else:
            dashboard = request(
                "/api/dashboard",
                "POST",
                {"name": dashboard_name, "collection_id": collection["id"]},
                session,
            )
            dashboard_id = dashboard["id"]
        current = request(f"/api/dashboard/{dashboard_id}", session=session)
        dashcards = current.get("dashcards", [])
        for name in names:
            card_id = card_ids[name]
            row, col, width, height = CARD_LAYOUT[name]
            dashcard = next(
                (item for item in dashcards if item.get("card_id") == card_id), None
            )
            if dashcard is None:
                dashcard = {"id": -len(dashcards) - 1, "card_id": card_id}
                dashcards.append(dashcard)
            dashcard.update({"row": row, "col": col, "size_x": width, "size_y": height})
        request(
            f"/api/dashboard/{dashboard_id}",
            "PUT",
            {"dashcards": dashcards},
            session,
        )
        print(f"Dashboard: {BASE_URL}/dashboard/{dashboard_id}")

    for card_id in card_ids.values():
        request(f"/api/card/{card_id}/query", "POST", {}, session)


def main():
    reader_password = os.getenv("METABASE_DB_PASSWORD")
    admin_email = os.getenv("METABASE_ADMIN_EMAIL")
    admin_password = os.getenv("METABASE_ADMIN_PASSWORD")
    if not reader_password or not admin_email or not admin_password:
        raise SystemExit(
            "Set METABASE_DB_PASSWORD, METABASE_ADMIN_EMAIL and "
            "METABASE_ADMIN_PASSWORD before running the setup script."
        )

    database_url = os.getenv(
        "PORTPULSE_ADMIN_DATABASE_URL",
        "postgresql://portpulse:admin@localhost:5432/portpulse",
    )
    with psycopg.connect(database_url) as connection:
        configure_reader(connection, reader_password)

    wait_until_ready()
    session = get_session(admin_email, admin_password)
    database_id = setup_database(session, reader_password)
    setup_cards_and_dashboards(session, database_id)
    print("PortPulse analytics connection and dashboard questions are ready.")


if __name__ == "__main__":
    main()
