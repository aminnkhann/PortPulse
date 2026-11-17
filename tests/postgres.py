"""Small helpers for PostgreSQL integration tests."""

import os
import uuid
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo


MIGRATIONS = Path(__file__).resolve().parents[1] / "migrations"


def create_test_schema(test_case, prefix):
    base_url = os.environ.get("PORTPULSE_TEST_DATABASE_URL")
    if not base_url:
        test_case.skipTest("set PORTPULSE_TEST_DATABASE_URL for PostgreSQL checks")

    schema_name = f"{prefix}_{uuid.uuid4().hex}"
    with psycopg.connect(base_url) as connection:
        connection.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name))
        )

    def drop_schema():
        with psycopg.connect(base_url) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema_name)
                )
            )

    test_case.addCleanup(drop_schema)
    return make_conninfo(base_url, options=f"-c search_path={schema_name}")


def apply_migrations(database_url, *names):
    migrations = (
        [MIGRATIONS / name for name in names]
        if names
        else sorted(MIGRATIONS.glob("*.sql"))
    )
    with psycopg.connect(database_url) as connection:
        for migration in migrations:
            contents = migration.read_text()
            if migration.name == "010_create_synthetic_eta_schema.sql":
                schema = connection.execute("SELECT current_schema()").fetchone()[0]
                if schema != "public":
                    contents = contents.replace("synthetic_demo", schema)
            connection.execute(contents, prepare=False)
