import os
import unittest

import psycopg
from psycopg.errors import InsufficientPrivilege


class MetabaseReaderIntegrationTests(unittest.TestCase):
    def setUp(self):
        if "METABASE_DB_PASSWORD" not in os.environ:
            self.skipTest(
                "set METABASE_DB_PASSWORD to run PostgreSQL permission checks"
            )

    def connect_reader(self):
        return psycopg.connect(
            host=os.getenv("METABASE_TEST_HOST", "127.0.0.1"),
            port=os.getenv("METABASE_TEST_PORT", "5432"),
            dbname=os.getenv("POSTGRES_DB", "portpulse"),
            user="metabase_reader",
            password=os.environ["METABASE_DB_PASSWORD"],
        )

    def test_reader_can_read_mart_but_not_sources_or_write(self):
        with self.connect_reader() as connection:
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM analytics.mart_pipeline_quality LIMIT 1"
                ).fetchone()
            )
            with self.assertRaises(InsufficientPrivilege):
                connection.execute(
                    "SELECT 1 FROM public.portnet_eta_observation LIMIT 1"
                )
            connection.rollback()
            with self.assertRaises(InsufficientPrivilege):
                connection.execute(
                    "SELECT 1 FROM synthetic_demo.eta_observation LIMIT 1"
                )
            connection.rollback()
            with self.assertRaises(InsufficientPrivilege):
                connection.execute(
                    "UPDATE analytics.mart_pipeline_quality "
                    "SET accepted_observations = accepted_observations WHERE FALSE"
                )
            connection.rollback()


if __name__ == "__main__":
    unittest.main()
