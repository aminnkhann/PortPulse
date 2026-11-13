"""Register the local Debezium PostgreSQL connector with Kafka Connect."""

import json
import os
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
CONNECTOR_FILE = ROOT / "config" / "debezium" / "portpulse-postgres.json"
CONNECT_URL = os.environ.get("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "portpulse-postgres-cdc"


def request_json(path: str, method: str = "GET", payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(
        f"{CONNECT_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=5) as response:
        body = response.read()
        return json.loads(body) if body else None


def wait_for_connect(deadline: float) -> list[str]:
    while True:
        try:
            return request_json("/connectors")
        except (URLError, TimeoutError, OSError) as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Kafka Connect did not become ready at "
                    f"{CONNECT_URL}; check `docker compose logs kafka-connect`: {error}"
                ) from error
            time.sleep(2)


def wait_for_connector_task(deadline: float) -> None:
    while time.monotonic() < deadline:
        try:
            status = request_json(f"/connectors/{CONNECTOR_NAME}/status")
        except (URLError, TimeoutError, OSError):
            time.sleep(2)
            continue
        tasks = status.get("tasks", [])
        if status.get("connector", {}).get("state") == "RUNNING" and tasks:
            if all(task.get("state") == "RUNNING" for task in tasks):
                print(f"Connector and {len(tasks)} task(s) are RUNNING")
                return
        if (
            any(task.get("state") == "FAILED" for task in tasks)
            or status.get("connector", {}).get("state") == "FAILED"
        ):
            raise RuntimeError(
                "Debezium connector failed: "
                + json.dumps(status, sort_keys=True)
                + ". Check PostgreSQL logical replication settings and migration 008."
            )
        time.sleep(2)
    raise RuntimeError(
        "Debezium connector did not reach RUNNING within 90 seconds; "
        f"inspect {CONNECT_URL}/connectors/{CONNECTOR_NAME}/status"
    )


def register_connector() -> None:
    connector = json.loads(CONNECTOR_FILE.read_text(encoding="utf-8"))
    connector["config"]["database.password"] = os.environ.get(
        "POSTGRES_PASSWORD", "admin"
    )
    deadline = time.monotonic() + 90
    names = wait_for_connect(deadline)
    if CONNECTOR_NAME not in names:
        request_json("/connectors", "POST", connector)
        print(f"Registered {CONNECTOR_NAME}")
    else:
        current = request_json(f"/connectors/{CONNECTOR_NAME}/config")
        expected_config = {
            key: value
            for key, value in connector["config"].items()
            if key != "database.password"
        }
        if any(current.get(key) != value for key, value in expected_config.items()):
            raise RuntimeError(
                f"{CONNECTOR_NAME} already exists with a different configuration; "
                "inspect it at GET /connectors/portpulse-postgres-cdc/config"
            )
        print(f"{CONNECTOR_NAME} is already registered")
    wait_for_connector_task(deadline)


if __name__ == "__main__":
    register_connector()
