"""Pydantic model for the v1 synthetic port-call event contract."""

import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "contracts" / "port_call_event_v1.schema.json"
UtcTimestamp = Annotated[
    str,
    StringConstraints(pattern=r"Z$"),
    Field(json_schema_extra={"format": "date-time"}),
]


class PortCallEvent(BaseModel):
    """Validated event fields; this model also generates the JSON Schema."""

    model_config = ConfigDict(
        extra="forbid", title="PortPulse synthetic port-call ETA event v1"
    )

    event_id: Annotated[str, StringConstraints(min_length=1)]
    port_call_id: Annotated[str, StringConstraints(min_length=1)]
    source_id: Literal["coordinator_sim", "carrier_sim", "terminal_sim"]
    event_time_utc: UtcTimestamp
    received_at_utc: UtcTimestamp
    eta_utc: UtcTimestamp
    schema_version: Literal["v1"]

    @field_validator("event_time_utc", "received_at_utc", "eta_utc")
    @classmethod
    def check_utc_timestamp(cls, value):
        try:
            datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as error:
            raise ValueError("must be an ISO-8601 UTC timestamp ending in Z") from error
        return value


def write_json_schema():
    content = json.dumps(build_json_schema(), indent=2) + "\n"
    SCHEMA.write_text(content, encoding="utf-8")


def build_json_schema():
    schema = PortCallEvent.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://portpulse.example/schemas/port_call_event_v1.schema.json"
    return schema


if __name__ == "__main__":
    write_json_schema()
