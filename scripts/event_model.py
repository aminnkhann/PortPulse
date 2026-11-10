"""Pydantic model for the v1 synthetic port-call event contract."""

import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "contracts" / "port_call_event_v1.schema.json"
MARINE_ETA_SCHEMA = ROOT / "contracts" / "marine_eta_event_v1.schema.json"
UtcTimestamp = Annotated[
    str,
    StringConstraints(pattern=r"Z$"),
    Field(json_schema_extra={"format": "date-time"}),
]


class EventFields(BaseModel):
    """Shared normalized fields for source-specific event contracts."""
    model_config = ConfigDict(
        extra="forbid"
    )

    event_id: Annotated[str, StringConstraints(min_length=1)]
    port_call_id: Annotated[str, StringConstraints(min_length=1)]
    event_time_utc: UtcTimestamp
    received_at_utc: UtcTimestamp
    eta_utc: UtcTimestamp

    @field_validator("event_time_utc", "received_at_utc", "eta_utc")
    @classmethod
    def check_utc_timestamp(cls, value):
        try:
            datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as error:
            raise ValueError("must be an ISO-8601 UTC timestamp ending in Z") from error
        return value


class PortCallEvent(EventFields):
    """Validated v1 event fields for the original synthetic fixture."""

    model_config = ConfigDict(
        extra="forbid", title="PortPulse synthetic port-call ETA event v1"
    )
    source_id: Literal["coordinator_sim", "carrier_sim", "terminal_sim"]
    schema_version: Literal["v1"]


class MarineEtaEvent(EventFields):
    """Validated ETA event normalized from Portnet V1."""

    model_config = ConfigDict(extra="forbid", title="PortPulse Portnet ETA event v1")
    source_id: Literal["portnet"]
    schema_version: Literal["marine-v1"]


class PortnetEtaPayload(BaseModel):
    """Selected Portnet ETA fields retained for audit."""

    model_config = ConfigDict(extra="forbid")

    port_call_id: Annotated[int, Field(gt=0)]
    port_area_code: str | None
    eta_utc: UtcTimestamp
    eta_source: str | None
    eta_timestamp_utc: UtcTimestamp

    @field_validator("eta_utc", "eta_timestamp_utc")
    @classmethod
    def check_utc_timestamp(cls, value):
        try:
            datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as error:
            raise ValueError("must be an ISO-8601 UTC timestamp ending in Z") from error
        return value


def write_json_schema():
    for path, schema in (
        (SCHEMA, build_json_schema()),
        (MARINE_ETA_SCHEMA, build_marine_eta_json_schema()),
    ):
        path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")


def build_json_schema():
    schema = PortCallEvent.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://portpulse.example/schemas/port_call_event_v1.schema.json"
    return schema


def build_marine_eta_json_schema():
    schema = MarineEtaEvent.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = "https://portpulse.example/schemas/marine_eta_event_v1.schema.json"
    return schema


if __name__ == "__main__":
    write_json_schema()
