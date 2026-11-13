"""Pydantic model for the v1 synthetic port-call event contract."""

import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
)


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "contracts" / "port_call_event_v1.schema.json"
MARINE_ETA_SCHEMA = ROOT / "contracts" / "marine_eta_event_v1.schema.json"


def check_utc_timestamp(value):
    try:
        datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError("must be an ISO-8601 UTC timestamp ending in Z") from error
    return value


UtcTimestamp = Annotated[
    str,
    StringConstraints(pattern=r"Z$"),
    Field(json_schema_extra={"format": "date-time"}),
    AfterValidator(check_utc_timestamp),
]


class EventFields(BaseModel):
    """Shared normalized fields for source-specific event contracts."""

    model_config = ConfigDict(extra="forbid")

    event_id: Annotated[str, StringConstraints(min_length=1)]
    port_call_id: Annotated[str, StringConstraints(min_length=1)]
    event_time_utc: UtcTimestamp
    received_at_utc: UtcTimestamp
    eta_utc: UtcTimestamp


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


def write_json_schema():
    for path, schema in (
        (SCHEMA, build_json_schema()),
        (MARINE_ETA_SCHEMA, build_marine_eta_json_schema()),
    ):
        path.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")


def build_json_schema():
    return build_schema(
        PortCallEvent,
        "https://portpulse.example/schemas/port_call_event_v1.schema.json",
    )


def build_marine_eta_json_schema():
    return build_schema(
        MarineEtaEvent,
        "https://portpulse.example/schemas/marine_eta_event_v1.schema.json",
    )


def build_schema(model, schema_id):
    schema = model.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = schema_id
    return schema


if __name__ == "__main__":
    write_json_schema()
