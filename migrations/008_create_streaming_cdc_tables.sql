CREATE SCHEMA IF NOT EXISTS stream_demo;

CREATE TABLE IF NOT EXISTS stream_demo.port_eta_state (
    scenario_id TEXT NOT NULL,
    port_call_id TEXT NOT NULL,
    port_area_code TEXT NOT NULL,
    eta_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scenario_id, port_call_id, port_area_code)
);

ALTER TABLE stream_demo.port_eta_state REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS stream_demo.port_eta_cdc_event (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('c', 'u', 'd', 'r')),
    scenario_id TEXT NOT NULL,
    port_call_id TEXT NOT NULL,
    port_area_code TEXT NOT NULL,
    before_values JSONB,
    after_values JSONB,
    source_updated_at_utc TIMESTAMPTZ,
    processed_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'portpulse_stream_publication'
    ) THEN
        CREATE PUBLICATION portpulse_stream_publication
            FOR TABLE stream_demo.port_eta_state
            WITH (publish = 'insert, update, delete');
    END IF;
END
$$;
