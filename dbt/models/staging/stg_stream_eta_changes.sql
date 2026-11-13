SELECT
    event_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    operation AS cdc_operation,
    scenario_id,
    port_call_id,
    port_area_code,
    CASE jsonb_typeof(COALESCE(after_values -> 'eta_utc', before_values -> 'eta_utc'))
        WHEN 'number' THEN to_timestamp(
            (COALESCE(after_values ->> 'eta_utc', before_values ->> 'eta_utc'))::double precision / 1000
        )
        WHEN 'string' THEN (
            COALESCE(after_values ->> 'eta_utc', before_values ->> 'eta_utc')
        )::timestamptz
    END AS eta_utc,
    source_updated_at_utc,
    processed_at_utc
FROM {{ source('stream_demo', 'port_eta_cdc_event') }}
