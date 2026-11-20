WITH ranked_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY scenario_id, port_call_id, port_area_code
            ORDER BY source_updated_at_utc DESC, kafka_partition DESC, kafka_offset DESC
        ) AS event_rank
    FROM {{ ref('stg_stream_eta_changes') }}
)
SELECT
    scenario_id,
    port_call_id,
    port_area_code,
    eta_utc,
    cdc_operation,
    source_updated_at_utc,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    processed_at_utc
FROM ranked_events
WHERE event_rank = 1
  AND cdc_operation <> 'd'
