SELECT scenario_id, port_call_id, port_area_code
FROM {{ ref('mart_stream_latest_eta') }}
WHERE cdc_operation = 'd'
