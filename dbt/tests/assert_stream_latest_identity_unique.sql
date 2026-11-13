SELECT scenario_id, port_call_id, port_area_code
FROM {{ ref('mart_stream_latest_eta') }}
GROUP BY scenario_id, port_call_id, port_area_code
HAVING COUNT(*) > 1
