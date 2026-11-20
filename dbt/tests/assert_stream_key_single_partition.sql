select
    scenario_id,
    port_call_id,
    port_area_code
from {{ ref('stg_stream_eta_changes') }}
group by scenario_id, port_call_id, port_area_code
having count(distinct kafka_partition) > 1
