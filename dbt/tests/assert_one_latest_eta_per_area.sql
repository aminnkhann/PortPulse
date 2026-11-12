
select
    port_call_id,
    port_area_code,
    count(*) as row_count
from {{ ref('mart_portnet_latest_eta') }}
group by
    port_call_id,
    port_area_code
having count(*) > 1
