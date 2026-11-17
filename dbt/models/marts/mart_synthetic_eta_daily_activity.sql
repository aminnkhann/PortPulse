select
    (event_time_utc at time zone 'UTC')::date as event_date_utc,
    count(*) as eta_updates,
    count(distinct port_call_id) as port_calls_updated
from {{ ref('stg_synthetic_eta') }}
group by 1
