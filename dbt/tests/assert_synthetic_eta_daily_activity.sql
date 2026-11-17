with expected as (

    select
        (event_time_utc at time zone 'UTC')::date as event_date_utc,
        count(*) as eta_updates,
        count(distinct port_call_id) as port_calls_updated
    from {{ ref('stg_synthetic_eta') }}
    group by 1

),

actual as (

    select event_date_utc, eta_updates, port_calls_updated
    from {{ ref('mart_synthetic_eta_daily_activity') }}

)

select
    expected.event_date_utc as expected_event_date_utc,
    actual.event_date_utc as actual_event_date_utc
from expected
full outer join actual using (event_date_utc)
where expected.event_date_utc is null
   or actual.event_date_utc is null
   or actual.eta_updates is distinct from expected.eta_updates
   or actual.port_calls_updated is distinct from expected.port_calls_updated
