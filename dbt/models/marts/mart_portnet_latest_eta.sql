
with collided_ids as (

    select distinct event_id
    from {{ source('portnet', 'portnet_eta_quality_issue') }}
    where issue_code = 'ID_COLLISION'

),

eligible as (

    select s.*
    from {{ ref('stg_portnet_eta') }} s
    left join collided_ids c
        on s.event_id = c.event_id
    where c.event_id is null

),

ranked as (

    select
        *,
        row_number() over (
            partition by port_call_id, port_area_code
            order by
                provider_updated_at_utc desc,
                collected_at_utc desc,
                ingested_at_utc desc,
                observation_id desc
        ) as row_num
    from eligible

)

select
    port_call_id,
    port_area_code,
    event_id,
    eta_utc,
    provider_updated_at_utc,
    collected_at_utc,
    ingested_at_utc

from ranked
where row_num = 1
