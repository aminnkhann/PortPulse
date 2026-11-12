
with accepted as (

    select
        count(*) as accepted_observations
    from {{ source('portnet', 'portnet_eta_observation') }}

),

quarantine as (

    select
        count(*) as rejected_observations
    from {{ source('portnet', 'portnet_eta_rejected_event') }}

),

issues as (

    select
        count(*) filter (
            where issue_code = 'EXACT_DUPLICATE'
        ) as duplicate_issue_rows,

        count(*) filter (
            where issue_code = 'ID_COLLISION'
        ) as collision_issue_rows

    from {{ source('portnet', 'portnet_eta_quality_issue') }}

),

latest as (

    select
        count(*) as latest_eta_records
    from {{ ref('mart_portnet_latest_eta') }}

)

select
    1 as metric_id,
    current_timestamp as refreshed_at_utc,
    accepted.accepted_observations,
    quarantine.rejected_observations,
    issues.duplicate_issue_rows,
    issues.collision_issue_rows,
    latest.latest_eta_records

from accepted
cross join quarantine
cross join issues
cross join latest
