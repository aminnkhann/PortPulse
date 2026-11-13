
select
    current_timestamp as refreshed_at_utc,
    (select count(*) from {{ source('portnet', 'portnet_eta_observation') }})
        as accepted_observations,
    (select count(*) from {{ source('portnet', 'portnet_eta_rejected_event') }})
        as rejected_observations,
    (select count(*) from {{ source('portnet', 'portnet_eta_quality_issue') }}
     where issue_code = 'EXACT_DUPLICATE') as duplicate_issue_rows,
    (select count(*) from {{ source('portnet', 'portnet_eta_quality_issue') }}
     where issue_code = 'ID_COLLISION') as collision_issue_rows,
    (select count(*) from {{ ref('mart_portnet_latest_eta') }})
        as latest_eta_records
