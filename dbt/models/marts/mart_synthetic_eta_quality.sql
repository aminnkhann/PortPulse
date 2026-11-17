select
    (select count(*) from {{ source('synthetic_demo', 'eta_observation') }})
        as accepted_observations,
    (select count(*) from {{ source('synthetic_demo', 'eta_rejected_event') }})
        as rejected_events,
    (select count(*) from {{ source('synthetic_demo', 'eta_quality_issue') }}
     where issue_code = 'EXACT_DUPLICATE') as duplicate_issues,
    (select count(*) from {{ source('synthetic_demo', 'eta_quality_issue') }}
     where issue_code = 'ID_COLLISION') as collision_issues
