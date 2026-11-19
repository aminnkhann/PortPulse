
select
    current_timestamp as refreshed_at_utc,
    (select count(*) from {{ source('portnet', 'portnet_eta_observation') }})
        as accepted_observations,
    (select count(*) from (
        select distinct reason_code, event_id, raw_payload, details
        from {{ source('portnet', 'portnet_eta_rejected_event') }}
    ) rejected) as unique_rejected_records,
    (select count(*) from (
        select distinct event_id, details ->> 'payload_sha256' as payload_sha256
        from {{ source('portnet', 'portnet_eta_quality_issue') }}
        where issue_code = 'EXACT_DUPLICATE'
    ) duplicates) as unique_duplicate_delivery_problems,
    (select count(distinct event_id)
     from {{ source('portnet', 'portnet_eta_quality_issue') }}
     where issue_code = 'ID_COLLISION') as identity_collision_groups,
    (select count(*) from {{ ref('mart_portnet_latest_eta') }})
        as latest_eta_records
