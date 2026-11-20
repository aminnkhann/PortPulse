with actual as (
    select latest_recorded_collection_utc
    from {{ ref('mart_portnet_latest_collection') }}
), fixture_loaded as (
    select exists (
        select 1
        from {{ source('portnet', 'portnet_eta_observation') }}
        where event_id = 'portnet:dbt-ci-latest-old'
    ) as loaded
)

select latest_recorded_collection_utc
from actual
cross join fixture_loaded
where fixture_loaded.loaded
  and latest_recorded_collection_utc <> timestamptz '2026-11-01T11:01:00Z'
