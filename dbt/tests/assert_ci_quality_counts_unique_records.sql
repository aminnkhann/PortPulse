with quality as (
    select *
    from {{ ref('mart_pipeline_quality') }}
), fixture_loaded as (
    select exists (
        select 1
        from {{ source('portnet', 'portnet_eta_observation') }}
        where event_id = 'portnet:dbt-ci-latest-old'
    ) as loaded
)

select quality.*
from quality
cross join fixture_loaded
where fixture_loaded.loaded
  and (
      quality.unique_rejected_records <> 1
      or quality.unique_duplicate_delivery_problems <> 1
      or quality.identity_collision_groups <> 1
  )
