with fixture_loaded as (
    select exists (
        select 1
        from {{ source('portnet', 'portnet_eta_observation') }}
        where event_id = 'portnet:dbt-ci-latest-old'
    ) as loaded
)

select 'portnet:dbt-ci-collision' as event_id
from fixture_loaded
where fixture_loaded.loaded
  and (
    select count(*)
    from {{ ref('mart_portnet_id_collisions') }}
    where event_id = 'portnet:dbt-ci-collision'
      and port_call_id = 'portnet:dbt-ci-collision'
  ) <> 2
