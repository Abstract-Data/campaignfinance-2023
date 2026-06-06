{#
  One row per entity natural key (entity_type, normalized_name, state) — the
  uix_entities_type_name_state grain. Person/org entities come from dim_persons (one
  entity_nk per person_nk, so person_id stays unique); committee entities from
  int_committees. When two distinct persons share a normalized name they collapse to
  one entity (min person_nk wins person_id — the builder's shared-entity behavior).
#}

with person_entities as (
    select
        entity_nk           as nk,
        entity_type,
        entity_name         as name,
        normalized_name,
        state_id,
        nk                  as person_nk,
        cast(null as text)  as committee_filer_id
    from {{ ref('dim_persons') }}
),

committee_entities as (
    select
        entity_nk           as nk,
        entity_type,
        name,
        normalized_name,
        state_id,
        cast(null as text)  as person_nk,
        filer_id            as committee_filer_id
    from {{ ref('int_committees') }}
),

all_entities as (
    select * from person_entities
    union all
    select * from committee_entities
)

select
    nk,
    max(entity_type)         as entity_type,
    max(name)                as name,
    max(normalized_name)     as normalized_name,
    max(state_id)            as state_id,
    min(person_nk)           as person_nk,
    min(committee_filer_id)  as committee_filer_id
from all_entities
group by nk
