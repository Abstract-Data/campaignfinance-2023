{#
  One row per person natural key (individuals: lower(first),lower(last),state;
  orgs: lower(org),state — the two uix_persons_* partial-index grains). Attributes
  aggregated across occurrences to mimic the builder's backfill. The entity identity
  (entity_type / normalized_name / entity_nk) is derived HERE, once per person, from
  the representative name — so each person maps to exactly one entity and
  unified_entities.person_id stays unique.
#}

with agg as (
    select
        person_nk        as nk,
        max(state_id)    as state_id,
        max(person_type) as person_type,
        max(first_name)  as first_name,
        max(middle_name) as middle_name,
        max(last_name)   as last_name,
        max(suffix)      as suffix,
        max(organization) as organization,
        max(employer)    as employer,
        max(occupation)  as occupation,
        max(job_title)   as job_title,
        min(address_nk)  as address_nk
    from {{ ref('int_parties') }}
    group by person_nk
),

entity_fields as (
    select
        *,
        case when organization is not null then 'ORGANIZATION' else 'PERSON' end as entity_type,
        case
            when organization is not null then organization
            else nullif(trim(concat_ws(' ', first_name, middle_name, last_name, suffix)), '')
        end as entity_name
    from agg
),

normalized as (
    select *, {{ cf_normalize_name('entity_name') }} as normalized_name
    from entity_fields
)

select
    *,
    {{ dbt_utils.generate_surrogate_key(['entity_type', 'normalized_name', 'state_id']) }} as entity_nk
from normalized
