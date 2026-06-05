select
    ek.id,
    e.entity_type,                 -- ::entitytype on publish
    e.name,
    e.normalized_name,
    pk.id              as person_id,    -- unique; null for committee entities
    e.committee_filer_id as committee_id,  -- string FK; null for person entities
    cast(null as integer) as address_id,
    e.state_id
from {{ ref('dim_entities') }} e
join {{ ref('entity_keys') }} ek on ek.nk = e.nk
left join {{ ref('person_keys') }} pk on pk.nk = e.person_nk
