select
    ek.id,
    e.entity_type,                 -- ::entitytype on publish
    e.name,
    e.normalized_name,
    pk.id              as person_id,    -- unique; null for committee entities
    e.committee_filer_id as committee_id,  -- string FK; null for person entities
    ak.id              as address_id,   -- the person's address (enables resolve occupancy backfill)
    e.state_id
from {{ ref('dim_entities') }} e
join {{ ref('entity_keys') }} ek on ek.nk = e.nk
left join {{ ref('person_keys') }} pk on pk.nk = e.person_nk
left join {{ ref('dim_persons') }} dp on dp.nk = e.person_nk
left join {{ ref('address_keys') }} ak on ak.nk = dp.address_nk
