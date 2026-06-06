{#
  Committee officers (treasurers) from the FILER master, linked to the committee +
  the officer's shared person/entity. One row per committee with a treasurer.
#}
select
    row_number() over (order by p.committee_filer_id) as id,
    p.committee_filer_id  as committee_id,
    pk.id                 as person_id,
    ek.id                 as entity_id,
    p.state_id,
    'TREASURER'           as role,
    true                  as is_active
from {{ ref('int_parties') }} p
join {{ ref('person_keys') }} pk on pk.nk = p.person_nk
join {{ ref('dim_persons') }} dp on dp.nk = p.person_nk
join {{ ref('entity_keys') }} ek on ek.nk = dp.entity_nk
join {{ ref('dim_committees') }} c on c.filer_id = p.committee_filer_id
where p.occurrence_type = 'officer'
