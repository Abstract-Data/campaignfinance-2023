select
    pk.id,
    d.first_name,
    d.last_name,
    d.middle_name,
    d.suffix,
    d.organization,
    d.employer,
    d.occupation,
    d.job_title,
    d.person_type,              -- ::persontype on publish
    ak.id as address_id,
    d.state_id
from {{ ref('dim_persons') }} d
join {{ ref('person_keys') }} pk on pk.nk = d.nk
left join {{ ref('address_keys') }} ak on ak.nk = d.address_nk
