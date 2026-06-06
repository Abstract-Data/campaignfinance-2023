select
    k.id,
    d.street_1,
    d.street_2,
    d.city,
    d.state,
    d.zip_code,
    d.country,
    d.county
from {{ ref('dim_addresses') }} d
join {{ ref('address_keys') }} k on k.nk = d.nk
