{#
  One row per address natural key. Two grains mirror the partial unique indexes:
  with-street (street_1,city,state,zip) and without-street (city,state,zip) — the
  has_street flag in the nk keeps them disjoint (PIPELINE_REVIEW Fix 5: NULL-street
  rows still dedupe instead of exploding).
#}

select
    address_nk      as nk,
    max(street_1)   as street_1,
    max(street_2)   as street_2,
    max(city)       as city,
    max(state)      as state,
    max(zip_code)   as zip_code,
    max(country)    as country,
    max(county)     as county
from {{ ref('int_parties') }}
where address_nk is not null
group by address_nk
