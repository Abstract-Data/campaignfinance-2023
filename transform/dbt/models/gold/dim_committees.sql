{#
  One row per committee (filer_id is the natural PK — no surrogate int). Name / type
  / status come from the FILER master via int_committees. Address enrichment from
  FILER is still out of scope (no committee address column wired). committee_type is
  the raw TEC filerTypeCd; publish seeds committee_types so its FK resolves.
#}

select
    filer_id,
    state_id,
    name,
    normalized_name,
    entity_nk,
    committee_type,
    filer_status,
    cast(null as integer) as address_nk
from {{ ref('int_committees') }}
