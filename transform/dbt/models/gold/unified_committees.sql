select
    c.filer_id,
    c.name,
    c.committee_type,
    c.filer_status,
    cast(null as integer) as address_id,   -- FILER-file address enrichment is Tier-3
    c.state_id
from {{ ref('dim_committees') }} c
