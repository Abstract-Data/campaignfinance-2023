{#
  FILER committee master (raw). One row per filer id, with the canonical 8-char
  filer id, name, committee type (filerTypeCd), and status (committeeStatusCd).
#}

select
    lpad(filerident::text, 8, '0')              as filer_id,
    {{ var('state_id') }}                       as state_id,
    {{ cf_clean('filername') }}                 as name,
    {{ cf_clean('filertypecd') }}               as committee_type,
    {{ cf_clean('committeestatuscd') }}         as filer_status,
    {{ cf_clean('filernameorganization') }}     as organization
from {{ source('silver', 'tx_filers') }}
where filerident is not null
