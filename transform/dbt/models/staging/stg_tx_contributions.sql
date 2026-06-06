{#
  RCPT → one CONTRIBUTOR person + committee (RECIPIENT). Each role names its OWN
  source columns (contributor*), so a contribution row can never emit a PAYEE/etc.
  Canonical wide shape shared by every record type (so the unions line up).
#}
with src as (select * from {{ source('silver', 'tx_contributions') }})
select
    'RCPT'                                          as record_type,
    'CONTRIBUTION'                                  as transaction_type,
    'CONTRIBUTOR'                                   as role,
    {{ var('state_id') }}                           as state_id,
    contributionInfoId::text                        as source_transaction_id,
    {{ cf_safe_numeric('contributionAmount') }}     as amount,
    {{ cf_safe_date('contributionDt') }}            as transaction_date,
    {{ cf_clean('contributionDescr') }}             as description,
    reportInfoIdent::text                           as report_ident,
    lpad(filerIdent::text, 8, '0')                  as committee_filer_id,
    {{ cf_clean('filerName') }}                     as committee_name,
    (contributorPersentTypeCd = 'ENTITY')           as is_org,
    {{ cf_clean('contributorNameFirst') }}          as first_name,
    cast(null as text)                              as middle_name,
    {{ cf_clean('contributorNameLast') }}           as last_name,
    {{ cf_clean('contributorNameSuffixCd') }}       as suffix,
    {{ cf_clean('contributorNameOrganization') }}   as organization,
    {{ cf_clean('contributorEmployer') }}           as employer,
    {{ cf_clean('contributorOccupation') }}         as occupation,
    {{ cf_clean('contributorJobTitle') }}           as job_title,
    cast(null as text)                              as street_1,
    cast(null as text)                              as street_2,
    {{ cf_clean('contributorStreetCity') }}         as city,
    {{ cf_clean('contributorStreetStateCd') }}      as state,
    {{ cf_clean('contributorStreetPostalCode') }}   as zip_code,
    {{ cf_clean('contributorStreetCountryCd') }}    as country,
    {{ cf_clean('contributorStreetCountyCd') }}     as county,
    cast(null as text)                              as parent_transaction_type,
    cast(null as text)                              as parent_transaction_id,
    cast(null as numeric)                           as parent_amount
from src
