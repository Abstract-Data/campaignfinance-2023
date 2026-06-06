{#
  Committee treasurers from the FILER master (one per committee with a treasurer name).
  Officers dedupe into the SAME shared person/entity dimensions as every other party,
  and drive unified_committee_persons + the TREASURER_OF associations.
#}
with shaped as (
    select
        'OFFICER'                                   as record_type,
        lpad(filerIdent::text, 8, '0')              as source_transaction_id,
        {{ var('state_id') }}                       as state_id,
        lpad(filerIdent::text, 8, '0')              as committee_filer_id,
        {{ cf_clean('treasNameFirst') }}            as first_name,
        cast(null as text)                          as middle_name,
        {{ cf_clean('treasNameLast') }}             as last_name,
        {{ cf_clean('treasNameSuffixCd') }}         as suffix,
        {{ cf_clean('treasNameOrganization') }}     as organization,
        {{ cf_clean('treasStreetAddr1') }}          as street_1,
        {{ cf_clean('treasStreetCity') }}           as city,
        {{ cf_clean('treasStreetStateCd') }}        as state,
        {{ cf_clean('treasStreetPostalCode') }}     as zip_code
    from {{ source('silver', 'tx_filers') }}
    where {{ cf_clean('treasNameLast') }} is not null
       or {{ cf_clean('treasNameOrganization') }} is not null
)

select *, {{ cf_party_key_columns() }} from shaped
