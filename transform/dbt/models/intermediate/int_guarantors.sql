{#
  Explode loan/debt guarantors (1-5 slots) into one party row each, so guarantors
  dedupe into the SAME shared person/entity/address dimensions as every other party.
  Carries the parent loan/debt source id + slot position for the loan_guarantors link.
  Computes the shared party keys identically to the transaction parties.
#}
{% set sources = [
    ('tx_loans', 'LOAN', true),
    ('tx_debts', 'DEBT', false)
] %}
{% set ns = namespace(first=true) %}

with raw as (
    {% for tbl, kind, has_employer in sources %}
    {% for i in range(1, 6) %}
    {% if not ns.first %}union all{% endif %}{% set ns.first = false %}
    select
        '{{ kind }}'                                       as record_type,
        loanInfoId::text                                   as source_transaction_id,
        {{ var('state_id') }}                              as state_id,
        {{ i }}                                            as position,
        {% if kind == 'LOAN' %}loanInfoId::text{% else %}cast(null as text){% endif %} as loan_source_id,
        {% if kind == 'DEBT' %}loanInfoId::text{% else %}cast(null as text){% endif %} as debt_source_id,
        {{ cf_clean('guarantorPersentTypeCd' ~ i) }}       as person_type_raw,
        {{ cf_clean('guarantorNameFirst' ~ i) }}           as first_name,
        cast(null as text)                                 as middle_name,
        {{ cf_clean('guarantorNameLast' ~ i) }}            as last_name,
        {{ cf_clean('guarantorNameSuffixCd' ~ i) }}        as suffix,
        {{ cf_clean('guarantorNamePrefixCd' ~ i) }}        as prefix,
        {{ cf_clean('guarantorNameOrganization' ~ i) }}    as organization,
        {% if has_employer %}{{ cf_clean('guarantorEmployer' ~ i) }}{% else %}cast(null as text){% endif %} as employer,
        {% if has_employer %}{{ cf_clean('guarantorOccupation' ~ i) }}{% else %}cast(null as text){% endif %} as occupation,
        {% if has_employer %}{{ cf_clean('guarantorJobTitle' ~ i) }}{% else %}cast(null as text){% endif %} as job_title,
        cast(null as text)                                 as street_1,
        cast(null as text)                                 as street_2,
        {{ cf_clean('guarantorStreetCity' ~ i) }}          as city,
        {{ cf_clean('guarantorStreetStateCd' ~ i) }}       as state,
        {{ cf_clean('guarantorStreetPostalCode' ~ i) }}    as zip_code,
        {{ cf_clean('guarantorStreetCountryCd' ~ i) }}     as country,
        {{ cf_clean('guarantorStreetCountyCd' ~ i) }}      as county,
        {{ cf_clean('guarantorStreetRegion' ~ i) }}        as region
    from {{ source('silver', tbl) }}
    where {{ cf_clean('guarantorNameLast' ~ i) }} is not null
       or {{ cf_clean('guarantorNameOrganization' ~ i) }} is not null
       or {{ cf_clean('guarantorNameFirst' ~ i) }} is not null
    {% endfor %}
    {% endfor %}
)

select
    *,
    {{ cf_party_key_columns() }}
from raw
