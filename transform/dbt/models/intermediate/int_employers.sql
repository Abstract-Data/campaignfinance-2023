{#
  Distinct employers (from every party's + guarantor's employer text) as ORGANIZATION
  occurrences, so they dedupe into the SAME shared person/entity dimensions as every
  other org — "AT&T the employer" becomes the same entity as "AT&T the contributor".
  EMPLOYED_BY associations then target these. Computes the shared party keys identically.
#}
with emp as (
    select employer, state_id from {{ ref('int_transactions') }}
    where employer is not null and role is not null
    union
    select employer, state_id from {{ ref('int_guarantors') }}
    where employer is not null
),

distinct_emp as (select distinct employer, state_id from emp),

shaped as (
    select
        'EMPLOYER'                                                       as record_type,
        {{ dbt_utils.generate_surrogate_key(['employer', 'state_id']) }} as source_transaction_id,
        state_id,
        cast(null as text)                                              as first_name,
        cast(null as text)                                              as middle_name,
        cast(null as text)                                              as last_name,
        cast(null as text)                                              as suffix,
        employer                                                        as organization,
        cast(null as text)                                              as street_1,
        cast(null as text)                                              as city,
        cast(null as text)                                              as state,
        cast(null as text)                                              as zip_code
    from distinct_emp
)

select *, {{ cf_party_key_columns() }} from shaped
