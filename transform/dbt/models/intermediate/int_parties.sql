{#
  The unified party-occurrence stream: every person/org that appears anywhere — as a
  transaction party (contributor/payee/lender/pledger/payor/traveller/candidate) OR a
  loan/debt guarantor — in one shape. This is what the shared dimensions
  (dim_persons / dim_entities / dim_addresses) dedupe on, so the SAME real-world party
  collapses to one entity regardless of record type or role. occurrence_type splits
  the downstream link tables: 'party' -> unified_transaction_persons,
  'guarantor' -> unified_loan_guarantors.
#}
with parties as (
    select
        record_type, source_transaction_id, role, 'party' as occurrence_type,
        amount, state_id,
        organization, first_name, middle_name, last_name, suffix,
        cast(null as text) as prefix, employer, occupation, job_title,
        street_1, street_2, city, state, zip_code, country, county,
        cast(null as text) as region, cast(null as text) as person_type_raw,
        cast(null as text) as loan_source_id, cast(null as text) as debt_source_id,
        cast(null as integer) as position,
        person_type, transaction_nk, person_nk, address_nk
    from {{ ref('int_transactions') }}
    where role is not null
      and (organization is not null or last_name is not null or first_name is not null)
),

guarantors as (
    select
        record_type, source_transaction_id, cast(null as text) as role, 'guarantor' as occurrence_type,
        cast(null as numeric) as amount, state_id,
        organization, first_name, middle_name, last_name, suffix,
        prefix, employer, occupation, job_title,
        street_1, street_2, city, state, zip_code, country, county,
        region, person_type_raw,
        loan_source_id, debt_source_id, position,
        person_type, transaction_nk, person_nk, address_nk
    from {{ ref('int_guarantors') }}
)

select * from parties
union all
select * from guarantors
