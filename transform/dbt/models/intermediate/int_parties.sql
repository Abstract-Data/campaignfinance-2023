{#
  The unified party-occurrence stream: every person/org that appears anywhere — a
  transaction party, a loan/debt guarantor, an EMPLOYER, or a committee OFFICER — in
  one shape. The shared dimensions dedupe on this, so the SAME real-world party
  collapses to one entity regardless of where it appears. occurrence_type splits the
  downstream consumers:
    party     -> unified_transaction_persons
    guarantor -> unified_loan_guarantors
    officer   -> unified_committee_persons + TREASURER_OF
    employer  -> EMPLOYED_BY target
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
        cast(null as integer) as position, cast(null as text) as committee_filer_id,
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
        loan_source_id, debt_source_id, position, cast(null as text) as committee_filer_id,
        person_type, transaction_nk, person_nk, address_nk
    from {{ ref('int_guarantors') }}
),

employers as (
    select
        record_type, source_transaction_id, cast(null as text) as role, 'employer' as occurrence_type,
        cast(null as numeric) as amount, state_id,
        organization, first_name, middle_name, last_name, suffix,
        cast(null as text) as prefix, cast(null as text) as employer,
        cast(null as text) as occupation, cast(null as text) as job_title,
        street_1, cast(null as text) as street_2, city, state, zip_code,
        cast(null as text) as country, cast(null as text) as county,
        cast(null as text) as region, cast(null as text) as person_type_raw,
        cast(null as text) as loan_source_id, cast(null as text) as debt_source_id,
        cast(null as integer) as position, cast(null as text) as committee_filer_id,
        person_type, transaction_nk, person_nk, address_nk
    from {{ ref('int_employers') }}
),

officers as (
    select
        record_type, source_transaction_id, cast(null as text) as role, 'officer' as occurrence_type,
        cast(null as numeric) as amount, state_id,
        organization, first_name, middle_name, last_name, suffix,
        cast(null as text) as prefix, cast(null as text) as employer,
        cast(null as text) as occupation, cast(null as text) as job_title,
        street_1, cast(null as text) as street_2, city, state, zip_code,
        cast(null as text) as country, cast(null as text) as county,
        cast(null as text) as region, cast(null as text) as person_type_raw,
        cast(null as text) as loan_source_id, cast(null as text) as debt_source_id,
        cast(null as integer) as position, committee_filer_id,
        person_type, transaction_nk, person_nk, address_nk
    from {{ ref('int_officers') }}
)

select * from parties
union all select * from guarantors
union all select * from employers
union all select * from officers
