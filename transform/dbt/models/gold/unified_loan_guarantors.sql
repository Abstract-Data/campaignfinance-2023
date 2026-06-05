{#
  Loan/debt guarantors, linked to the parent loan/debt AND to the shared entity
  dimension (entity_id) so a guarantor dedupes against every other party. Only
  guarantors whose parent loan/debt actually materialized (both its entity FKs
  resolved) are emitted, so loan_id/debt_id always reference an existing row.
#}
select
    row_number() over (order by g.transaction_nk, g.position) as id,
    ul.id                 as loan_id,
    ud.id                 as debt_id,
    g.position,
    ek.id                 as entity_id,
    g.person_type_raw     as person_type,
    g.organization,
    g.last_name,
    g.first_name,
    g.suffix,
    g.prefix,
    g.city,
    g.state               as state_code,
    g.county,
    g.country,
    g.zip_code            as postal_code,
    g.region
from {{ ref('int_parties') }} g
join {{ ref('transaction_keys') }} tk on tk.nk = g.transaction_nk
left join {{ ref('unified_loans') }} ul on ul.id = tk.id and g.record_type = 'LOAN'
left join {{ ref('unified_debts') }} ud on ud.id = tk.id and g.record_type = 'DEBT'
left join {{ ref('dim_persons') }} dp on dp.nk = g.person_nk
left join {{ ref('entity_keys') }} ek on ek.nk = dp.entity_nk
where g.occurrence_type = 'guarantor'
  and (ul.id is not null or ud.id is not null)
