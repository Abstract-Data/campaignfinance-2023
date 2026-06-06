{#
  EXPN detail. payer_entity_id = the committee's entity; payee_entity_id = the payee
  person's entity.
#}

select
    row_number() over (order by t.nk) as id,
    tk.id                 as transaction_id,
    committee_ek.id       as payer_entity_id,
    payee_ek.id           as payee_entity_id,
    t.state_id,
    t.amount,
    t.transaction_date    as expenditure_date,
    cast(null as text)    as expenditure_type,
    t.description
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
left join {{ ref('entity_keys') }} committee_ek on committee_ek.nk = t.committee_entity_nk
join {{ ref('entity_keys') }} payee_ek on payee_ek.nk = t.person_entity_nk
where t.record_type in ('EXPN', 'CAND')   -- CAND = direct expenditure to a candidate
