{#  CRED detail: payor (party) + recipient (committee). #}
select
    tk.id                 as id,
    tk.id                 as transaction_id,
    payor_ek.id           as payor_entity_id,
    committee_ek.id       as recipient_entity_id,
    t.state_id,
    t.amount,
    t.transaction_date    as credit_date,
    t.description
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
join {{ ref('entity_keys') }} payor_ek on payor_ek.nk = t.person_entity_nk
join {{ ref('entity_keys') }} committee_ek on committee_ek.nk = t.committee_entity_nk
where t.record_type = 'CRED'
