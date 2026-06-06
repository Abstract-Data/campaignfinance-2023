{#  PLDG detail: pledgor (party) + recipient (committee). #}
select
    tk.id                 as id,
    tk.id                 as transaction_id,
    pledgor_ek.id         as pledgor_entity_id,
    committee_ek.id       as recipient_entity_id,
    t.state_id,
    t.amount,
    t.transaction_date    as pledge_date,
    t.description
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
join {{ ref('entity_keys') }} pledgor_ek on pledgor_ek.nk = t.person_entity_nk
join {{ ref('entity_keys') }} committee_ek on committee_ek.nk = t.committee_entity_nk
where t.record_type = 'PLDG'
