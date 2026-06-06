{#  DEBT detail: creditor (party, via lender* cols) + debtor (committee). TEC debt
    rows carry no amount/date. id = transaction key id (for guarantor FK). #}
select
    tk.id                 as id,
    tk.id                 as transaction_id,
    creditor_ek.id        as creditor_entity_id,
    committee_ek.id       as debtor_entity_id,
    t.state_id,
    t.amount,
    t.transaction_date    as debt_date,
    t.description
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
join {{ ref('entity_keys') }} creditor_ek on creditor_ek.nk = t.person_entity_nk
join {{ ref('entity_keys') }} committee_ek on committee_ek.nk = t.committee_entity_nk
where t.record_type = 'DEBT'
