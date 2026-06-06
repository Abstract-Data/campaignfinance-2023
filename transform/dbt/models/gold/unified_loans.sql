{#  LOAN detail: lender (party) + borrower (committee). id = transaction key id so
    loan_guarantors can FK to it. #}
select
    tk.id                 as id,
    tk.id                 as transaction_id,
    lender_ek.id          as lender_entity_id,
    committee_ek.id       as borrower_entity_id,
    t.state_id,
    t.amount,
    t.transaction_date    as loan_date
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
join {{ ref('entity_keys') }} lender_ek on lender_ek.nk = t.person_entity_nk
join {{ ref('entity_keys') }} committee_ek on committee_ek.nk = t.committee_entity_nk
where t.record_type = 'LOAN'
