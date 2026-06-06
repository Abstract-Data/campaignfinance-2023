{#
  RCPT detail. contributor_entity_id = the contributor person's entity;
  recipient_entity_id = the committee's entity (the Fix 2 correction — the committee
  is the recipient, never a self-contribution).
#}

select
    row_number() over (order by t.nk) as id,
    tk.id                 as transaction_id,
    contributor_ek.id     as contributor_entity_id,
    committee_ek.id       as recipient_entity_id,
    t.state_id,
    t.amount,
    t.transaction_date    as receipt_date,
    cast(null as text)    as contribution_type,
    t.description
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
join {{ ref('entity_keys') }} contributor_ek on contributor_ek.nk = t.person_entity_nk
left join {{ ref('entity_keys') }} committee_ek on committee_ek.nk = t.committee_entity_nk
where t.record_type = 'RCPT'
