{#
  Exactly ONE person row per transaction that has an external party (every record
  type except ASSET). role ∈ {CONTRIBUTOR, PAYEE}; the committee is never here.
  Reads the 'party' occurrences from int_parties (guarantors are excluded — they go to
  unified_loan_guarantors). The entity is the party's shared entity (via dim_persons).
#}
select
    row_number() over (order by rp.transaction_nk) as id,
    tk.id        as transaction_id,
    pk.id        as person_id,
    ek.id        as entity_id,
    rp.state_id,
    rp.role,                       -- ::personrole on publish (CONTRIBUTOR | PAYEE)
    rp.amount
from {{ ref('int_parties') }} rp
join {{ ref('transaction_keys') }} tk on tk.nk = rp.transaction_nk
join {{ ref('person_keys') }} pk on pk.nk = rp.person_nk
join {{ ref('dim_persons') }} dp on dp.nk = rp.person_nk
join {{ ref('entity_keys') }} ek on ek.nk = dp.entity_nk
where rp.occurrence_type = 'party'
