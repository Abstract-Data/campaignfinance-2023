{#  TRVL detail: traveler is a PERSON (not entity). Parent transaction is denormalized
    (no FK — mirrors _build_travel_detail). #}
select
    tk.id                 as id,
    tk.id                 as transaction_id,
    pk.id                 as traveler_person_id,
    t.state_id,
    t.parent_transaction_type,
    t.parent_transaction_id,
    t.parent_amount,
    t.amount,
    t.transaction_date    as travel_date,
    t.description          as travel_purpose
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
left join {{ ref('person_keys') }} pk on pk.nk = t.person_nk
where t.record_type = 'TRVL'
