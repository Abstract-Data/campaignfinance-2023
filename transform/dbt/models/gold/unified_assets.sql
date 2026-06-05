{#  ASSET detail: committee property; no external party. #}
select
    tk.id                 as id,
    tk.id                 as transaction_id,
    t.committee_filer_id  as committee_id,
    t.state_id,
    t.description
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
where t.record_type = 'ASSET'
