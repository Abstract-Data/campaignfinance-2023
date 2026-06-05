select
    tk.id,
    t.transaction_id,              -- source id (NOT globally unique)
    t.amount,
    t.transaction_date,
    t.description,
    t.transaction_type,            -- ::transactiontype on publish
    t.committee_filer_id as committee_id,
    cast(null as integer) as campaign_id,   -- campaigns out of PoC scope
    t.state_id,
    t.report_ident
from {{ ref('dim_transactions') }} t
join {{ ref('transaction_keys') }} tk on tk.nk = t.nk
