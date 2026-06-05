{#
  Every transaction across ALL record types (one row per record_type +
  source_transaction_id), with the shared party/transaction/address keys. ASSET rows
  have no party (role null). dim_transactions reads this; int_parties derives the
  person-bearing subset for the shared dimensions.
#}
with unioned as (
    select * from {{ ref('stg_tx_contributions') }}
    union all select * from {{ ref('stg_tx_expenditures') }}
    union all select * from {{ ref('stg_tx_loans') }}
    union all select * from {{ ref('stg_tx_debts') }}
    union all select * from {{ ref('stg_tx_pledges') }}
    union all select * from {{ ref('stg_tx_credits') }}
    union all select * from {{ ref('stg_tx_travel') }}
    union all select * from {{ ref('stg_tx_candidate') }}
    union all select * from {{ ref('stg_tx_assets') }}
)

select
    *,
    {{ cf_party_key_columns() }}
from unioned
