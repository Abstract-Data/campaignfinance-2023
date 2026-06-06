{#
  Committee master. The FILER file (stg_tx_filers) is authoritative for name / type
  / status; any filer_id seen on a transaction but absent from FILER is still
  included (name from the transaction row) so every transactions.committee_id FK
  resolves. filer_id is the canonical zero-padded 8-char TEC id on both sides.
#}

with txn_committees as (
    select committee_filer_id as filer_id, committee_name, state_id
    from {{ ref('stg_tx_contributions') }}
    where committee_filer_id is not null
    union all
    select committee_filer_id, committee_name, state_id
    from {{ ref('stg_tx_expenditures') }}
    where committee_filer_id is not null
),

txn_dedup as (
    select filer_id, state_id, name from (
        select
            filer_id,
            state_id,
            committee_name as name,
            row_number() over (partition by filer_id order by committee_name nulls last) as rn
        from txn_committees
    ) ranked
    where rn = 1
),

filer_master as (
    select filer_id, state_id, name, committee_type, filer_status, organization
    from {{ ref('stg_tx_filers') }}
),

all_ids as (
    select filer_id from filer_master
    union
    select filer_id from txn_dedup
),

merged as (
    select
        a.filer_id,
        coalesce(fm.state_id, td.state_id)              as state_id,
        coalesce(fm.name, fm.organization, td.name)     as name,
        fm.committee_type,
        fm.filer_status
    from all_ids a
    left join filer_master fm on fm.filer_id = a.filer_id
    left join txn_dedup td on td.filer_id = a.filer_id
),

norm as (
    select
        filer_id,
        state_id,
        name,
        committee_type,
        filer_status,
        'COMMITTEE'                       as entity_type,
        {{ cf_normalize_name('name') }}   as normalized_name
    from merged
)

select
    *,
    {{ dbt_utils.generate_surrogate_key(["'COMMITTEE'", 'normalized_name', 'state_id']) }} as entity_nk
from norm
