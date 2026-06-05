{#
  One row per transaction natural key across ALL record types (record_type +
  source_transaction_id; NOT globally unique, so record_type is in the nk). Carries
  the party entity nk (via dim_persons, so it matches the entity assigned everywhere)
  and the committee entity nk for the per-type detail FKs. ASSET rows have no party
  (person_entity_nk null). DISTINCT ON guards against any duplicate source id.
#}
select distinct on (t.transaction_nk)
    t.transaction_nk         as nk,
    t.record_type,
    t.source_transaction_id  as transaction_id,
    t.transaction_type,
    t.amount,
    t.transaction_date,
    t.description,
    t.report_ident,
    t.committee_filer_id,
    t.state_id,
    t.person_nk,
    dp.entity_nk             as person_entity_nk,
    c.entity_nk              as committee_entity_nk,
    t.parent_transaction_type,
    t.parent_transaction_id,
    t.parent_amount
from {{ ref('int_transactions') }} t
left join {{ ref('dim_persons') }} dp on dp.nk = t.person_nk and t.role is not null
left join {{ ref('int_committees') }} c on c.filer_id = t.committee_filer_id
order by t.transaction_nk
