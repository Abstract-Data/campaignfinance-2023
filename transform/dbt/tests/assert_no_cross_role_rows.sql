-- Each transaction type may only carry its own role: contribution-like types
-- (contribution/loan/debt/pledge/credit) yield a CONTRIBUTOR; outflow types
-- (expenditure/travel) yield a PAYEE. Any mismatch is a PIPELINE_REVIEW Fix-1 phantom.
select tp.id, t.transaction_type, tp.role
from {{ ref('unified_transaction_persons') }} tp
join {{ ref('unified_transactions') }} t on t.id = tp.transaction_id
where (t.transaction_type in ('CONTRIBUTION', 'LOAN', 'DEBT', 'PLEDGE', 'CREDIT') and tp.role <> 'CONTRIBUTOR')
   or (t.transaction_type in ('EXPENDITURE', 'TRAVEL') and tp.role <> 'PAYEE')
