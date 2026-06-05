-- No transaction may have MORE THAN ONE transaction_person (the phantom-row symptom
-- the ELT eliminates). Zero is allowed: ASSET has no party, and a transaction whose
-- only party has a blank name resolves to no person. >1 would be a phantom.
select t.id, count(tp.id) as person_rows
from {{ ref('unified_transactions') }} t
join {{ ref('unified_transaction_persons') }} tp on tp.transaction_id = t.id
group by t.id
having count(tp.id) > 1
