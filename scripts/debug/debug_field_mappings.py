import sys

sys.path.insert(0, '/Users/johneakin/PyCharmProjects/campaignfinance')

# Must import processor first — it has the side-effect import of UnifiedReport
# that SQLAlchemy needs to configure the UnifiedTransaction mapper
from app.core.processor import unified_sql_processor  # noqa: F401
from app.core.unified_field_library import UnifiedFieldLibrary

lib = UnifiedFieldLibrary()
mappings = lib.get_state_mappings('texas')
print('Texas mappings count:', len(mappings))
field_map = {m.state_field: m.unified_field for m in mappings}
td_fields = [(k,v) for k,v in field_map.items() if v == 'transaction_date']
fid_fields = [(k,v) for k,v in field_map.items() if v == 'committee_filer_id']
amt_fields = [(k,v) for k,v in field_map.items() if v == 'amount']
print('transaction_date mappings:', td_fields)
print('committee_filer_id mappings:', fid_fields)
print('amount mappings:', amt_fields)

raw_data = {
    'recordType': 'RCPT',
    'contributionDt': '20130629',
    'contributionAmount': '500.00',
    'contributionInfoId': '100042122',
    'filerIdent': '00015447',
    'filerName': 'Test Committee',
    'filerTypeCd': 'COH',
    'contributorNameFirst': 'John',
    'contributorNameLast': 'Smith',
}

def get_field_value(raw_data, unified_field, field_mappings):
    if unified_field in raw_data:
        return raw_data[unified_field]
    for state_field, mapped_field in field_mappings.items():
        if mapped_field == unified_field and state_field in raw_data:
            return raw_data[state_field]
    return None

print('--- Simulated _get_field_value ---')
print('transaction_date:', get_field_value(raw_data, 'transaction_date', field_map))
print('committee_filer_id:', get_field_value(raw_data, 'committee_filer_id', field_map))
print('amount:', get_field_value(raw_data, 'amount', field_map))

from app.core.builders import UnifiedSQLModelBuilder  # noqa: E402

builder = UnifiedSQLModelBuilder('texas', state_id=1, state_code='TX', session=None)
txn = builder.build_transaction(raw_data)
print('--- build_transaction output ---')
print('transaction_date:', txn.transaction_date)
print('amount:', txn.amount)
print('transaction_type:', txn.transaction_type)

committee = builder.build_committee(raw_data)
print('--- build_committee output ---')
print('filer_id:', committee.filer_id if committee else None)
print('name:', committee.name if committee else None)
