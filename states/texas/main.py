import pandas as pd

from states.texas.texas import TexasConfigs, TECFileDownloader, TECCategories, TECValidator
from states.texas.validators import TECExpenses, TECFiler, TECContribution
from tqdm import tqdm
from collections import Counter


download = TECFileDownloader()
folder = TECCategories()
expenses = TECValidator()
download.read()

folder.load('expenses')

expenses.validate(records=folder.expenses, validator=TECExpenses)


# filers_passed, filers_failed = [], []
#
# for record in tqdm(folder.filers, desc="Validating Records"):
#     try:
#         r = TECFiler(**record)
#         filers_passed.append(r)
#     except Exception as e:
#         filers_failed.append({'errors': e, 'record': record})
#
# error_report = [{'type': str(f['errors'].errors()[0]['type']), 'msg': f['errors'].errors()[0]['msg']} for f in failed]
#
# filer_errors = pd.DataFrame.from_dict(Counter([str(e) for e in error_report]), orient='index', columns=['count']).rename_axis('error').reset_index()

# expenses = []
# record_count = 0
# for file in folder.expenses:
#     record_count += 1
#     for _record in folder.expenses[file]:
#         expenses.append({record_count: folder.expenses[file][_record]})

# expenses = [folder.expenses[file] for file in folder.expenses]
# folder.combine_files(folder.expenses)

# expenses = TECValidator(folder.expenses)
# expenses.validate()

# expenses = TECReportLoader(folder.expenses)
# expenses.validate()
#
# contributions = TECReportLoader(folder.contributions)
# contributions.validate()

# expense_loader = TECReportLoader(expense_files)
# expense_loader.load_records()


