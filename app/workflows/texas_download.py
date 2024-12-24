#! /usr/bin/env python3
from app.states.texas import TECFileDownloader, TECCategory

download = TECFileDownloader()
download.download()

filers = TECCategory("filers")
contributions = TECCategory("contributions")
expenses = TECCategory("expenses")
reports = TECCategory("reports")

filers.read()
filers.validate()
expenses.write_to_csv(
    records=filers.validation.passed,
    validation_status="passed"
).write_to_csv(
    records=filers.validation.failed,
    validation_status="failed"
)

expenses.read()
expenses.validate()
expenses.write_to_csv(
    records=expenses.validation.passed,
    validation_status="passed"
).write_to_csv(
    records=expenses.validation.failed,
    validation_status='failed'
)


contributions.read()
contributions.validate()
contributions.write_to_csv(
    records=contributions.validation.passed, 
    validation_status="passed"
).write_to_csv(
    records=contributions.validation.failed,
    validation_status="failed"
)

reports.read()
reports.validate()
reports.write_to_csv(
    records=reports.validation.passed,
    validation_status="passed"
).write_to_csv(
    records=reports.validation.failed,
    validation_status="failed"
)