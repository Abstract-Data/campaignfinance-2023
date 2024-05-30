from __future__ import annotations
from pathlib import Path
from typing import ClassVar
from abcs import StateCampaignFinanceConfigClass


class TexasConfigs(StateCampaignFinanceConfigClass):
    STATE: ClassVar[str] = "Texas"
    STATE_ABBREVIATION: ClassVar[str] = "TX"
    TEMP_FOLDER: ClassVar[StateCampaignFinanceConfigClass.TEMP_FOLDER] = Path(__file__).parents[3] / "tmp" / "texas"
    TEMP_FILENAME: ClassVar[StateCampaignFinanceConfigClass.TEMP_FILENAME] = (
            Path.cwd().parent / "tmp" / "texas" / "TEC_CF_CSV.zip")

    # EXPENSE_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.EXPENSE_VALIDATOR]
    # ] = TECExpense
    # EXPENSE_MODEL: ClassVar[Type[SQLModel]] = None
    EXPENSE_FILE_PREFIX: ClassVar[str] = "expend"

    # CONTRIBUTION_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR]
    # ] = TECContribution
    # CONTRIBUTION_MODEL: ClassVar[Type[SQLModel]] = None
    CONTRIBUTION_FILE_PREFIX: ClassVar[str] = "contribs"

    # FILERS_VALIDATOR: ClassVar[
    #     Type[StateCampaignFinanceConfigs.FILERS_VALIDATOR]
    # ] = TECFiler
    # FILERS_MODEL: ClassVar[Type[SQLModel]] = None
    FILERS_FILE_PREFIX: ClassVar[str] = "filer"

    REPORTS_FILE_PREFIX: ClassVar[str] = "finals"
    TRAVEL_FILE_PREFIX: ClassVar[str] = "travel"
    CANDIDATE_FILE_PREFIX: ClassVar[str] = "cand"
    DEBT_FILE_PREFIX: ClassVar[str] = "debts"
    LOAN_FILE_PREFIX: ClassVar[str] = "loans"

    STATE_CAMPAIGN_FINANCE_AGENCY: ClassVar[str] = "TEC"
    ZIPFILE_URL: ClassVar[
        str
    ] = "https://ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip"

    VENDOR_NAME_COLUMN: ClassVar[str] = "payeeCompanyName"
    FILER_NAME_COLUMN: ClassVar[str] = "filerNameFormatted"

    PAYMENT_RECEIVED_DATE_COLUMN: ClassVar[str] = "receivedDt"
    EXPENDITURE_DATE_COLUMN: ClassVar[str] = "expendDt"
    CONTRIBUTION_DATE_COLUMN: ClassVar[str] = "contributionDt"

    EXPENDITURE_AMOUNT_COLUMN: ClassVar[str] = "expendAmount"