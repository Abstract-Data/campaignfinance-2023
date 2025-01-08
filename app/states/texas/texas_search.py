from states.texas.texas_database import SessionLocal
import pandas as pd
from states.texas.texas import TECFilerRecord, TECExpenseRecord, TECContributionRecord
from dataclasses import dataclass, field
from typing import ClassVar, Union
from nameparser import HumanName
from matplotlib import pyplot as plt

@dataclass
class TECCampaignSearch:
    query: str
    filer_id: int = field(init=False)

    def __post_init__(self):
        self.search_for_campaign()

    def search_for_campaign(self):
        with SessionLocal() as session:
            results = session.query(TECFilerRecord).where(TECFilerRecord.org_names.ilike(f'%%{self.query}%%')).all()
            result_dict = [x.__dict__ for x in results]
            result_list = {x[0]: {
                'Names': x[1]['org_names'],
                'id': x[1]['filerIdent']} for x in enumerate(result_dict, 1)}
            for x in result_list:
                print(f"{x}: {result_list[x]['Names']}")

            _choice = int(input("Choose a filer: "))
            self.filer_id = result_list[_choice]['id']
            return self.filer_id

@dataclass
class TECCriteriaPrompts:
    type_of_search: str = field(init=False)
    campaign_details: TECCampaignSearch = field(init=False)
    person_name: HumanName = None
    organization_name: str = None
    type_model: TECExpenseRecord or TECContributionRecord = field(init=False)
    _fields: dict = field(init=False)

    def ask_contribution_or_expense(self):
        _choice = input("""
        1. Contribution
        2. Expense
        """)
        fields = {}
        if _choice == '1':
            self.type_of_search = 'contribution'
            self.type_model = TECContributionRecord
            fields['name_filer'] = self.type_model.filerName
            fields['name_first'] = self.type_model.contributorNameFirst
            fields['name_last'] = self.type_model.contributorNameLast
            fields['name_organization'] = self.type_model.contributorNameOrganization
            fields['date_field'] = self.type_model.contributionDt
            fields['amount_field'] = self.type_model.contributionAmount
        elif _choice == '2':
            self.type_of_search = 'expense'
            self.type_model = TECExpenseRecord
            fields['name_filer'] = self.type_model.filerName
            fields['name_first'] = self.type_model.payeeNameFirst
            fields['name_last'] = self.type_model.payeeNameLast
            fields['name_organization'] = self.type_model.payeeNameOrganization
            fields['date_field'] = self.type_model.expendDt
            fields['amount_field'] = self.type_model.expendAmount
        else:
            raise ValueError("Invalid choice")
        self._fields = fields
        return self

    def ask_search_type(self):
        _choice = input(f"""Would you like to:
        1. Search all {self.type_of_search.lower()}s for a campaign
        2. Search all {self.type_of_search.lower()}s made by a person to any campaign
        3. Search all {self.type_of_search.lower()}s made by an organization to any campaign
        4. Search {self.type_of_search.lower()}s made by a person to a specific campaign
        5. Search {self.type_of_search.lower()}s made by an organization to a specific campaign
        """)
        if _choice in ['1', '4', '5']:
            self.campaign_details = TECCampaignSearch(input("Enter a campaign name: "))
        if _choice in ['2', '4']:
            _person_name = HumanName(input("Enter a person's name: "))
            if not _person_name.first and not _person_name.last:
                raise ValueError("Name must have a first and last name")
            self.person_name = _person_name
        if _choice in ['3', '5']:
            self.organization_name = input("Enter an organization's name: ")
        return self


@dataclass
class TECResults:
    results: list

    def print_results(self):
        for x in self.results:
            print(x)

    def to_dataframe(self):
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        return pd.DataFrame.from_records([x.__dict__ for x in self.results]).drop(columns=['_sa_instance_state'])

    def by_year(self, **kwargs):
        _campaign = kwargs.get('name_filer').name
        _date_col = kwargs.get('date_field').name
        _amount_col = kwargs.get('amount_field').name

        _df = self.to_dataframe()
        _df[_date_col] = pd.to_datetime(_df[_date_col])

        _df['year'] = _df[_date_col].dt.year
        _df['quarter'] = _df[_date_col].dt.quarter

        if kwargs.get('name_first') and kwargs.get('name_last'):
            _crosstabs = pd.crosstab(index=_df[_campaign], columns=_df['year'], values=_df[_amount_col], aggfunc='sum', margins=True)
        else:
            _crosstabs = pd.crosstab(index=_df[_campaign], columns=_df['year'], values=_df[_amount_col], aggfunc='sum', margins=True)
        # Change sum values to dollar amounts
        _crosstabs = _crosstabs.map(lambda x: f"${x:,.2f}")
        print(_crosstabs)
        return _crosstabs

@dataclass
class TECSearchQuery:
    data: TECCriteriaPrompts = field(default_factory=TECCriteriaPrompts)
    __session: SessionLocal = SessionLocal

    def __post_init__(self):
        self.data.ask_contribution_or_expense().ask_search_type()

    def search(self):

        with self.__session() as session:
            query = session.query(self.data.type_model)
            if self.data.campaign_details:
                query = query.where(self.data.type_model.filerIdent == self.data.campaign_details.filer_id)
            else:
                pass
            if self.data.person_name:
                query = query.where(self.data._fields['name_first'].ilike(f"%%{self.data.person_name.first}%%"))
                query = query.where(self.data._fields['name_last'].ilike(self.data.person_name.last))
            else:
                pass

            if self.data.organization_name:
                query = query.where(self.data._fields['name_organization'].ilike(f"%%{self.data.organization_name}%%"))
            else:
                pass

            return query.all()


# 84766

# @dataclass
# class TECContributionSearch:
#     model: ClassVar[TECContributionRecord] = TECContributionRecord
#     campaign_id: TECFilerSearch = TECFilerSearch()
#
#     def create_campaign_filter(self, session: SessionLocal = SessionLocal) -> SessionLocal:
#         return session.query(self.model).where(self.model.filerIdent == self.campaign_id.filer_id)
#
#     def search_by_name(self, name: str, campaign_filter: SessionLocal = create_campaign_filter):
#         _name = HumanName(name)
#         if not _name.first and not _name.last:
#             raise ValueError("Name must have a first and last name")
#         return campaign_filter.where(
#             self.model.contributorNameFirst.ilike(_name.first)
#             .where(self.model.contributorNameLast.ilike(_name.last)))

# with SessionLocal() as session:
#     _all_contributions = session.query(TECContributionRecord).where(TECContributionRecord.filerIdent == search_test.filer_id)
#     _name = _all_contributions.where(TECContributionRecord.contributorNameLast == 'DUNN')
#     contribution_results = [x.__dict__ for x in _name.all()]
