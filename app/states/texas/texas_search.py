import datetime

from states.texas.database import SessionLocal
import pandas as pd
from states.texas.models import TECFilerRecord, TECExpenseRecord, TECContributionRecord
from dataclasses import dataclass, field
from typing import ClassVar, Union, Optional, Type
from nameparser import HumanName
from matplotlib import pyplot as plt
from collections import namedtuple
from pathlib import Path
from sqlalchemy import and_

TableFields = namedtuple(
    "QueryFields",
    [
        "filerId",
        "name_filer",
        "name_first",
        "name_last",
        "name_organization",
        "date_field",
        "amount_field",
    ],
)


# @dataclass
# class TECCriteriaPrompts:
#     campaign: str = None
#     person_name: HumanName = None
#     organization_name: str = None
#     type_model: [TECExpenseRecord, TECContributionRecord] = field(init=False)
#     _fields: TableFields = field(init=False)
#
#     def __str__(self):
#         return "{search_type} search for {search_criteria}".format(
#             search_type=self.type_of_search,
#             search_criteria=self.campaign_name if
#             self.campaign_name else self.person_name.full_name if
#             self.person_name.full_name else self.organization_name)
#
#     def ask_contribution_or_expense(self):
#         # Choose contribution or expense
#         _choice = input(
#             """
#         1. Contribution
#         2. Expense
#         """
#         )
#         fields = {}
#         if _choice == "1":
#             # Contribution search fields
#             self.type_of_search = "contribution"
#             self.type_model = TECContributionRecord
#             fields = QueryFields(
#                 self.type_model.filerIdent,
#                 self.type_model.filerName,
#                 self.type_model,
#                 self.type_model.contributorNameFirst,
#                 self.type_model.contributorNameLast,
#                 self.type_model.contributorNameOrganization,
#                 self.type_model.contributionDt,
#                 self.type_model.contributionAmount,
#             )
#         elif _choice == "2":
#             # Expense search fields
#             self.type_of_search = "expense"
#             self.type_model = TECExpenseRecord
#             fields = QueryFields(
#                 self.type_model.filerIdent,
#                 self.type_model.filerName,
#                 self.type_model,
#                 self.type_model.payeeNameFirst,
#                 self.type_model.payeeNameLast,
#                 self.type_model.payeeNameOrganization,
#                 self.type_model.expendDt,
#                 self.type_model.expendAmount,
#             )
#         else:
#             raise ValueError("Invalid choice")
#         self._fields = fields
#         return self
#
#     def ask_search_type(self):
#         if self.type_of_search == "contribution":
#             _for = "by"
#             _from = "to"
#         elif self.type_of_search == "expense":
#             _for = "to"
#             _from = "by"
#         else:
#             raise ValueError("Invalid choice")
#         _choice = input(
#             f"""Would you like to:
#         1. Search all {self.type_of_search.lower()}s {_for} a campaign
#         2. Search all {self.type_of_search.lower()}s made {_for} a person {_from} any campaign
#         3. Search all {self.type_of_search.lower()}s made {_for} an organization {_from} any campaign
#         4. Search {self.type_of_search.lower()}s made {_for} a person {_from} a specific campaign
#         5. Search {self.type_of_search.lower()}s made {_for} an organization {_from} a specific campaign
#         """
#         )
#         if _choice in ["1", "4", "5"]:
#             _enter_campaign_name = input("Enter a campaign name: ")
#             self.campaign_name = _enter_campaign_name
#         if _choice in ["2", "4"]:
#             _enter_person_name = input("Enter a person's name: ")
#             self.person_name = HumanName(_enter_person_name)
#         if _choice in ["3", "5"]:
#             self.organization_name = input("Enter an organization's name: ")
#         return self


@dataclass
class TECCampaignLookup:
    filer_name: str = None
    filer_id: int = field(init=False)
    __session: SessionLocal = SessionLocal

    def search(self):
        if self.filer_name:
            with self.__session() as session:
                results = (
                    session.query(TECFilerRecord)
                    .where(
                        TECFilerRecord.org_names.ilike(
                            f"%%{self.filer_name}%%"
                        )
                    )
                    .all()
                )
                result_dict = [x.__dict__ for x in results]
                result_list = {
                    x[0]: {"Names": x[1]["org_names"], "id": x[1]["filerIdent"]}
                    for x in enumerate(result_dict, 1)
                }
                for x in result_list:
                    print(f"{x}: {result_list[x]['Names']}")

                _choice = int(input("Choose a filer: "))
                self.filer_id = result_list[_choice]["id"]
                return self.filer_id



@dataclass
class TECPersonOrgLookup:

    @staticmethod
    def to_from_person(name) -> HumanName:
        return HumanName(name)

    @staticmethod
    def to_from_org(name) -> str:
        return name


@dataclass
class TECQueryBuilder:
    filer_id: int = None
    name: Union[HumanName, str] = None
    _query: str = field(init=False)
    _sql_model: TECExpenseRecord or TECContributionRecord = None

    def contribution(self):
        self._sql_model = TECContributionRecord
        return self

    def expense(self):
        self._sql_model = TECExpenseRecord
        return self

    def campaign(self, name: str):
        self.filer_id = TECCampaignLookup(name).search()
        return self

    def person(self, name: str):
        self.name = TECPersonOrgLookup.to_from_person(name)
        return self

    def organization(self, name: str):
        self.name = TECPersonOrgLookup.to_from_org(name)
        return self

    def search(self):
        model = self._sql_model
        name = self.name
        filer_id = self.filer_id
        _query = None

        if all([isinstance(name, HumanName), filer_id]):
            if model.__name__ == 'TECContributionRecord':
                _query = and_(
                    model.filerIdent == filer_id,
                    model.contributorNameFirst.ilike(name.first),
                    model.contributorNameLast.ilike(name.last)
                )
            elif model.__name__ == 'TECExpenseRecord':
                _query = and_(
                    model.filerIdent == filer_id,
                    model.payeeNameFirst.ilike(name.first),
                    model.payeeNameLast.ilike(name.last)
                )
        elif all([isinstance(name, str), filer_id]):
            if model.__name__ == 'TECContributionRecord':
                _query = and_(
                    model.filerIdent == filer_id,
                    model.contributorNameOrganization.ilike(name)
                )
            elif model.__name__ == 'TECExpenseRecord':
                _query = and_(
                    model.filerIdent == filer_id,
                    model.payeeNameOrganization.ilike(name)
                )

        elif all([isinstance(name, HumanName), not filer_id]):
            if model.__name__ == 'TECContributionRecord':
                _query = and_(
                    model.contributorNameFirst.ilike(name.first),
                    model.contributorNameLast.ilike(name.last)
                )
            elif model.__name__ == 'TECExpenseRecord':
                _query = and_(
                    model.payeeNameFirst.ilike(name.first),
                    model.payeeNameLast.ilike(name.last)
                )
        elif all([isinstance(name, str), not filer_id]):
            if model.__name__ == 'TECContributionRecord':
                _query = model.contributorNameOrganization.ilike(name)
            elif model.__name__ == 'TECExpenseRecord':
                _query = model.payeeNameOrganization.ilike(name)
        elif all([not name, filer_id]):
            _query = model.filerIdent == filer_id
        else:
            raise ValueError("Invalid choice")

        with SessionLocal() as session:
            self._query = _query
            _results = session.query(self._sql_model).filter(_query).all()
            self.results = TECResults([x.__dict__ for x in _results])

        return self

    def to_dataframe(self):
        return self.results.to_dataframe()

    def by_year(self):
        return self.results.by_year(self._generate_fields())

    def _generate_fields(self):
        if self._sql_model.__name__ == 'TECContributionRecord':
            _fields = TableFields(
                filerId=self._sql_model.filerIdent,
                name_filer=self._sql_model.filerName,
                name_first=self._sql_model.contributorNameFirst,
                name_last=self._sql_model.contributorNameLast,
                name_organization=self._sql_model.contributorNameOrganization,
                date_field=self._sql_model.contributionDt,
                amount_field=self._sql_model.contributionAmount,
            )
        elif self._sql_model.__name__ == 'TECExpenseRecord':
            _fields = TableFields(
                filerId=self._sql_model.filerIdent,
                name_filer=self._sql_model.filerName,
                name_first=self._sql_model.payeeNameFirst,
                name_last=self._sql_model.payeeNameLast,
                name_organization=self._sql_model.payeeNameOrganization,
                date_field=self._sql_model.expendDt,
                amount_field=self._sql_model.expendAmount,
            )
        else:
            raise ValueError("Invalid choice")
        return _fields



@dataclass
class TECResults:
    results: list

    def print_results(self):
        for x in self.results:
            print(x)
        return self

    def group_orgs(self):
        _filer_ids = list(set(x['filerIdent'] for x in self.results))
        org_names = {}
        for _id in _filer_ids:
            org_names.update({_id: ', '.join(list(set([x['filerName'] for x in self.results if x['filerIdent'] == _id])))})

        for x in self.results:
            for _id, _names in org_names.items():
                if x['filerIdent'] == _id:
                    x['org_names'] = _names
        return self


    def to_dataframe(self):
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)
        self.group_orgs()
        _df = pd.DataFrame.from_records([x for x in self.results])
        if "_sa_instance_state" in _df.columns:
            _df = _df.drop(columns=["_sa_instance_state"])
        return _df

    def by_year(self, fields: TableFields):
        _campaign = 'org_names'
        _date_col = fields.date_field.name
        _amount_col = fields.amount_field.name
        _filer_id = fields.filerId.name
        _organization = fields.name_organization.name


        _df = self.to_dataframe()
        _df[_date_col] = pd.to_datetime(_df[_date_col])

        _df["year"] = _df[_date_col].dt.year
        _df["quarter"] = _df[_date_col].dt.quarter

        # if kwargs.get("name_organization"):
        #     _idx = [_df[_campaign], _df[_filer_id]]
        #     _year = [_df["year"], _df[_organization]]
        # else:
        _idx = [_df[_campaign], _df[_filer_id]]
        _year = [_df["year"]]

        # if kwargs.get("name_first") and kwargs.get("name_last"):
        #     _crosstabs = pd.crosstab(
        #         index=[_df[_campaign], _df[_filer_id]],
        #         columns=_df["year"],
        #         values=_df[_amount_col],
        #         aggfunc="sum",
        #         margins=True,
        #     )
        # else:
        _crosstabs = pd.crosstab(
            index=_idx,
            columns=_year,
            values=_df[_amount_col],
            aggfunc="sum",
            margins=True,
        )
        # Change sum values to dollar amounts
        _crosstabs = _crosstabs.fillna(0)
        _crosstabs = _crosstabs.map(lambda x: f"${x:,.2f}")
        return _crosstabs

    def export(self, df: pd.DataFrame, type_file:str, query: str):
        path = Path.home() / "Downloads"
        extension = str(datetime.datetime.now().strftime("%Y%m%d")) + '_' + type_file.lower() + '_' + query.lower() + '.csv'
        full_path = path / extension
        df.to_csv(full_path, index=False)


# @dataclass
# class TECSearchQuery:
#     prompt: TECCriteriaPrompts = field(default_factory=TECCriteriaPrompts)
#     data: TECCampaignLookup = field(init=False)
#     results: TECResults = field(init=False)
#     __session: SessionLocal = SessionLocal
#
#     def __repr__(self):
#         return self.prompt.__repr__()
#
#     @property
#     def fields(self):
#         return self.prompt._fields
#
#     def __post_init__(self):
#
#         self.prompt.ask_contribution_or_expense().ask_search_type()
#         self.data = TECCampaignLookup(self.prompt)
#         self.data.campaign_lookup()
#         self.results = TECResults(self.data.search())
#         self.results.group_orgs()
#
#     def by_year(self):
#         return self.results.by_year(**self.fields._asdict())
#
#     def export_file(self, df: pd.DataFrame, type_file: str, query: str):
#         _type_file = self.prompt.type_of_search if not type_file else type_file
#         _query = self.data.org_names if not query else query.replace(' ', '_')
#         self.results.export(df, _type_file, _query)


    # def search(self):
    #
    #     with self.__session() as session:
    #         query = session.query(self.data.type_model)
    #         if self.data.campaign_details:
    #             query = query.where(self.data.type_model.filerIdent == self.data.campaign_details.filer_id)
    #         else:
    #             pass
    #         if self.data.person_name:
    #             query = query.where(self.data._fields['name_first'].ilike(f"%%{self.data.person_name.first}%%"))
    #             query = query.where(self.data._fields['name_last'].ilike(self.data.person_name.last))
    #         else:
    #             pass
    #
    #         if self.data.organization_name:
    #             query = query.where(self.data._fields['name_organization'].ilike(f"%%{self.data.organization_name}%%"))
    #         else:
    #             pass
    #
    #         return query.all()

    # def group_by_filerid(self):
    #     _results = self.search()
    #     _df = pd.DataFrame.from_records([x.__dict__ for x in _results]).drop(columns=['_sa_instance_state'])
    #     _filer_ids = _df['filerIdent'].unique().tolist()
    #     with self.__session() as session:
    #         _org_name_list = session.query(TECFilerRecord).where(TECFilerRecord.filerIdent.in_(_filer_ids)).all()
    #         _org_name_df = pd.DataFrame.from_records([x.__dict__ for x in _org_name_list]).drop(columns=['_sa_instance_state'])
    #         _org_name_df = _org_name_df[['filerIdent', 'org_names']]
    #         _df = _df.merge(_org_name_df, on='filerIdent', how='left')
    #         self.data._fields['filerId'] = TECFilerRecord.filerIdent
    #         self.data._fields['org_names'] = TECFilerRecord.org_names
    #     return _df.to_dict('records')

#
# viewer = TECResults(results)
# viewer.print_results()
# by_year = viewer.by_year(**search.data._fields)


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
