from abcs import FileDownloader, StateCategories, StateFileValidation, StateCampaignFinanceConfigs
from typing import Protocol, Callable, List
from db_loaders.postgres_loader import PostgresLoader
from states.texas.database import Base, engine, SessionLocal
from collections import namedtuple


class VoterFileSetup(Protocol):
    download: FileDownloader
    folder: StateCategories
    validation: StateFileValidation
    _db: Callable[[PostgresLoader], Base]
    _db_base: Base
    _engine: engine

    @property
    def db(self):
        return self._db(self._db_base)

    @property
    def validators(self):
        Validators = namedtuple('Validators', ['expenses', 'contributions', 'filers'])
        return Validators(
            expenses=StateCampaignFinanceConfigs.EXPENSE_VALIDATOR,
            contributions=StateCampaignFinanceConfigs.CONTRIBUTION_VALIDATOR,
            filers=StateCampaignFinanceConfigs.FILERS_VALIDATOR
        )

    @property
    def models(self):
        Models = namedtuple('Models', ['expenses', 'contributions', 'filers'])
        return Models(
            expenses=StateCampaignFinanceConfigs.EXPENSE_MODEL,
            contributions=StateCampaignFinanceConfigs.CONTRIBUTION_MODEL,
            filers=StateCampaignFinanceConfigs.FILERS_MODEL
        )

    def load(self, **kwargs):
        if kwargs.get('file_type'):
            self.folder.load(
                kwargs.get('file_type')) if kwargs.get('file_type') in [
                'expenses',
                'contributions',
                'filers'
            ] else self.folder.load()

    def read(self):
        self.download.read()

    def validate(self, file_type: str, validator: validators):
        self.validation.validate(
            records=self.folder.__getattribute__(file_type),
            validator=self.validators[validator]
        )

    def create_models(self, records: List[dict], model: models):
        self.db.create(values=records, table=self.models[model])

    def build_tables(self):
        self.db.build(engine=self._engine)

    def load_records(self):
        self.db.load(session=SessionLocal)
