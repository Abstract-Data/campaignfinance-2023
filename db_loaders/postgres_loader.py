from typing import Protocol, List
from dataclasses import dataclass, field
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from tqdm import tqdm


@dataclass
class PostgresLoader:
    _base: declarative_base
    table: declarative_base = field(init=False)
    models: List[declarative_base] = field(init=False)

    def build(self, engine: create_engine):
        self._base.metadata.create_all(engine)
        print("Created Postgres tables")

    def create(self, values: List[dict], table: declarative_base):
        self.table = table
        self.models = [
            table(**dict(x)) for x in tqdm(
                values,
                desc=f"Creating {table.__tablename__} models"
            )
        ]
        return self.table, self.models

    def load(self, session: sessionmaker):

        def upload_records(records):
            with session() as upload:
                upload.add_all(records)
                upload.commit()

        _queue = []
        for model in tqdm(self.models, desc=f"Loading to {self.table.__tablename__}"):
            _queue.append(model)
            if len(_queue) == 16000:
                upload_records(_queue)
                _queue = []
        upload_records(_queue)


