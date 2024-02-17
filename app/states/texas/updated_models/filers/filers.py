from typing import Optional, List, Annotated
from sqlalchemy import ForeignKey, String, Date
from sqlalchemy.orm import Mapped, mapped_column, relationship
from ...database import Base


class FilerModel(Base):
    __tablename__ = 'filers'
    __table_args__ = {"schema": 'texas'}
    filerIdent: Mapped[int] = mapped_column(primary_key=True)
    filerTypeCd: Mapped[str]
    filerName = Annotated[int, mapped_column(ForeignKey('filer_names.filerIdent', use_alter=True))]
