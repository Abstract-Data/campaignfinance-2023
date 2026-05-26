"""Four-level Base / Create / Read / Table hierarchy for state validator models.

Wave 4 (TASK-4e) introduces this split; not every state validator is migrated yet.

Still single-class (migrate in Wave 5):
  Texas: TECExpense, TECFiler, TECFinalReport, TECTravelData, CandidateData, DebtData,
         TECFilerLink, TECTreasurerLink, TECFilerName, TECTreasurer, TECPersonName, TECAddress
  Oklahoma: OklahomaContribution, OklahomaLobbyistExpenditure

Migrated (four-level):
  Texas: TECContribution (Base/Create/Read/Table)
  Oklahoma: OklahomaExpenditure (Base/Create/Read/Table)
"""

from __future__ import annotations

from datetime import datetime

from pydantic import ConfigDict
from sqlmodel import SQLModel


class BaseValidatorModel(SQLModel):
    """Shared config and common validators for all state validator models.

    Not a Table model — no database mapping here.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        use_enum_values=True,
        extra="ignore",
    )


class CreateValidatorModel(BaseValidatorModel):
    """Fields accepted when creating/ingesting a new record.

    Excludes server-set fields (id, created_at, updated_at).
    """


class ReadValidatorModel(BaseValidatorModel):
    """Fields returned to API consumers or downstream consumers.

    May add computed/derived fields not present in Create.
    """

    id: int
    created_at: datetime


class TableValidatorModel(BaseValidatorModel):
    """SQLModel Table base — subclasses set ``table=True`` and ``__tablename__``.

    Server-set columns (``id``, timestamps) belong on the Table class, not Create.
    Subclasses define concrete column types to match existing database schemas.
    """

    __abstract__ = True
