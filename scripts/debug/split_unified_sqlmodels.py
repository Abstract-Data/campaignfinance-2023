"""One-off mechanical split of unified_sqlmodels.py (TASK-3a)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "app/core/unified_sqlmodels.py"
lines = SRC.read_text().splitlines(keepends=True)


def class_line_index(name: str) -> int:
    for i, line in enumerate(lines):
        if line.startswith(f"class {name}"):
            return i
    raise KeyError(name)


def class_names() -> list[str]:
    out: list[str] = []
    for line in lines:
        if line.startswith("class "):
            out.append(line.split("(")[0].split()[-1].rstrip(":"))
    return out


markers = {name: class_line_index(name) for name in class_names()}

enum_start = markers["TransactionType"]
state_start = markers["State"]
indexes_start = markers["UnifiedTransactionIndexes"]
builder_start = markers["UnifiedSQLModelBuilder"]
processor_start = markers["UnifiedSQLDataProcessor"]

enum_block = lines[enum_start:state_start]
indexes_block = lines[indexes_start:builder_start]
builder_block = lines[builder_start:processor_start]
processor_block = lines[processor_start:]

model_groups = {
    "reference": ["State", "FileOrigin"],
    "entities": ["UnifiedAddress", "UnifiedPerson", "UnifiedCommittee", "UnifiedEntity"],
    "transactions": [
        "UnifiedTransaction",
        "UnifiedTransactionPerson",
        "UnifiedTransactionVersion",
        "UnifiedPersonVersion",
        "UnifiedCommitteeVersion",
        "UnifiedAddressVersion",
        "UnifiedCommitteePerson",
        "UnifiedCommitteePersonVersion",
        "UnifiedEntityAssociation",
    ],
    "campaigns": [
        "UnifiedCampaign",
        "UnifiedCampaignEntity",
        "UnifiedContribution",
        "UnifiedLoan",
        "UnifiedDebt",
        "UnifiedCredit",
        "UnifiedTravel",
        "UnifiedAsset",
    ],
}

ordered = class_names()
ordered_idx = {n: markers[n] for n in ordered}


def slice_block(names: list[str]) -> str:
    chunks: list[str] = []
    for name in names:
        start = ordered_idx[name]
        pos = ordered.index(name)
        end = ordered_idx[ordered[pos + 1]] if pos + 1 < len(ordered) else indexes_start
        chunks.extend(lines[start:end])
    return "".join(chunks)


MODEL_HEADER = '''"""SQLModel table definitions."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import Column, ForeignKey, Index, Integer, Numeric, String, Text
from sqlmodel import Field, Relationship, SQLModel

from app.core.constants import MONEY_TYPE
from app.core.enums import (
    AssociationType,
    CampaignRole,
    CommitteeRole,
    EntityType,
    PersonRole,
    PersonType,
    TransactionType,
)

'''

models_dir = ROOT / "app/core/models"
models_dir.mkdir(exist_ok=True)

for fname, names in model_groups.items():
    (models_dir / f"{fname}.py").write_text(MODEL_HEADER + slice_block(names))

indexes_header = '''"""Database indexes for unified tables."""

from sqlalchemy import Index

from app.core.models.campaigns import (
    UnifiedAsset,
    UnifiedCampaign,
    UnifiedCampaignEntity,
    UnifiedContribution,
    UnifiedCredit,
    UnifiedDebt,
    UnifiedLoan,
    UnifiedTravel,
)
from app.core.models.entities import (
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)
from app.core.models.transactions import (
    UnifiedTransaction,
    UnifiedTransactionPerson,
)

'''
(models_dir / "indexes.py").write_text(indexes_header + "".join(indexes_block[1:]))

(ROOT / "app/core/enums.py").write_text(
    '"""Campaign finance domain enumerations."""\n\nfrom enum import Enum\n\n' + "".join(enum_block)
)

(ROOT / "app/core/constants.py").write_text(
    '''"""Shared constants and SQLAlchemy types for unified models."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Numeric

from app.core.enums import TransactionType

MONEY_TYPE = Numeric(15, 2)
DEFAULT_STATE: str | None = None

RECORD_TYPE_CODES: frozenset[str] = frozenset(
    {"RCPT", "EXPN", "LOAN", "PLDG", "DEBT", "CRED", "TRVL", "ASSET"}
)

PLACEHOLDER_NAMES: frozenset[str] = frozenset(
    {
        "NON-ITEMIZED CONTRIBUTOR",
        "NON-ITEMIZED",
        "UNKNOWN",
        "ANONYMOUS",
    }
)

AMOUNT_BUCKETS: tuple[tuple[Decimal, Decimal], ...] = (
    (Decimal("0"), Decimal("50")),
    (Decimal("50"), Decimal("200")),
    (Decimal("200"), Decimal("1000")),
    (Decimal("1000"), Decimal("10000")),
    (Decimal("10000"), Decimal("999999999")),
)

RECORD_TYPE_TO_TRANSACTION: dict[str, TransactionType] = {
    "RCPT": TransactionType.CONTRIBUTION,
    "EXPN": TransactionType.EXPENDITURE,
    "LOAN": TransactionType.LOAN,
    "PLDG": TransactionType.PLEDGE,
    "DEBT": TransactionType.DEBT,
    "CRED": TransactionType.CREDIT,
    "TRVL": TransactionType.TRAVEL,
    "ASSET": TransactionType.ASSET,
}
'''
)

(models_dir / "__init__.py").write_text(
    '''"""Unified SQLModel table classes."""

from app.core.models.campaigns import *
from app.core.models.entities import *
from app.core.models.indexes import UnifiedTransactionIndexes
from app.core.models.reference import *
from app.core.models.transactions import *
'''
)

builder_src = "".join(builder_block)
builder_src = builder_src.replace(
    "from .unified_field_library import field_library\n",
    "",
)
builder_header = '''"""Build unified SQLModel rows from state-specific records."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, select

from app.core.constants import PLACEHOLDER_NAMES, RECORD_TYPE_TO_TRANSACTION
from app.core.enums import TransactionType
from app.core.models import UnifiedAddress, UnifiedCommittee, UnifiedEntity, UnifiedPerson, UnifiedTransaction
from app.core.unified_field_library import field_library

'''
(ROOT / "app/core/builders.py").write_text(builder_header + builder_src)

processor_src = "".join(processor_block)
processor_header = '''"""High-level unified SQLModel processor."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from app.core.builders import UnifiedSQLModelBuilder
from app.core.models import UnifiedTransaction

'''
(ROOT / "app/core/processor.py").write_text(processor_header + processor_src)

# Shim replaces monolith
shim = '''"""Backward-compatible re-exports for unified SQLModel symbols (TASK-3a shim)."""

from app.core.builders import UnifiedSQLModelBuilder
from app.core.constants import (
    AMOUNT_BUCKETS,
    DEFAULT_STATE,
    MONEY_TYPE,
    PLACEHOLDER_NAMES,
    RECORD_TYPE_CODES,
    RECORD_TYPE_TO_TRANSACTION,
)
from app.core.enums import (
    AssociationType,
    CampaignRole,
    CommitteeRole,
    EntityType,
    PersonRole,
    PersonType,
    TransactionType,
)
from app.core.models import *
from app.core.models.indexes import UnifiedTransactionIndexes
from app.core.processor import UnifiedSQLDataProcessor, unified_sql_processor

__all__ = [
    "AMOUNT_BUCKETS",
    "AssociationType",
    "CampaignRole",
    "CommitteeRole",
    "DEFAULT_STATE",
    "EntityType",
    "MONEY_TYPE",
    "PLACEHOLDER_NAMES",
    "PersonRole",
    "PersonType",
    "RECORD_TYPE_CODES",
    "RECORD_TYPE_TO_TRANSACTION",
    "TransactionType",
    "UnifiedSQLDataProcessor",
    "UnifiedSQLModelBuilder",
    "UnifiedTransactionIndexes",
    "unified_sql_processor",
]
'''
SRC.write_text(shim)

print("Wrote split modules.")
