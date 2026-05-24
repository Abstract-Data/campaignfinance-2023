"""Pytest configuration for tests/resolve.

SQLModel 0.0.22 uses a single global MetaData instance.  When multiple test
modules each define their own stub SQLModel table classes (e.g. ``_State``,
``_UnifiedCommittee``) with the same ``__tablename__``, the second module to
be collected raises ``InvalidRequestError: Table '...' is already defined``.

The fixture below marks the affected stub tables as ``extend_existing=True``
via the MetaData ``info`` dict so that subsequent definitions of the same
table-name are silently merged rather than rejected.
"""

from __future__ import annotations

import pytest
from sqlmodel import SQLModel


@pytest.fixture(autouse=True, scope="session")
def extend_existing_metadata() -> None:
    """Allow duplicate table registrations across test modules."""
    SQLModel.metadata.info["extend_existing"] = True
