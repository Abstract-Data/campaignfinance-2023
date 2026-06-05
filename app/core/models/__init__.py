"""Unified SQLModel table classes."""

from app.core.models.tables import *  # noqa: F403

# Register source-layer models that participate in tables.py relationships so the
# SQLAlchemy mapper can resolve them (e.g. UnifiedTransaction.report ->
# UnifiedReport) regardless of caller import order.  Imported *after* the
# ``import *`` above so the source modules' ``from app.core.models import ...``
# back-imports resolve against the now-populated namespace (no import cycle).
from app.core.source_models.reports import UnifiedReport  # noqa: E402, F401
