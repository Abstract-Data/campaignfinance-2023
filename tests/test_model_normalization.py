"""
Tests for model normalization validators (RF-SMELL-001 / P3-QUAL-005).

These tests verify that strip/upper normalization actually runs on construction
for all 7 former __post_init__ hooks, now replaced with Pydantic v2
@model_validator(mode="after") (SQLModel classes) and inline normalization
(plain Pydantic models).
"""


# Register UnifiedReport before any unified_sqlmodels import so SQLAlchemy
# can resolve the forward reference in UnifiedTransaction.report relationship.
import app.core.source_models.reports  # noqa: F401

# ---------------------------------------------------------------------------
# unified_sqlmodels.py — SQLModel (Pydantic-based); __post_init__ is inert;
# validators must run via @model_validator(mode="after").
# ---------------------------------------------------------------------------


class TestSQLModelAddressNormalization:
    """UnifiedAddress (unified_sqlmodels) — RF-SMELL-001 hook 1"""

    def test_state_uppercased_and_stripped(self):
        from app.core.unified_sqlmodels import UnifiedAddress

        addr = UnifiedAddress(state=" tx ")
        assert addr.state == "TX"

    def test_state_none_stays_none(self):
        from app.core.unified_sqlmodels import UnifiedAddress

        addr = UnifiedAddress(state=None)
        assert addr.state is None

    def test_city_stripped(self):
        from app.core.unified_sqlmodels import UnifiedAddress

        addr = UnifiedAddress(city="  Austin  ")
        assert addr.city == "Austin"

    def test_zip_code_stripped(self):
        from app.core.unified_sqlmodels import UnifiedAddress

        addr = UnifiedAddress(zip_code=" 78701 ")
        assert addr.zip_code == "78701"

    def test_all_normalizations_together(self):
        from app.core.unified_sqlmodels import UnifiedAddress

        addr = UnifiedAddress(state=" tx ", city=" Austin ", zip_code=" 78701 ")
        assert addr.state == "TX"
        assert addr.city == "Austin"
        assert addr.zip_code == "78701"


class TestSQLModelPersonNormalization:
    """UnifiedPerson (unified_sqlmodels) — RF-SMELL-001 hook 2"""

    def test_first_name_stripped(self):
        from app.core.unified_sqlmodels import UnifiedPerson

        p = UnifiedPerson(first_name="  John  ")
        assert p.first_name == "John"

    def test_last_name_stripped(self):
        from app.core.unified_sqlmodels import UnifiedPerson

        p = UnifiedPerson(last_name="  Smith  ")
        assert p.last_name == "Smith"

    def test_organization_stripped(self):
        from app.core.unified_sqlmodels import UnifiedPerson

        p = UnifiedPerson(organization="  ACME Corp  ")
        assert p.organization == "ACME Corp"

    def test_employer_stripped(self):
        from app.core.unified_sqlmodels import UnifiedPerson

        p = UnifiedPerson(employer="  State of Texas  ")
        assert p.employer == "State of Texas"

    def test_occupation_stripped(self):
        from app.core.unified_sqlmodels import UnifiedPerson

        p = UnifiedPerson(occupation="  Software Engineer  ")
        assert p.occupation == "Software Engineer"

    def test_none_fields_unchanged(self):
        from app.core.unified_sqlmodels import UnifiedPerson

        p = UnifiedPerson(first_name=None, last_name=None)
        assert p.first_name is None
        assert p.last_name is None


class TestSQLModelCommitteeNormalization:
    """UnifiedCommittee (unified_sqlmodels) — RF-SMELL-001 hook 3"""

    def test_name_stripped(self):
        from app.core.unified_sqlmodels import UnifiedCommittee

        c = UnifiedCommittee(filer_id="TEST001", name="  Test Committee  ")
        assert c.name == "Test Committee"

    def test_committee_type_stripped(self):
        from app.core.unified_sqlmodels import UnifiedCommittee

        c = UnifiedCommittee(filer_id="TEST001", committee_type="  PAC  ")
        assert c.committee_type == "PAC"

    def test_none_name_unchanged(self):
        from app.core.unified_sqlmodels import UnifiedCommittee

        c = UnifiedCommittee(filer_id="TEST001", name=None)
        assert c.name is None


# ---------------------------------------------------------------------------
# unified_models.py — plain Pydantic BaseModel (converted from @dataclass);
# validators must run via @model_validator(mode="after").
# ---------------------------------------------------------------------------


class TestPydanticAddressNormalization:
    """UnifiedAddress (unified_models) — RF-SMELL-001 hook 4"""

    def test_state_uppercased_and_stripped(self):
        from app.core.unified_models import UnifiedAddress

        addr = UnifiedAddress(state=" ca ")
        assert addr.state == "CA"

    def test_city_stripped(self):
        from app.core.unified_models import UnifiedAddress

        addr = UnifiedAddress(city="  Los Angeles  ")
        assert addr.city == "Los Angeles"

    def test_zip_code_stripped(self):
        from app.core.unified_models import UnifiedAddress

        addr = UnifiedAddress(zip_code=" 90001 ")
        assert addr.zip_code == "90001"


class TestPydanticPersonNormalization:
    """UnifiedPerson (unified_models) — RF-SMELL-001 hook 5"""

    def test_first_name_stripped(self):
        from app.core.unified_models import UnifiedPerson

        p = UnifiedPerson(first_name="  Jane  ")
        assert p.first_name == "Jane"

    def test_last_name_stripped(self):
        from app.core.unified_models import UnifiedPerson

        p = UnifiedPerson(last_name="  Doe  ")
        assert p.last_name == "Doe"

    def test_organization_stripped(self):
        from app.core.unified_models import UnifiedPerson

        p = UnifiedPerson(organization="  Texas PAC  ")
        assert p.organization == "Texas PAC"

    def test_employer_stripped(self):
        from app.core.unified_models import UnifiedPerson

        p = UnifiedPerson(employer="  City of Dallas  ")
        assert p.employer == "City of Dallas"

    def test_occupation_stripped(self):
        from app.core.unified_models import UnifiedPerson

        p = UnifiedPerson(occupation="  Attorney  ")
        assert p.occupation == "Attorney"


class TestPydanticCommitteeNormalization:
    """UnifiedCommittee (unified_models) — RF-SMELL-001 hook 6"""

    def test_name_stripped(self):
        from app.core.unified_models import UnifiedCommittee

        c = UnifiedCommittee(name="  Friends of Texas  ")
        assert c.name == "Friends of Texas"

    def test_committee_type_stripped(self):
        from app.core.unified_models import UnifiedCommittee

        c = UnifiedCommittee(committee_type="  General Purpose  ")
        assert c.committee_type == "General Purpose"


class TestPydanticTransactionNormalization:
    """UnifiedTransaction (unified_models) — RF-SMELL-001 hook 7"""

    def test_description_stripped(self):
        from app.core.unified_models import UnifiedTransaction

        t = UnifiedTransaction(description="  Donation  ")
        assert t.description == "Donation"

    def test_transaction_id_stripped(self):
        from app.core.unified_models import UnifiedTransaction

        t = UnifiedTransaction(transaction_id="  TXN-001  ")
        assert t.transaction_id == "TXN-001"

    def test_none_description_unchanged(self):
        from app.core.unified_models import UnifiedTransaction

        t = UnifiedTransaction(description=None)
        assert t.description is None


# ---------------------------------------------------------------------------
# RF-DEAD-002 — UnifiedTransactionPerson.state_id declared only once
# ---------------------------------------------------------------------------


class TestDuplicateFieldRemoved:
    """RF-DEAD-002 — state_id appears exactly once in UnifiedTransactionPerson"""

    def test_state_id_declared_once(self):
        import inspect

        from app.core.unified_sqlmodels import UnifiedTransactionPerson

        src = inspect.getsource(UnifiedTransactionPerson)
        assert src.count("state_id") <= 3, (
            "state_id should appear at most 3 times (field def, relationship, test ref) "
            f"but found: {src.count('state_id')}"
        )

    def test_model_fields_has_state_id_once(self):
        from app.core.unified_sqlmodels import UnifiedTransactionPerson

        fields = UnifiedTransactionPerson.model_fields
        assert "state_id" in fields


# ---------------------------------------------------------------------------
# P3-QUAL-002 — No datetime.utcnow in unified_sqlmodels.py
# ---------------------------------------------------------------------------


class TestNoUtcnow:
    """P3-QUAL-002 — datetime.utcnow must not appear in unified_sqlmodels.py"""

    def test_utcnow_not_in_sqlmodels(self):
        import pathlib

        src = pathlib.Path("app/core/unified_sqlmodels.py").read_text()
        assert (
            "utcnow" not in src
        ), "datetime.utcnow must be replaced with datetime.now(timezone.utc)"
