import sys
import os
from datetime import date
from decimal import Decimal
from sqlmodel import SQLModel, create_engine, Session

# Ensure we are running from 'app' directory context
if os.path.basename(os.getcwd()) != 'app':
    print(f"Current directory: {os.getcwd()}")
    # If not in app, add app to path and assume we can import states
    if os.path.exists('app'):
        sys.path.insert(0, 'app')
    elif os.path.exists('../app'):
        sys.path.insert(0, '../app')

from app.states.texas.normalized_models import (
    TECFiler, TECCoverSheet1, TECContribution, TECPerson, TECAddress
)

def test_texas_relationships():
    print("Setting up in-memory database...")
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        print("Creating test data...")
        
        # 1. Create a Filer
        filer = TECFiler(
            filer_ident="FILER123",
            filer_name="Test Candidate Committee",
            filer_type_cd="CAND"
        )
        session.add(filer)
        session.commit()
        session.refresh(filer)
        print(f"Created Filer: {filer.filer_name} (ID: {filer.id})")

        # 2. Create a Cover Sheet linked to the Filer
        cover_sheet = TECCoverSheet1(
            report_info_ident=999001,
            filer_ident=filer.filer_ident,
            report_type_cd="JAN15",
            filed_dt=date(2024, 1, 15)
        )
        session.add(cover_sheet)
        session.commit()
        session.refresh(cover_sheet)
        print(f"Created Cover Sheet: {cover_sheet.report_info_ident} (ID: {cover_sheet.id})")

        # Verify Filer -> CoverSheet relationship
        session.refresh(filer)
        assert len(filer.cover_sheets) == 1
        assert filer.cover_sheets[0].report_info_ident == 999001
        print("Verified Filer -> CoverSheet relationship")

        # Verify CoverSheet -> Filer relationship
        assert cover_sheet.filer is not None
        assert cover_sheet.filer.filer_ident == "FILER123"
        print("Verified CoverSheet -> Filer relationship")

        # 3. Create a Contributor (Person + Address)
        addr = TECAddress(
            street_addr1="123 Donor Ln",
            city="Austin",
            state_cd="TX",
            address_hash="HASH1"
        )
        person = TECPerson(
            name_last="Smith",
            name_first="John",
            person_type="INDIVIDUAL",
            person_hash="HASH2",
            address=addr
        )
        session.add(person)
        session.commit()
        session.refresh(person)

        # 4. Create a Contribution linked to Filer, CoverSheet, and Contributor
        contribution = TECContribution(
            contribution_info_id=5001,
            contribution_amount=Decimal("100.00"),
            contribution_dt=date(2024, 1, 10),
            filer_ident=filer.filer_ident,
            report_info_ident=cover_sheet.report_info_ident,
            contributor_id=person.id
        )
        session.add(contribution)
        session.commit()
        session.refresh(contribution)
        print(f"Created Contribution: ${contribution.contribution_amount} (ID: {contribution.id})")

        # Verify Contribution -> Filer
        assert contribution.filer is not None
        assert contribution.filer.filer_ident == "FILER123"
        print("Verified Contribution -> Filer")

        # Verify Contribution -> CoverSheet
        assert contribution.cover_sheet is not None
        assert contribution.cover_sheet.report_info_ident == 999001
        print("Verified Contribution -> CoverSheet")

        # Verify Filer -> Contributions
        session.refresh(filer)
        assert len(filer.contributions) == 1
        assert filer.contributions[0].contribution_amount == Decimal("100.00")
        print("Verified Filer -> Contributions")

        # Verify CoverSheet -> Contributions
        session.refresh(cover_sheet)
        assert len(cover_sheet.contributions) == 1
        assert cover_sheet.contributions[0].contribution_amount == Decimal("100.00")
        print("Verified CoverSheet -> Contributions")

    print("\nAll relationship tests passed!")

if __name__ == "__main__":
    test_texas_relationships()
