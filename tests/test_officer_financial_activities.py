#!/usr/bin/env python3
"""
Test script for linking committee officers' financial activities to their roles.
"""

from pathlib import Path
from icecream import ic
from datetime import date
from decimal import Decimal

from app.states.unified_sqlmodels import (
    unified_sql_processor, UnifiedTransaction, PersonRole, TransactionType,
    CommitteeRole, UnifiedPerson, UnifiedCommittee, UnifiedTransactionPerson
)
from app.states.unified_database import db_manager


def test_officer_financial_activities():
    """Test linking committee officers' financial activities to their roles."""
    
    ic("Testing Officer Financial Activities")
    ic("=" * 50)
    
    # Test 1: Create sample data
    ic("\n1. Creating Sample Data")
    ic("-" * 30)
    
    # Create a person (committee officer)
    with db_manager.get_session() as session:
        person = UnifiedPerson(
            first_name="John",
            last_name="Smith",
            employer="Smith & Associates",
            occupation="Attorney",
            person_type="individual"
        )
        session.add(person)
        session.commit()
        session.refresh(person)
        
        # Create a committee
        committee = UnifiedCommittee(
            name="Smith for Senate Committee",
            committee_type="candidate",
            filer_id="C12345"
        )
        session.add(committee)
        session.commit()
        session.refresh(committee)
        
        ic(f"Created person: {person.full_name} (ID: {person.id})")
        ic(f"Created committee: {committee.name} (ID: {committee.id})")
    
    # Test 2: Add person as treasurer
    ic("\n2. Adding Person as Treasurer")
    ic("-" * 30)
    
    treasurer_role = db_manager.add_person_to_committee(
        person_id=person.id,
        committee_id=committee.id,
        role=CommitteeRole.TREASURER,
        start_date=date(2024, 1, 1),
        notes="Primary treasurer for the campaign",
        user="admin_user"
    )
    
    ic(f"Added {person.full_name} as {treasurer_role.role} to {committee.name}")
    
    # Test 3: Create sample transactions
    ic("\n3. Creating Sample Transactions")
    ic("-" * 30)
    
    # Create a contribution transaction (officer contributing to their own committee)
    with db_manager.get_session() as session:
        contribution_tx = UnifiedTransaction(
            transaction_id="TX001",
            amount=Decimal("1000.00"),
            transaction_date=date(2024, 2, 15),
            description="Personal contribution from treasurer",
            transaction_type=TransactionType.CONTRIBUTION,
            committee_id=committee.id,
            state="texas",
            file_origin="test_data"
        )
        session.add(contribution_tx)
        session.commit()
        session.refresh(contribution_tx)
        
        # Create transaction-person relationship for contribution
        contrib_tx_person = UnifiedTransactionPerson(
            transaction_id=contribution_tx.id,
            person_id=person.id,
            role=PersonRole.CONTRIBUTOR,
            amount=Decimal("1000.00")
        )
        session.add(contrib_tx_person)
        session.commit()
        session.refresh(contrib_tx_person)
        
        ic(f"Created contribution transaction: {contribution_tx.transaction_id} - ${contribution_tx.amount}")
        ic(f"Created transaction-person relationship: {contrib_tx_person.id}")
    
    # Create an expenditure transaction (committee paying officer for services)
    with db_manager.get_session() as session:
        expenditure_tx = UnifiedTransaction(
            transaction_id="TX002",
            amount=Decimal("500.00"),
            transaction_date=date(2024, 3, 1),
            description="Payment for treasurer services",
            transaction_type=TransactionType.EXPENDITURE,
            committee_id=committee.id,
            state="texas",
            file_origin="test_data"
        )
        session.add(expenditure_tx)
        session.commit()
        session.refresh(expenditure_tx)
        
        # Create transaction-person relationship for expenditure
        exp_tx_person = UnifiedTransactionPerson(
            transaction_id=expenditure_tx.id,
            person_id=person.id,
            role=PersonRole.PAYEE,
            amount=Decimal("500.00")
        )
        session.add(exp_tx_person)
        session.commit()
        session.refresh(exp_tx_person)
        
        ic(f"Created expenditure transaction: {expenditure_tx.transaction_id} - ${expenditure_tx.amount}")
        ic(f"Created transaction-person relationship: {exp_tx_person.id}")
    
    # Test 4: Link transactions to committee role
    ic("\n4. Linking Transactions to Committee Role")
    ic("-" * 30)
    
    # Link contribution to treasurer role
    linked_contrib = db_manager.link_transaction_to_committee_role(
        transaction_person_id=contrib_tx_person.id,
        committee_person_id=treasurer_role.id,
        user="admin_user",
        notes="Treasurer contributing to own committee"
    )
    ic(f"Linked contribution to treasurer role: {linked_contrib}")
    
    # Link expenditure to treasurer role
    linked_exp = db_manager.link_transaction_to_committee_role(
        transaction_person_id=exp_tx_person.id,
        committee_person_id=treasurer_role.id,
        user="admin_user",
        notes="Committee paying treasurer for services"
    )
    ic(f"Linked expenditure to treasurer role: {linked_exp}")
    
    # Test 5: Get officer contributions
    ic("\n5. Getting Officer Contributions")
    ic("-" * 30)
    
    officer_contributions = db_manager.get_officer_contributions(treasurer_role.id)
    ic(f"Contributions made by {person.full_name} as treasurer:")
    for contrib in officer_contributions:
        ic(f"  - ${contrib.transaction.amount} on {contrib.transaction.transaction_date}")
        ic(f"    Description: {contrib.transaction.description}")
        ic(f"    Notes: {contrib.notes}")
    
    # Test 6: Get officer expenditures
    ic("\n6. Getting Officer Expenditures")
    ic("-" * 30)
    
    officer_expenditures = db_manager.get_officer_expenditures(treasurer_role.id)
    ic(f"Expenditures received by {person.full_name} as treasurer:")
    for exp in officer_expenditures:
        ic(f"  - ${exp.transaction.amount} on {exp.transaction.transaction_date}")
        ic(f"    Description: {exp.transaction.description}")
        ic(f"    Notes: {exp.notes}")
    
    # Test 7: Get committee officer activities
    ic("\n7. Getting Committee Officer Activities")
    ic("-" * 30)
    
    committee_activities = db_manager.get_committee_officer_activities(committee.id)
    ic(f"All officer activities for {committee.name}:")
    ic(f"  Contributions: {len(committee_activities['contributions'])}")
    ic(f"  Expenditures: {len(committee_activities['expenditures'])}")
    
    for contrib in committee_activities['contributions']:
        ic(f"    Contribution: ${contrib.transaction.amount} by {contrib.person.full_name}")
    
    for exp in committee_activities['expenditures']:
        ic(f"    Expenditure: ${exp.transaction.amount} to {exp.person.full_name}")
    
    # Test 8: Get person's committee financial summary
    ic("\n8. Getting Person's Committee Financial Summary")
    ic("-" * 30)
    
    financial_summary = db_manager.get_person_committee_financial_summary(person.id)
    ic(f"Financial summary for {person.full_name}:")
    ic(f"  Total contributions: ${financial_summary['total_contributions']}")
    ic(f"  Total expenditures: ${financial_summary['total_expenditures']}")
    
    for role_summary in financial_summary['committee_roles']:
        ic(f"  Role: {role_summary['role']} at {role_summary['committee']}")
        ic(f"    Contributions: ${role_summary['total_contributions']}")
        ic(f"    Expenditures: ${role_summary['total_expenditures']}")
        
        if role_summary['contributions']:
            ic(f"    Contribution details:")
            for contrib in role_summary['contributions']:
                ic(f"      - ${contrib['amount']} on {contrib['date']}: {contrib['description']}")
        
        if role_summary['expenditures']:
            ic(f"    Expenditure details:")
            for exp in role_summary['expenditures']:
                ic(f"      - ${exp['amount']} on {exp['date']}: {exp['description']}")
    
    # Test 9: Add another person and create cross-committee activities
    ic("\n9. Creating Cross-Committee Activities")
    ic("-" * 30)
    
    # Create another person
    with db_manager.get_session() as session:
        person2 = UnifiedPerson(
            first_name="Jane",
            last_name="Doe",
            employer="Doe Consulting",
            occupation="Consultant",
            person_type="individual"
        )
        session.add(person2)
        session.commit()
        session.refresh(person2)
        
        # Create another committee
        committee2 = UnifiedCommittee(
            name="Doe for Governor Committee",
            committee_type="candidate",
            filer_id="C67890"
        )
        session.add(committee2)
        session.commit()
        session.refresh(committee2)
    
    # Add person2 as chair of committee2
    chair_role = db_manager.add_person_to_committee(
        person_id=person2.id,
        committee_id=committee2.id,
        role=CommitteeRole.CHAIR,
        start_date=date(2024, 1, 1),
        user="admin_user"
    )
    
    # Create a contribution from person2 (chair) to person1's committee
    with db_manager.get_session() as session:
        cross_contrib_tx = UnifiedTransaction(
            transaction_id="TX003",
            amount=Decimal("2500.00"),
            transaction_date=date(2024, 4, 1),
            description="Support contribution from Doe campaign",
            transaction_type=TransactionType.CONTRIBUTION,
            committee_id=committee.id,  # Contributing to Smith's committee
            state="texas",
            file_origin="test_data"
        )
        session.add(cross_contrib_tx)
        session.commit()
        session.refresh(cross_contrib_tx)
        
        # Create transaction-person relationship
        cross_contrib_tx_person = UnifiedTransactionPerson(
            transaction_id=cross_contrib_tx.id,
            person_id=person2.id,  # Doe is the contributor
            role=PersonRole.CONTRIBUTOR,
            amount=Decimal("2500.00")
        )
        session.add(cross_contrib_tx_person)
        session.commit()
        session.refresh(cross_contrib_tx_person)
    
    # Link this contribution to person2's chair role
    linked_cross = db_manager.link_transaction_to_committee_role(
        transaction_person_id=cross_contrib_tx_person.id,
        committee_person_id=chair_role.id,
        user="admin_user",
        notes="Chair of Doe campaign contributing to Smith campaign"
    )
    
    ic(f"Created cross-committee contribution: ${cross_contrib_tx.amount}")
    ic(f"Linked to chair role: {linked_cross}")
    
    # Test 10: Get comprehensive financial summary
    ic("\n10. Getting Comprehensive Financial Summary")
    ic("-" * 30)
    
    # Get summary for person2 (Doe)
    doe_summary = db_manager.get_person_committee_financial_summary(person2.id)
    ic(f"Financial summary for {person2.full_name}:")
    ic(f"  Total contributions: ${doe_summary['total_contributions']}")
    ic(f"  Total expenditures: ${doe_summary['total_expenditures']}")
    
    for role_summary in doe_summary['committee_roles']:
        ic(f"  Role: {role_summary['role']} at {role_summary['committee']}")
        ic(f"    Contributions: ${role_summary['total_contributions']}")
        ic(f"    Expenditures: ${role_summary['total_expenditures']}")
    
    ic("\nOfficer Financial Activities Test Complete!")
    ic("=" * 50)


if __name__ == "__main__":
    test_officer_financial_activities() 