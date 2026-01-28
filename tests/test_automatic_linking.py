#!/usr/bin/env python3
"""
Test script for automatic linking of transactions to committee roles.
"""

from pathlib import Path
from icecream import ic
from datetime import date
from decimal import Decimal

from app.core.unified_sqlmodels import (
    unified_sql_processor, UnifiedTransaction, PersonRole, TransactionType,
    CommitteeRole, UnifiedPerson, UnifiedCommittee, UnifiedTransactionPerson
)
from app.core.unified_database import db_manager


def test_automatic_linking():
    """Test automatic linking of transactions to committee roles."""
    
    ic("Testing Automatic Transaction Linking")
    ic("=" * 50)
    
    # Test 1: Create sample data with unlinked transactions
    ic("\n1. Creating Sample Data with Unlinked Transactions")
    ic("-" * 30)
    
    # Create a person and committee
    with db_manager.get_session() as session:
        person = UnifiedPerson(
            first_name="Alice",
            last_name="Johnson",
            employer="Johnson Law",
            occupation="Attorney",
            person_type="individual"
        )
        session.add(person)
        session.commit()
        session.refresh(person)
        
        committee = UnifiedCommittee(
            name="Johnson for Congress Committee",
            committee_type="candidate",
            filer_id="C99999"
        )
        session.add(committee)
        session.commit()
        session.refresh(committee)
        
        ic(f"Created person: {person.full_name} (ID: {person.id})")
        ic(f"Created committee: {committee.name} (ID: {committee.id})")
    
    # Add person as treasurer
    treasurer_role = db_manager.add_person_to_committee(
        person_id=person.id,
        committee_id=committee.id,
        role=CommitteeRole.TREASURER,
        start_date=date(2024, 1, 1),
        user="admin_user"
    )
    
    # Create transactions WITHOUT linking them to committee roles
    with db_manager.get_session() as session:
        # Transaction 1: Contribution
        contrib_tx = UnifiedTransaction(
            transaction_id="AUTO001",
            amount=Decimal("2000.00"),
            transaction_date=date(2024, 2, 1),
            description="Personal contribution from Alice",
            transaction_type=TransactionType.CONTRIBUTION,
            committee_id=committee.id,
            state="texas",
            file_origin="test_data"
        )
        session.add(contrib_tx)
        session.commit()
        session.refresh(contrib_tx)
        
        contrib_tx_person = UnifiedTransactionPerson(
            transaction_id=contrib_tx.id,
            person_id=person.id,
            role=PersonRole.CONTRIBUTOR,
            amount=Decimal("2000.00")
        )
        session.add(contrib_tx_person)
        session.commit()
        
        # Transaction 2: Expenditure
        exp_tx = UnifiedTransaction(
            transaction_id="AUTO002",
            amount=Decimal("750.00"),
            transaction_date=date(2024, 3, 1),
            description="Payment for treasurer services",
            transaction_type=TransactionType.EXPENDITURE,
            committee_id=committee.id,
            state="texas",
            file_origin="test_data"
        )
        session.add(exp_tx)
        session.commit()
        session.refresh(exp_tx)
        
        exp_tx_person = UnifiedTransactionPerson(
            transaction_id=exp_tx.id,
            person_id=person.id,
            role=PersonRole.PAYEE,
            amount=Decimal("750.00")
        )
        session.add(exp_tx_person)
        session.commit()
        
        ic(f"Created unlinked transactions:")
        ic(f"  - Contribution: ${contrib_tx.amount} (ID: {contrib_tx_person.id})")
        ic(f"  - Expenditure: ${exp_tx.amount} (ID: {exp_tx_person.id})")
    
    # Test 2: Find unlinked transactions
    ic("\n2. Finding Unlinked Transactions")
    ic("-" * 30)
    
    unlinked = db_manager.get_unlinked_officer_transactions(committee.id)
    ic(f"Found {len(unlinked)} unlinked transactions for committee officers")
    
    for tx_person in unlinked:
        ic(f"  - {tx_person.person.full_name} ({tx_person.role}): ${tx_person.transaction.amount}")
        ic(f"    Committee role: {tx_person._committee_role_info['role']}")
    
    # Test 3: Auto-link transactions to committee roles
    ic("\n3. Auto-Linking Transactions to Committee Roles")
    ic("-" * 30)
    
    linked_counts = db_manager.auto_link_transactions_to_committee_roles(committee.id)
    ic(f"Auto-linked transactions:")
    ic(f"  - Contributions: {linked_counts['contributions']}")
    ic(f"  - Expenditures: {linked_counts['expenditures']}")
    ic(f"  - Total: {linked_counts['total']}")
    
    # Test 4: Verify the linking worked
    ic("\n4. Verifying Auto-Linking Results")
    ic("-" * 30)
    
    # Check officer contributions
    officer_contributions = db_manager.get_officer_contributions(treasurer_role.id)
    ic(f"Contributions by {person.full_name} as treasurer:")
    for contrib in officer_contributions:
        ic(f"  - ${contrib.transaction.amount} on {contrib.transaction.transaction_date}")
    
    # Check officer expenditures
    officer_expenditures = db_manager.get_officer_expenditures(treasurer_role.id)
    ic(f"Expenditures to {person.full_name} as treasurer:")
    for exp in officer_expenditures:
        ic(f"  - ${exp.transaction.amount} on {exp.transaction.transaction_date}")
    
    # Test 5: Process new transaction with automatic linking
    ic("\n5. Processing New Transaction with Automatic Linking")
    ic("-" * 30)
    
    # Create another person and committee
    with db_manager.get_session() as session:
        person2 = UnifiedPerson(
            first_name="Bob",
            last_name="Wilson",
            employer="Wilson Consulting",
            occupation="Consultant",
            person_type="individual"
        )
        session.add(person2)
        session.commit()
        session.refresh(person2)
        
        committee2 = UnifiedCommittee(
            name="Wilson for Mayor Committee",
            committee_type="candidate",
            filer_id="C88888"
        )
        session.add(committee2)
        session.commit()
        session.refresh(committee2)
    
    # Add person2 as chair
    chair_role = db_manager.add_person_to_committee(
        person_id=person2.id,
        committee_id=committee2.id,
        role=CommitteeRole.CHAIR,
        start_date=date(2024, 1, 1),
        user="admin_user"
    )
    
    # Process a new transaction with automatic linking
    transaction_data = {
        "transaction_id": "AUTO003",
        "amount": "1500.00",
        "transaction_date": "2024-04-01",
        "description": "Personal contribution from Bob",
        "transaction_type": "CONTRIBUTION",
        "committee_id": committee2.id,
        "state": "texas",
        "file_origin": "test_data",
        "contributor": {
            "first_name": "Bob",
            "last_name": "Wilson",
            "employer": "Wilson Consulting",
            "occupation": "Consultant"
        }
    }
    
    committee_officers = [
        {
            "person_id": person2.id,
            "committee_id": committee2.id,
            "role": CommitteeRole.CHAIR
        }
    ]
    
    new_transaction = db_manager.process_transaction_with_officer_linking(
        transaction_data=transaction_data,
        committee_officers=committee_officers,
        user="admin_user"
    )
    
    ic(f"Processed new transaction: {new_transaction.transaction_id} - ${new_transaction.amount}")
    
    # Verify the automatic linking worked
    chair_contributions = db_manager.get_officer_contributions(chair_role.id)
    ic(f"Contributions by {person2.full_name} as chair:")
    for contrib in chair_contributions:
        ic(f"  - ${contrib.transaction.amount} on {contrib.transaction.transaction_date}")
    
    # Test 6: Get comprehensive financial summary
    ic("\n6. Getting Comprehensive Financial Summary")
    ic("-" * 30)
    
    alice_summary = db_manager.get_person_committee_financial_summary(person.id)
    ic(f"Financial summary for {person.full_name}:")
    ic(f"  Total contributions: ${alice_summary['total_contributions']}")
    ic(f"  Total expenditures: ${alice_summary['total_expenditures']}")
    
    bob_summary = db_manager.get_person_committee_financial_summary(person2.id)
    ic(f"Financial summary for {person2.full_name}:")
    ic(f"  Total contributions: ${bob_summary['total_contributions']}")
    ic(f"  Total expenditures: ${bob_summary['total_expenditures']}")
    
    ic("\nAutomatic Linking Test Complete!")
    ic("=" * 50)


if __name__ == "__main__":
    test_automatic_linking() 