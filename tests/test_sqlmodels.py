#!/usr/bin/env python3
"""
Test script for SQLModel-based unified models with database operations.
"""

from pathlib import Path
from icecream import ic
from decimal import Decimal

from app.core.unified_sqlmodels import (
    unified_sql_processor, UnifiedTransaction, PersonRole, TransactionType
)
from app.core.unified_database import db_manager


def test_sqlmodels():
    """Test the SQLModel-based unified models with database operations."""
    
    ic("Testing SQLModel-Based Unified Models")
    ic("=" * 50)
    
    # Test 1: Create transactions from sample data
    ic("\n1. Creating Sample Transactions")
    ic("-" * 30)
    
    # Sample Texas contribution record
    texas_contribution = {
        "contributionInfoId": "TX12345",
        "contributionAmount": "1000.00",
        "contributionDt": "2024-01-15",
        "contributionDescr": "Campaign contribution",
        "contributorNameFirst": "John",
        "contributorNameLast": "Doe",
        "contributorEmployer": "Tech Corp",
        "contributorOccupation": "Engineer",
        "contributorStreetAddr1": "123 Main St",
        "contributorStreetCity": "Austin",
        "contributorStreetStateCd": "TX",
        "contributorStreetPostalCode": "78701",
        "filerName": "Committee for Progress",
        "filerTypeCd": "candidate",
        "filedDt": "2024-01-20",
        "file_origin": "texas_contributions_2024",
        "download_date": "2024-01-25"
    }
    
    # Sample Oklahoma expenditure record
    oklahoma_expenditure = {
        "Expenditure ID": "OK67890",
        "Expenditure Amount": "500.00",
        "Expenditure Date": "01/15/2024",
        "Description": "Campaign advertising",
        "Expenditure Type": "Ordinary and Necessary Campaign Expense",
        "First Name": "Jane",
        "Last Name": "Smith",
        "Employer": "Marketing Inc",
        "Occupation": "Consultant",
        "Address 1": "456 Oak Ave",
        "City": "Oklahoma City",
        "State": "OK",
        "Zip": "73102",
        "Committee Name": "Smith for Senate",
        "Committee Type": "Candidate Committee",
        "Filed Date": "01/20/2024",
        "Amended": "N",
        "file_origin": "oklahoma_expenditures_2024",
        "download_date": "2024-01-25"
    }
    
    # Process records into SQLModel instances
    texas_transaction = unified_sql_processor.process_record(texas_contribution, "texas")
    oklahoma_transaction = unified_sql_processor.process_record(oklahoma_expenditure, "oklahoma")
    
    ic(f"Texas Transaction: {texas_transaction.transaction_id} - ${texas_transaction.amount}")
    ic(f"Oklahoma Transaction: {oklahoma_transaction.transaction_id} - ${oklahoma_transaction.amount}")
    
    # Test 2: Database operations
    ic("\n2. Database Operations")
    ic("-" * 30)
    
    # Save transactions to database
    transactions = [texas_transaction, oklahoma_transaction]
    saved_count = db_manager.save_transactions(transactions)
    ic(f"Saved {saved_count} transactions to database")
    
    # Retrieve transactions from database
    all_transactions = db_manager.get_transactions()
    ic(f"Retrieved {len(all_transactions)} transactions from database")
    
    # Test filtering
    texas_transactions = db_manager.get_transactions(state="texas")
    ic(f"Texas transactions: {len(texas_transactions)}")
    
    contribution_transactions = db_manager.get_transactions(transaction_type=TransactionType.CONTRIBUTION)
    ic(f"Contribution transactions: {len(contribution_transactions)}")
    
    # Test 3: Relationship queries
    ic("\n3. Relationship Queries")
    ic("-" * 30)
    
    for tx in all_transactions:
        ic(f"Transaction: {tx.transaction_id} ({tx.state})")
        
        if tx.contributor:
            ic(f"  Contributor: {tx.contributor.person.full_name}")
            ic(f"  Employer: {tx.contributor.person.employer}")
            ic(f"  Address: {tx.contributor.person.address.full_address if tx.contributor.person.address else 'No address'}")
        
        if tx.committee:
            ic(f"  Committee: {tx.committee.name}")
            ic(f"  Committee Type: {tx.committee.committee_type}")
    
    # Test 4: Summary statistics
    ic("\n4. Summary Statistics")
    ic("-" * 30)
    
    stats = db_manager.get_summary_statistics()
    ic(f"Total transactions: {stats['total_transactions']}")
    ic(f"Total amount: ${stats['total_amount']}")
    
    ic("By state:")
    for state, data in stats['by_state'].items():
        ic(f"  {state}: {data['count']} transactions, ${data['total_amount']}")
    
    ic("By transaction type:")
    for tx_type, data in stats['by_type'].items():
        ic(f"  {tx_type}: {data['count']} transactions, ${data['total_amount']}")
    
    # Test 5: Cross-state analysis
    ic("\n5. Cross-State Analysis")
    ic("-" * 30)
    
    analysis = db_manager.get_cross_state_analysis()
    ic(f"Total transactions: {analysis['total_transactions']}")
    
    ic("Amount ranges:")
    for range_name, count in analysis['amount_ranges'].items():
        ic(f"  {range_name}: {count} transactions")
    
    # Test 6: Export functionality
    ic("\n6. Export Functionality")
    ic("-" * 30)
    
    # Export to JSON
    export_path = Path("unified_transactions_export.json")
    db_manager.export_to_json(export_path)
    ic(f"Exported transactions to {export_path}")
    
    # Test 7: Custom queries
    ic("\n7. Custom SQL Queries")
    ic("-" * 30)
    
    # Example custom query
    custom_query = """
    SELECT 
        state,
        transaction_type,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM unified_transactions 
    GROUP BY state, transaction_type
    ORDER BY state, total_amount DESC
    """
    
    results = db_manager.run_custom_query(custom_query)
    ic("Custom query results:")
    for row in results:
        ic(f"  {row['state']} - {row['transaction_type']}: {row['transaction_count']} transactions, ${row['total_amount']}")
    
    # Test 8: Advanced queries
    ic("\n8. Advanced Queries")
    ic("-" * 30)
    
    # Get high-value transactions
    high_value_tx = db_manager.get_transactions_by_amount_range(500, 10000)
    ic(f"High-value transactions (500-10000): {len(high_value_tx)}")
    
    for tx in high_value_tx:
        ic(f"  {tx.transaction_id}: ${tx.amount} ({tx.state})")
    
    # Test 9: Person and committee lookups
    ic("\n9. Person and Committee Lookups")
    ic("-" * 30)
    
    # Look up person by name
    john_does = db_manager.get_person_by_name("John", "Doe")
    ic(f"Found {len(john_does)} people named John Doe")
    
    for person in john_does:
        ic(f"  {person.full_name} - {person.employer}")
    
    # Look up committee by name
    committees = db_manager.get_committee_by_name("Committee for Progress")
    ic(f"Found {len(committees)} committees named 'Committee for Progress'")
    
    for committee in committees:
        ic(f"  {committee.name} - {committee.committee_type}")
    
    ic("\nSQLModel Test Complete!")
    ic("=" * 50)


def test_file_loading():
    """Test loading data from actual files."""
    
    ic("\nTesting File Loading")
    ic("=" * 30)
    
    # Try to load actual files if they exist
    test_files = [
        (Path("tmp/texas/contribs_20250805w.parquet"), "texas"),
        (Path("tmp/oklahoma/2014_ExpenditureExtract.csv"), "oklahoma")
    ]
    
    for file_path, state in test_files:
        if file_path.exists():
            ic(f"Loading {file_path}...")
            try:
                # Load and save to database
                saved_count = db_manager.load_and_save_file(file_path, state)
                ic(f"Successfully loaded and saved {saved_count} transactions from {state}")
                
                # Get summary for this state
                state_transactions = db_manager.get_transactions(state=state)
                total_amount = sum(tx.amount for tx in state_transactions if tx.amount)
                ic(f"Total amount for {state}: ${total_amount}")
                
            except Exception as e:
                ic(f"Error loading {file_path}: {e}")
        else:
            ic(f"File not found: {file_path}")


if __name__ == "__main__":
    test_sqlmodels()
    test_file_loading() 