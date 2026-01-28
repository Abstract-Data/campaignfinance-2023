#!/usr/bin/env python3
"""
Test script for unified models that can handle data from any state.
"""

from pathlib import Path
from icecream import ic
from app.core.unified_models import unified_processor, TransactionType, PersonType
from decimal import Decimal


def test_unified_models():
    """Test the unified models with data from different states."""
    
    ic("Testing Unified Models for Campaign Finance Data")
    ic("=" * 50)
    
    # Test with Texas data
    ic("\n1. Testing Texas Data Processing")
    ic("-" * 30)
    
    # Sample Texas contribution record
    texas_contribution = {
        "contributionInfoId": "12345",
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
    
    # Process Texas record
    texas_transaction = unified_processor.process_record(texas_contribution, "texas")
    
    ic(f"Transaction ID: {texas_transaction.transaction_id}")
    ic(f"Amount: ${texas_transaction.amount}")
    ic(f"Date: {texas_transaction.transaction_date}")
    ic(f"Type: {texas_transaction.transaction_type.value}")
    ic(f"Description: {texas_transaction.description}")
    
    if texas_transaction.contributor:
        ic(f"Contributor: {texas_transaction.contributor.full_name}")
        ic(f"Employer: {texas_transaction.contributor.employer}")
        ic(f"Occupation: {texas_transaction.contributor.occupation}")
        ic(f"Person Type: {texas_transaction.contributor.person_type.value}")
        
        if texas_transaction.contributor.address:
            ic(f"Address: {texas_transaction.contributor.address.street_1}, {texas_transaction.contributor.address.city}, {texas_transaction.contributor.address.state}")
    
    if texas_transaction.committee:
        ic(f"Committee: {texas_transaction.committee.name}")
        ic(f"Committee Type: {texas_transaction.committee.committee_type}")
    
    ic(f"State: {texas_transaction.state}")
    ic(f"Filed Date: {texas_transaction.filed_date}")
    ic(f"Amended: {texas_transaction.amended}")
    
    # Test with Oklahoma data
    ic("\n2. Testing Oklahoma Data Processing")
    ic("-" * 30)
    
    # Sample Oklahoma expenditure record
    oklahoma_expenditure = {
        "Expenditure ID": "67890",
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
    
    # Process Oklahoma record
    oklahoma_transaction = unified_processor.process_record(oklahoma_expenditure, "oklahoma")
    
    ic(f"Transaction ID: {oklahoma_transaction.transaction_id}")
    ic(f"Amount: ${oklahoma_transaction.amount}")
    ic(f"Date: {oklahoma_transaction.transaction_date}")
    ic(f"Type: {oklahoma_transaction.transaction_type.value}")
    ic(f"Description: {oklahoma_transaction.description}")
    
    if oklahoma_transaction.contributor:
        ic(f"Contributor: {oklahoma_transaction.contributor.full_name}")
        ic(f"Employer: {oklahoma_transaction.contributor.employer}")
        ic(f"Occupation: {oklahoma_transaction.contributor.occupation}")
        ic(f"Person Type: {oklahoma_transaction.contributor.person_type.value}")
        
        if oklahoma_transaction.contributor.address:
            ic(f"Address: {oklahoma_transaction.contributor.address.street_1}, {oklahoma_transaction.contributor.address.city}, {oklahoma_transaction.contributor.address.state}")
    
    if oklahoma_transaction.committee:
        ic(f"Committee: {oklahoma_transaction.committee.name}")
        ic(f"Committee Type: {oklahoma_transaction.committee.committee_type}")
    
    ic(f"State: {oklahoma_transaction.state}")
    ic(f"Filed Date: {oklahoma_transaction.filed_date}")
    ic(f"Amended: {oklahoma_transaction.amended}")
    
    # Test file processing
    ic("\n3. Testing File Processing")
    ic("-" * 30)
    
    # Try to process actual files if they exist
    test_files = [
        Path("tmp/texas/contribs_20250805w.parquet"),
        Path("tmp/oklahoma/2014_ExpenditureExtract.csv")
    ]
    
    for file_path in test_files:
        if file_path.exists():
            ic(f"Processing file: {file_path}")
            try:
                state = "texas" if "texas" in str(file_path) else "oklahoma"
                transactions = unified_processor.process_file(file_path, state)
                ic(f"Successfully processed {len(transactions)} transactions")
                
                # Show first transaction as example
                if transactions:
                    first_tx = transactions[0]
                    ic(f"First transaction: {first_tx.transaction_id} - ${first_tx.amount} - {first_tx.transaction_type.value}")
                    
                    if first_tx.contributor:
                        ic(f"Contributor: {first_tx.contributor.full_name}")
                    
                    if first_tx.committee:
                        ic(f"Committee: {first_tx.committee.name}")
                
            except Exception as e:
                ic(f"Error processing {file_path}: {e}")
        else:
            ic(f"File not found: {file_path}")
    
    # Test cross-state analysis
    ic("\n4. Cross-State Analysis Example")
    ic("-" * 30)
    
    # Create a list of transactions from different states
    all_transactions = []
    
    # Add Texas transaction
    all_transactions.append(texas_transaction)
    
    # Add Oklahoma transaction
    all_transactions.append(oklahoma_transaction)
    
    # Now we can analyze across states using unified fields
    ic(f"Total transactions: {len(all_transactions)}")
    
    # Group by transaction type
    by_type = {}
    for tx in all_transactions:
        tx_type = tx.transaction_type.value
        if tx_type not in by_type:
            by_type[tx_type] = []
        by_type[tx_type].append(tx)
    
    ic("Transactions by type:")
    for tx_type, transactions in by_type.items():
        ic(f"  {tx_type}: {len(transactions)} transactions")
        total_amount = sum(tx.amount for tx in transactions if tx.amount)
        ic(f"    Total amount: ${total_amount}")
    
    # Group by state
    by_state = {}
    for tx in all_transactions:
        state = tx.state
        if state not in by_state:
            by_state[state] = []
        by_state[state].append(tx)
    
    ic("Transactions by state:")
    for state, transactions in by_state.items():
        ic(f"  {state}: {len(transactions)} transactions")
        total_amount = sum(tx.amount for tx in transactions if tx.amount)
        ic(f"    Total amount: ${total_amount}")
    
    # Find high-value transactions
    high_value_threshold = Decimal("500.00")
    high_value_transactions = [tx for tx in all_transactions if tx.amount and tx.amount >= high_value_threshold]
    
    ic(f"High-value transactions (>= ${high_value_threshold}): {len(high_value_transactions)}")
    for tx in high_value_transactions:
        ic(f"  {tx.transaction_id}: ${tx.amount} ({tx.state}) - {tx.contributor.full_name if tx.contributor else 'Unknown'}")
    
    ic("\nUnified Models Test Complete!")
    ic("=" * 50)


if __name__ == "__main__":
    test_unified_models() 