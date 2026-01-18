#!/usr/bin/env python3
"""
Test script to process a single record and debug the issue.
"""

from app.states.unified_state_loader import UnifiedStateLoader
from app.states.postgres_config import create_postgres_database_manager
from pathlib import Path

def test_single_record():
    """Test processing a single record"""
    print("Testing single record processing...")
    
    # Create loader
    db_manager = create_postgres_database_manager()
    loader = UnifiedStateLoader("oklahoma", Path("tmp"))
    loader.db_manager = db_manager
    
    # Find the 2020 file
    files = loader._discover_data_files()
    test_file = None
    for file in files:
        if "2020" in file.name:
            test_file = file
            break
    
    if not test_file:
        print("❌ No 2020 file found")
        return
    
    print(f"✅ Testing with file: {test_file.name}")
    
    # Process just 1 record
    from app.funcs.csv_reader import FileReader
    file_reader = FileReader()
    data_generator = file_reader.read_csv(test_file)
    
    # Get the first record
    record = next(data_generator)
    print(f"Sample record keys: {list(record.keys())}")
    print(f"Org ID: {record.get('Org ID', 'NOT_FOUND')}")
    print(f"Committee Name: {record.get('Committee Name', 'NOT_FOUND')}")
    print(f"Candidate Name: {record.get('Candidate Name', 'NOT_FOUND')}")
    
    # Add state and file origin information
    record['state'] = 'oklahoma'
    record['file_origin'] = test_file.name
    
    try:
        # Create transaction
        transaction = loader._create_transaction_from_record(record)
        if transaction:
            print(f"✅ Transaction created: {transaction.transaction_id}")
        else:
            print(f"❌ Failed to create transaction")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_record() 