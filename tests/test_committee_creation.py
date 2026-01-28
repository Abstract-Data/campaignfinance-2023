#!/usr/bin/env python3
"""
Test script to load a few records and see committee creation.
"""

from app.core.unified_state_loader import UnifiedStateLoader
from app.states.postgres_config import create_postgres_database_manager
from pathlib import Path

def test_committee_creation():
    """Test committee creation with a few records"""
    print("Testing committee creation...")
    
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
    
    # Process just 10 records
    from app.funcs.csv_reader import FileReader
    file_reader = FileReader()
    data_generator = file_reader.read_csv(test_file)
    
    success_count = 0
    error_count = 0
    
    for i, record in enumerate(data_generator):
        if i >= 10:  # Only process 10 records
            break
            
        try:
            # Add state and file origin information
            record['state'] = 'oklahoma'
            record['file_origin'] = test_file.name
            
            # Create transaction
            transaction = loader._create_transaction_from_record(record)
            if transaction:
                success_count += 1
                print(f"  ✅ Record {i+1}: Transaction {transaction.transaction_id} created")
            else:
                error_count += 1
                print(f"  ❌ Record {i+1}: Failed to create transaction")
                
        except Exception as e:
            error_count += 1
            print(f"  ❌ Record {i+1}: Error - {e}")
    
    print(f"\n📊 Results:")
    print(f"  ✅ Successful: {success_count}")
    print(f"  ❌ Failed: {error_count}")
    
    # Check committees
    with db_manager.get_session() as session:
        from sqlalchemy import text
        committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
        print(f"  📋 Committees in database: {committee_count}")

if __name__ == "__main__":
    test_committee_creation() 