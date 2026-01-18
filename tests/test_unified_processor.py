#!/usr/bin/env python3
"""
Test to use the unified processor directly.
"""

from app.states.unified_sqlmodels import unified_sql_processor
from app.states.postgres_config import create_postgres_database_manager

def test_unified_processor():
    """Test the unified processor directly"""
    print("Testing unified processor...")
    
    # Sample record
    record = {
        "Receipt ID": "TEST456",
        "Org ID": "9908", 
        "Receipt Amount": "200.00",
        "Receipt Date": "2020-01-01",
        "Description": "Test transaction",
        "Last Name": "TEST",
        "First Name": "USER"
    }
    
    # Use the unified processor
    print(f"Processing record with unified processor...")
    transaction = unified_sql_processor.process_record(record, "oklahoma")
    
    if transaction:
        print(f"✅ Transaction processed: {transaction.transaction_id} - ${transaction.amount}")
        print(f"  Committee ID: {transaction.committee_id}")
        
        # Try to save to database
        try:
            db_manager = create_postgres_database_manager()
            with db_manager.get_session() as session:
                # Save the transaction
                session.add(transaction)
                session.commit()
                session.refresh(transaction)
                print(f"✅ Transaction saved to database with ID: {transaction.id}")
                
        except Exception as e:
            print(f"❌ Error saving transaction: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("❌ No transaction created")

if __name__ == "__main__":
    test_unified_processor() 