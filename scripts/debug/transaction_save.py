#!/usr/bin/env python3
"""
Debug script to test transaction saving in a single session.
"""

from app.core.unified_sqlmodels import UnifiedSQLModelBuilder
from app.states.postgres_config import create_postgres_database_manager

def debug_transaction_save():
    """Debug transaction saving"""
    print("Testing transaction saving...")
    
    # Sample record
    record = {
        "Receipt ID": "TEST123",
        "Org ID": "9908", 
        "Receipt Amount": "100.00",
        "Receipt Date": "2020-01-01",
        "Description": "Test transaction",
        "Last Name": "TEST",
        "First Name": "USER"
    }
    
    # Create builder
    builder = UnifiedSQLModelBuilder("oklahoma")
    
    # Build transaction
    print(f"Building transaction from record...")
    transaction = builder.build_transaction(record)
    
    if transaction:
        print(f"✅ Transaction built: {transaction.transaction_id} - ${transaction.amount}")
        
        # Try to save to database
        try:
            db_manager = create_postgres_database_manager()
            with db_manager.get_session() as session:
                session.add(transaction)
                session.commit()
                session.refresh(transaction)
                print(f"✅ Transaction saved to database with ID: {transaction.id}")
                
                # Check if it's still there
                saved_transaction = session.get(type(transaction), transaction.id)
                if saved_transaction:
                    print(f"✅ Transaction retrieved from database: {saved_transaction.transaction_id}")
                else:
                    print(f"❌ Transaction not found in database after save")
                    
        except Exception as e:
            print(f"❌ Error saving transaction: {e}")
    else:
        print("❌ No transaction created")

if __name__ == "__main__":
    debug_transaction_save() 