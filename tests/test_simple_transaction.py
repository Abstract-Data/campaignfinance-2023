#!/usr/bin/env python3
"""
Simple test to create a transaction with existing committee.
"""

from app.states.unified_sqlmodels import UnifiedSQLModelBuilder
from app.states.postgres_config import create_postgres_database_manager

def test_simple_transaction():
    """Test creating a simple transaction"""
    print("Testing simple transaction creation...")
    
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
        print(f"  Committee ID: {transaction.committee_id}")
        
        # Try to save to database
        try:
            db_manager = create_postgres_database_manager()
            with db_manager.get_session() as session:
                # Check if committee exists
                from app.states.unified_sqlmodels import UnifiedCommittee
                committee = session.get(UnifiedCommittee, "9908")
                if committee:
                    print(f"✅ Found committee: {committee.name}")
                else:
                    print(f"❌ Committee not found")
                
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
    test_simple_transaction() 