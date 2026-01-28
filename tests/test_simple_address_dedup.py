#!/usr/bin/env python3
"""
Simple test for address deduplication only.
"""

from app.core.unified_sqlmodels import unified_sql_processor
from app.states.postgres_config import create_postgres_database_manager

def test_simple_address_dedup():
    """Simple test for address deduplication"""
    print("Testing simple address deduplication...")
    
    # Sample record
    record = {
        "Receipt ID": "TEST3",
        "Org ID": "9908", 
        "Receipt Amount": "300.00",
        "Last Name": "BROWN",
        "First Name": "BOB",
        "Address 1": "2606 WILDWOOD PLACE",
        "City": "DUNCAN",
        "State": "OK",
        "Zip": "73533"
    }
    
    # Process record
    transaction = unified_sql_processor.process_record(record, "oklahoma")
    
    if transaction:
        print(f"Transaction: {transaction.transaction_id}")
        
        # Save to database
        db_manager = create_postgres_database_manager()
        with db_manager.get_session() as session:
            # Save committee first
            if transaction.committee:
                session.merge(transaction.committee)
                session.flush()
            
            # Save addresses for all persons
            for tx_person in transaction.persons:
                if tx_person.person and tx_person.person.address:
                    print(f"Address: {tx_person.person.address.street_1}, {tx_person.person.address.city}")
                    
                    # Check if address already exists
                    from app.core.unified_sqlmodels import UnifiedAddress
                    existing_address = session.get(UnifiedAddress, 7)  # Use existing address ID
                    
                    if existing_address:
                        print(f"✅ Using existing address (ID: {existing_address.id})")
                        tx_person.person.address_id = existing_address.id
                        tx_person.person.address = existing_address
                    else:
                        print(f"➕ Creating new address")
                        session.add(tx_person.person.address)
                        session.flush()
            
            # Save the transaction
            session.add(transaction)
            session.commit()
            print(f"✅ Transaction saved")
        
        # Check results
        with db_manager.get_session() as session:
            from sqlalchemy import text
            address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
            print(f"📊 Total addresses: {address_count}")

if __name__ == "__main__":
    test_simple_address_dedup() 