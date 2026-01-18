#!/usr/bin/env python3
"""
Test to verify address deduplication is working.
"""

from app.states.unified_sqlmodels import unified_sql_processor, UnifiedAddress
from app.states.postgres_config import create_postgres_database_manager

def test_address_dedup_fix():
    """Test address deduplication"""
    print("Testing address deduplication fix...")
    
    # Sample records with same address
    records = [
        {
            "Receipt ID": "TEST1",
            "Org ID": "9908", 
            "Receipt Amount": "100.00",
            "Last Name": "SMITH",
            "First Name": "JOHN",
            "Address 1": "2606 WILDWOOD PLACE",
            "City": "DUNCAN",
            "State": "OK",
            "Zip": "73533"
        },
        {
            "Receipt ID": "TEST2",
            "Org ID": "9908", 
            "Receipt Amount": "200.00",
            "Last Name": "JONES",
            "First Name": "JANE",
            "Address 1": "2606 WILDWOOD PLACE",
            "City": "DUNCAN",
            "State": "OK",
            "Zip": "73533"
        }
    ]
    
    # Process records in the same session
    db_manager = create_postgres_database_manager()
    with db_manager.get_session() as session:
        for i, record in enumerate(records):
            print(f"\nProcessing record {i+1}...")
            
            # Use unified processor
            transaction = unified_sql_processor.process_record(record, "oklahoma")
            
            if transaction:
                print(f"  Transaction: {transaction.transaction_id}")
                
                # Save committee first if it exists
                if transaction.committee:
                    session.merge(transaction.committee)
                    session.flush()
                
                # Save addresses for all persons
                for tx_person in transaction.persons:
                    if tx_person.person and tx_person.person.address:
                        print(f"  Address: {tx_person.person.address.street_1}, {tx_person.person.address.city}")
                        
                        # Check if address already exists in session
                        existing_address = None
                        for addr in session.query(UnifiedAddress).filter(
                            UnifiedAddress.street_1 == tx_person.person.address.street_1,
                            UnifiedAddress.city == tx_person.person.address.city,
                            UnifiedAddress.state == tx_person.person.address.state,
                            UnifiedAddress.zip_code == tx_person.person.address.zip_code
                        ).all():
                            existing_address = addr
                            break
                        
                        if existing_address:
                            print(f"  ✅ Found existing address (ID: {existing_address.id})")
                            tx_person.person.address_id = existing_address.id
                            tx_person.person.address = existing_address
                        else:
                            print(f"  ➕ Creating new address")
                            session.add(tx_person.person.address)
                            session.flush()
                
                # Save the transaction
                session.add(transaction)
        
        # Commit everything
        session.commit()
        print(f"\n✅ All records processed and committed")
        
        # Check results
        from sqlalchemy import text
        address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
        unique_address_count = session.exec(text("SELECT COUNT(DISTINCT street_1, city, state, zip_code) FROM unified_addresses")).first()
        print(f"📊 Total addresses: {address_count}")
        print(f"📊 Unique addresses: {unique_address_count}")

if __name__ == "__main__":
    test_address_dedup_fix() 