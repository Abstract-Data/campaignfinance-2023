#!/usr/bin/env python3
"""
Debug script to test address deduplication step by step.
"""

from app.states.unified_sqlmodels import unified_sql_processor
from app.states.postgres_config import create_postgres_database_manager

def debug_address_dedup():
    """Debug address deduplication"""
    print("🔍 Debugging address deduplication...")
    
    # Sample record
    record = {
        "Receipt ID": "TEST1",
        "Org ID": "9908", 
        "Receipt Amount": "100.00",
        "Last Name": "SMITH",
        "First Name": "JOHN",
        "Address 1": "2606 WILDWOOD PLACE",
        "City": "DUNCAN",
        "State": "OK",
        "Zip": "73533"
    }
    
    # Process record
    transaction = unified_sql_processor.process_record(record, "oklahoma")
    
    if transaction:
        print(f"✅ Transaction created: {transaction.transaction_id}")
        
        # Check if transaction has persons with addresses
        for i, tx_person in enumerate(transaction.persons):
            if tx_person.person and tx_person.person.address:
                print(f"  Person {i+1} has address: {tx_person.person.address.street_1}, {tx_person.person.address.city}")
                
                # Test address deduplication logic
                db_manager = create_postgres_database_manager()
                with db_manager.get_session() as session:
                    from app.states.unified_sqlmodels import UnifiedAddress
                    from sqlalchemy import select
                    
                    # Check if address already exists using raw SQL
                    from sqlalchemy import text
                    query = text("SELECT id FROM unified_addresses WHERE street_1 = :street_1 AND city = :city AND state = :state AND zip_code = :zip_code LIMIT 1")
                    existing_address = session.exec(query, {
                        "street_1": tx_person.person.address.street_1,
                        "city": tx_person.person.address.city,
                        "state": tx_person.person.address.state,
                        "zip_code": tx_person.person.address.zip_code
                    }).first()
                    
                    if existing_address:
                        print(f"  ✅ Found existing address (ID: {existing_address.id})")
                    else:
                        print(f"  ➕ No existing address found - will create new one")
                        
                        # Save new address
                        session.add(tx_person.person.address)
                        session.flush()
                        print(f"  ✅ Created new address (ID: {tx_person.person.address.id})")
                        
                        # Check again
                        query = text("SELECT id FROM unified_addresses WHERE street_1 = :street_1 AND city = :city AND state = :state AND zip_code = :zip_code LIMIT 1")
                        existing_address = session.exec(query, {
                            "street_1": tx_person.person.address.street_1,
                            "city": tx_person.person.address.city,
                            "state": tx_person.person.address.state,
                            "zip_code": tx_person.person.address.zip_code
                        }).first()
                        
                        if existing_address:
                            print(f"  ✅ Address now exists (ID: {existing_address.id})")
                        else:
                            print(f"  ❌ Address still not found after creation")
                    
                    session.commit()
    else:
        print("❌ No transaction created")

if __name__ == "__main__":
    debug_address_dedup() 