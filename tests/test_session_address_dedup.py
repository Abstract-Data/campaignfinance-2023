#!/usr/bin/env python3
"""
Test script to verify address deduplication within the same session.
"""

from app.states.unified_sqlmodels import UnifiedSQLModelBuilder
from app.states.postgres_config import create_postgres_database_manager

def test_session_address_dedup():
    """Test address deduplication within the same session"""
    print("Testing address deduplication within session...")
    
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
        },
        {
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
    ]
    
    # Create builder
    builder = UnifiedSQLModelBuilder("oklahoma")
    
    # Process addresses in the same session
    try:
        db_manager = create_postgres_database_manager()
        with db_manager.get_session() as session:
            addresses_created = []
            
            for i, record in enumerate(records):
                print(f"\nProcessing record {i+1}...")
                
                # Build address
                address = builder.build_address(record, "contributor")
                if address:
                    print(f"  Address: {address.street_1}, {address.city}, {address.state} {address.zip_code}")
                    
                    # Check if this address already exists in the session
                    from app.states.unified_sqlmodels import UnifiedAddress
                    from sqlalchemy import select
                    existing_address = session.exec(
                        select(UnifiedAddress).where(
                            UnifiedAddress.street_1 == address.street_1,
                            UnifiedAddress.city == address.city,
                            UnifiedAddress.state == address.state,
                            UnifiedAddress.zip_code == address.zip_code
                        )
                    ).first()
                    
                    if existing_address:
                        print(f"  ✅ Found existing address in session (ID: {existing_address.id})")
                        addresses_created.append(existing_address)
                    else:
                        print(f"  ➕ Creating new address")
                        session.add(address)
                        session.flush()  # Flush to get the ID
                        addresses_created.append(address)
                else:
                    print(f"  No address created")
            
            # Commit the session
            session.commit()
            print(f"\n📊 Session Results:")
            print(f"  Total addresses in session: {len(addresses_created)}")
            
            # Check how many unique addresses are in database
            from sqlalchemy import text
            address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
            print(f"  📋 Total addresses in database: {address_count}")
            
            # Check unique addresses
            unique_count = session.exec(text("SELECT COUNT(DISTINCT street_1, city, state, zip_code) FROM unified_addresses")).first()
            print(f"  📋 Unique addresses in database: {unique_count}")
            
    except Exception as e:
        print(f"  ❌ Error: {e}")

if __name__ == "__main__":
    test_session_address_dedup() 