#!/usr/bin/env python3
"""
Test script to specifically test address deduplication.
"""

from app.states.unified_sqlmodels import UnifiedSQLModelBuilder
from app.states.postgres_config import create_postgres_database_manager

def test_address_dedup():
    """Test address deduplication"""
    print("Testing address deduplication...")
    
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
    
    # Process each record
    addresses_created = []
    for i, record in enumerate(records):
        print(f"\nProcessing record {i+1}...")
        
        # Build address
        address = builder.build_address(record, "contributor")
        if address:
            print(f"  Address: {address.street_1}, {address.city}, {address.state} {address.zip_code}")
            addresses_created.append(address)
        else:
            print(f"  No address created")
    
    print(f"\n📊 Address Results:")
    print(f"  Total addresses created: {len(addresses_created)}")
    
    # Check if addresses are the same object (deduplication working)
    if len(addresses_created) > 1:
        first_address = addresses_created[0]
        for i, addr in enumerate(addresses_created[1:], 1):
            if addr is first_address:
                print(f"  ✅ Address {i+1} is same object as first (deduplication working)")
            else:
                print(f"  ❌ Address {i+1} is different object (deduplication failed)")
    
    # Try to save addresses to database
    try:
        db_manager = create_postgres_database_manager()
        with db_manager.get_session() as session:
            for i, address in enumerate(addresses_created):
                session.merge(address)
            session.commit()
            print(f"  ✅ Saved {len(addresses_created)} addresses to database")
            
            # Check how many unique addresses are in database
            from sqlalchemy import text
            address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
            print(f"  📋 Unique addresses in database: {address_count}")
            
    except Exception as e:
        print(f"  ❌ Error saving addresses: {e}")

if __name__ == "__main__":
    test_address_dedup() 