#!/usr/bin/env python3
"""
Simple test to create addresses.
"""

from app.states.unified_sqlmodels import UnifiedSQLModelBuilder
from app.states.postgres_config import create_postgres_database_manager

def simple_address_test():
    """Simple address creation test"""
    print("Testing simple address creation...")
    
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
    
    # Create builder
    builder = UnifiedSQLModelBuilder("oklahoma")
    
    # Build address
    print(f"Building address from record...")
    address = builder.build_address(record, "contributor")
    
    if address:
        print(f"✅ Address built: {address.street_1}, {address.city}, {address.state} {address.zip_code}")
        
        # Try to save to database
        try:
            db_manager = create_postgres_database_manager()
            with db_manager.get_session() as session:
                session.add(address)
                session.commit()
                session.refresh(address)
                print(f"✅ Address saved to database with ID: {address.id}")
                
                # Check if it's still there
                from app.states.unified_sqlmodels import UnifiedAddress
                saved_address = session.get(UnifiedAddress, address.id)
                if saved_address:
                    print(f"✅ Address retrieved from database: {saved_address.street_1}, {saved_address.city}")
                else:
                    print(f"❌ Address not found in database after save")
                    
        except Exception as e:
            print(f"❌ Error saving address: {e}")
    else:
        print("❌ No address created")

if __name__ == "__main__":
    simple_address_test() 