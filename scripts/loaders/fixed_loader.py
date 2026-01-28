#!/usr/bin/env python3
"""
Fixed loader with proper address deduplication using global cache.
"""

from app.core.unified_sqlmodels import unified_sql_processor
from app.states.postgres_config import create_postgres_database_manager
from pathlib import Path

def fixed_loader():
    """Fixed loader with proper address deduplication"""
    print("🔧 Fixed loader with proper address deduplication...")
    
    # Find the 2020 file
    from app.funcs.csv_reader import FileReader
    file_reader = FileReader()
    test_file = Path("tmp/oklahoma/2020_ContributionLoanExtract.csv")
    
    if not test_file.exists():
        print("❌ File not found")
        return
    
    print(f"✅ Processing file: {test_file.name}")
    
    # Process records
    data_generator = file_reader.read_csv(test_file)
    
    success_count = 0
    error_count = 0
    
    # Create database manager
    db_manager = create_postgres_database_manager()
    
    # Global cache for addresses and committees
    address_cache = {}
    committee_cache = {}
    
    # Process records in batches with proper deduplication
    with db_manager.get_session() as session:
        # Pre-load existing addresses and committees
        from app.core.unified_sqlmodels import UnifiedAddress, UnifiedCommittee
        from sqlalchemy import select
        
        print("📋 Loading existing addresses and committees...")
        
        # Load existing addresses using raw SQL
        from sqlalchemy import text
        existing_addresses = session.exec(text("SELECT id, street_1, city, state, zip_code FROM unified_addresses")).all()
        for addr in existing_addresses:
            if addr.street_1 and addr.city:
                key = (addr.street_1, addr.city, addr.state, addr.zip_code)
                # Get the full address object
                full_addr = session.get(UnifiedAddress, addr.id)
                address_cache[key] = full_addr
        
        # Load existing committees using raw SQL
        existing_committees = session.exec(text("SELECT filer_id FROM unified_committees")).all()
        for committee in existing_committees:
            # Get the full committee object
            full_committee = session.get(UnifiedCommittee, committee.filer_id)
            committee_cache[full_committee.filer_id] = full_committee
        
        print(f"  📍 Loaded {len(address_cache)} addresses")
        print(f"  🏛️ Loaded {len(committee_cache)} committees")
        
        # Process records
        for i, record in enumerate(data_generator):
            if i >= 50:  # Process 50 records for testing
                break
                
            try:
                # Add state and file origin information
                record['state'] = 'oklahoma'
                record['file_origin'] = test_file.name
                
                # Use unified processor
                transaction = unified_sql_processor.process_record(record, "oklahoma")
                
                if transaction:
                    # Handle committee with caching
                    if transaction.committee:
                        if transaction.committee.filer_id in committee_cache:
                            # Use existing committee
                            existing_committee = committee_cache[transaction.committee.filer_id]
                            transaction.committee_id = existing_committee.filer_id
                            transaction.committee = existing_committee
                        else:
                            # Save new committee and add to cache
                            session.merge(transaction.committee)
                            session.flush()
                            committee_cache[transaction.committee.filer_id] = transaction.committee
                    
                    # Handle addresses with caching
                    for tx_person in transaction.persons:
                        if tx_person.person and tx_person.person.address:
                            address_key = (
                                tx_person.person.address.street_1,
                                tx_person.person.address.city,
                                tx_person.person.address.state,
                                tx_person.person.address.zip_code
                            )
                            
                            if address_key in address_cache:
                                # Use existing address
                                existing_address = address_cache[address_key]
                                tx_person.person.address_id = existing_address.id
                                tx_person.person.address = existing_address
                                print(f"  ✅ Reused address: {existing_address.street_1}, {existing_address.city}")
                            else:
                                # Save new address and add to cache
                                session.add(tx_person.person.address)
                                session.flush()
                                address_cache[address_key] = tx_person.person.address
                                print(f"  ➕ Created address: {tx_person.person.address.street_1}, {tx_person.person.address.city}")
                    
                    # Save the transaction
                    session.add(transaction)
                    success_count += 1
                    print(f"  ✅ Record {i+1}: Transaction {transaction.transaction_id} created")
                else:
                    error_count += 1
                    print(f"  ❌ Record {i+1}: Failed to create transaction")
                    
            except Exception as e:
                error_count += 1
                print(f"  ❌ Record {i+1}: Error - {e}")
        
        # Commit everything
        session.commit()
        print(f"\n✅ All records processed and committed")
    
    print(f"\n📊 Final Results:")
    print(f"  ✅ Successful: {success_count}")
    print(f"  ❌ Failed: {error_count}")
    print(f"  🗂️ Address cache size: {len(address_cache)}")
    print(f"  🏛️ Committee cache size: {len(committee_cache)}")
    
    # Check database
    with db_manager.get_session() as session:
        from sqlalchemy import text
        tx_count = session.exec(text("SELECT COUNT(*) FROM unified_transactions")).first()
        committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
        address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
        
        print(f"  📋 Transactions in database: {tx_count}")
        print(f"  📋 Committees in database: {committee_count}")
        print(f"  📋 Addresses in database: {address_count}")

if __name__ == "__main__":
    fixed_loader() 