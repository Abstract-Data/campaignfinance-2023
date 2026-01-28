#!/usr/bin/env python3
"""
Simple loader that uses the unified processor directly.
"""

from app.core.unified_sqlmodels import unified_sql_processor
from app.states.postgres_config import create_postgres_database_manager
from pathlib import Path

def simple_loader():
    """Simple loader using unified processor directly"""
    print("Simple loader using unified processor...")
    
    # Find the 2020 file
    from app.funcs.csv_reader import FileReader
    file_reader = FileReader()
    test_file = Path("tmp/oklahoma/2020_ContributionLoanExtract.csv")
    
    if not test_file.exists():
        print("❌ File not found")
        return
    
    print(f"✅ Testing with file: {test_file.name}")
    
    # Process records
    data_generator = file_reader.read_csv(test_file)
    
    success_count = 0
    error_count = 0
    
    for i, record in enumerate(data_generator):
        if i >= 10:  # Only process 10 records
            break
            
        try:
            # Add state and file origin information
            record['state'] = 'oklahoma'
            record['file_origin'] = test_file.name
            
            # Use unified processor directly
            transaction = unified_sql_processor.process_record(record, "oklahoma")
            
            if transaction:
                # Save to database with all related entities
                db_manager = create_postgres_database_manager()
                with db_manager.get_session() as session:
                    # Save committee first if it exists
                    if transaction.committee:
                        session.merge(transaction.committee)
                        session.flush()
                    
                    # Save addresses for all persons
                    for tx_person in transaction.persons:
                        if tx_person.person and tx_person.person.address:
                            # Check if address already exists in database using SQLModel
                            from app.core.unified_sqlmodels import UnifiedAddress
                            from sqlalchemy import select
                            existing_address = session.exec(
                                select(UnifiedAddress).where(
                                    UnifiedAddress.street_1 == tx_person.person.address.street_1,
                                    UnifiedAddress.city == tx_person.person.address.city,
                                    UnifiedAddress.state == tx_person.person.address.state,
                                    UnifiedAddress.zip_code == tx_person.person.address.zip_code
                                )
                            ).first()
                            
                            if existing_address:
                                # Use existing address
                                tx_person.person.address_id = existing_address.id
                                tx_person.person.address = existing_address
                            else:
                                # Save new address
                                session.add(tx_person.person.address)
                                session.flush()
                    
                    # Save the transaction
                    session.add(transaction)
                    session.commit()
                    session.refresh(transaction)
                
                success_count += 1
                print(f"  ✅ Record {i+1}: Transaction {transaction.transaction_id} created")
            else:
                error_count += 1
                print(f"  ❌ Record {i+1}: Failed to create transaction")
                
        except Exception as e:
            error_count += 1
            print(f"  ❌ Record {i+1}: Error - {e}")
    
    print(f"\n📊 Results:")
    print(f"  ✅ Successful: {success_count}")
    print(f"  ❌ Failed: {error_count}")
    
    # Check database
    db_manager = create_postgres_database_manager()
    with db_manager.get_session() as session:
        from sqlalchemy import text
        tx_count = session.exec(text("SELECT COUNT(*) FROM unified_transactions")).first()
        committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
        address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
        print(f"  📋 Transactions in database: {tx_count}")
        print(f"  📋 Committees in database: {committee_count}")
        print(f"  📋 Addresses in database: {address_count}")

if __name__ == "__main__":
    simple_loader() 