#!/usr/bin/env python3
"""
High-performance loader with optimized session management and caching.
"""

from app.core.unified_sqlmodels import unified_sql_processor
from app.states.postgres_config import create_postgres_database_manager
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
import time

def high_performance_loader(batch_size: int = 100, max_records: int = None):
    """High-performance loader with optimized session management"""
    print("🚀 High-performance loader with optimized session management...")
    
    # Find the 2020 file
    from app.funcs.csv_reader import FileReader
    file_reader = FileReader()
    test_file = Path("tmp/oklahoma/2020_ContributionLoanExtract.csv")
    
    if not test_file.exists():
        print("❌ File not found")
        return
    
    print(f"✅ Processing file: {test_file.name}")
    print(f"📦 Batch size: {batch_size}")
    if max_records:
        print(f"📊 Max records: {max_records}")
    
    # Process records
    data_generator = file_reader.read_csv(test_file)
    
    success_count = 0
    error_count = 0
    batch_count = 0
    
    # Create database manager
    db_manager = create_postgres_database_manager()
    
    # Cache for addresses and committees to avoid repeated database queries
    address_cache = {}
    committee_cache = {}
    
    # Process in batches
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    ) as progress:
        
        batch_task = progress.add_task("Processing batches...", total=None)
        record_task = progress.add_task("Processing records...", total=None)
        
        current_batch = []
        
        for i, record in enumerate(data_generator):
            if max_records and i >= max_records:
                break
                
            # Add state and file origin information
            record['state'] = 'oklahoma'
            record['file_origin'] = test_file.name
            
            current_batch.append(record)
            
            # Process batch when it reaches batch_size
            if len(current_batch) >= batch_size:
                batch_success, batch_errors = process_batch_optimized(
                    current_batch, db_manager, address_cache, committee_cache, progress, record_task
                )
                success_count += batch_success
                error_count += batch_errors
                batch_count += 1
                
                progress.update(batch_task, description=f"Processed {batch_count} batches")
                progress.update(record_task, description=f"Processed {success_count + error_count} records")
                
                current_batch = []
        
        # Process remaining records
        if current_batch:
            batch_success, batch_errors = process_batch_optimized(
                current_batch, db_manager, address_cache, committee_cache, progress, record_task
            )
            success_count += batch_success
            error_count += batch_errors
            batch_count += 1
    
    print(f"\n📊 Final Results:")
    print(f"  ✅ Successful: {success_count}")
    print(f"  ❌ Failed: {error_count}")
    print(f"  📦 Batches: {batch_count}")
    print(f"  🗂️ Address cache hits: {len(address_cache)}")
    print(f"  🏛️ Committee cache hits: {len(committee_cache)}")
    
    # Check database
    with db_manager.get_session() as session:
        from sqlalchemy import text
        tx_count = session.exec(text("SELECT COUNT(*) FROM unified_transactions")).first()
        committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
        address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
        
        # Count unique addresses manually
        all_addresses = session.exec(select(UnifiedAddress)).all()
        unique_addresses = set()
        for addr in all_addresses:
            if addr.street_1 and addr.city:  # Only count addresses with required fields
                unique_addresses.add((addr.street_1, addr.city, addr.state, addr.zip_code))
        unique_address_count = len(unique_addresses)
        
        print(f"  📋 Transactions in database: {tx_count}")
        print(f"  📋 Committees in database: {committee_count}")
        print(f"  📋 Total addresses: {address_count}")
        print(f"  📋 Unique addresses: {unique_address_count}")

def process_batch_optimized(records, db_manager, address_cache, committee_cache, progress, record_task):
    """Process a batch of records with optimized session management"""
    batch_success = 0
    batch_errors = 0
    
    with db_manager.get_session() as session:
        # Pre-load existing addresses and committees to avoid repeated queries
        from app.core.unified_sqlmodels import UnifiedAddress, UnifiedCommittee
        from sqlalchemy import select
        
        # Load all existing addresses into cache if not already loaded
        if not address_cache:
            existing_addresses = session.exec(select(UnifiedAddress)).all()
            for addr in existing_addresses:
                if addr.street_1 and addr.city:  # Only cache addresses with required fields
                    key = (addr.street_1, addr.city, addr.state, addr.zip_code)
                    address_cache[key] = addr
        
        # Load all existing committees into cache if not already loaded
        if not committee_cache:
            existing_committees = session.exec(select(UnifiedCommittee)).all()
            for committee in existing_committees:
                committee_cache[committee.filer_id] = committee
        
        for i, record in enumerate(records):
            try:
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
                            else:
                                # Save new address and add to cache
                                session.add(tx_person.person.address)
                                session.flush()
                                address_cache[address_key] = tx_person.person.address
                    
                    # Save the transaction
                    session.add(transaction)
                    batch_success += 1
                else:
                    batch_errors += 1
                    
            except Exception as e:
                batch_errors += 1
                # Don't print every error to avoid spam
        
        # Commit the entire batch
        session.commit()
    
    return batch_success, batch_errors

if __name__ == "__main__":
    # Test with 1000 records
    high_performance_loader(batch_size=100, max_records=1000) 