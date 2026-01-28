#!/usr/bin/env python3
"""
Optimized loader for larger datasets with batch processing and proper deduplication.
"""

from app.core.unified_sqlmodels import unified_sql_processor
from app.states.postgres_config import create_postgres_database_manager
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
import time

def optimized_loader(batch_size: int = 100, max_records: int = None):
    """Optimized loader with batch processing"""
    print("🚀 Optimized loader with batch processing...")
    
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
                batch_success, batch_errors = process_batch(current_batch, db_manager, progress, record_task)
                success_count += batch_success
                error_count += batch_errors
                batch_count += 1
                
                progress.update(batch_task, description=f"Processed {batch_count} batches")
                progress.update(record_task, description=f"Processed {success_count + error_count} records")
                
                current_batch = []
        
        # Process remaining records
        if current_batch:
            batch_success, batch_errors = process_batch(current_batch, db_manager, progress, record_task)
            success_count += batch_success
            error_count += batch_errors
            batch_count += 1
    
    print(f"\n📊 Final Results:")
    print(f"  ✅ Successful: {success_count}")
    print(f"  ❌ Failed: {error_count}")
    print(f"  📦 Batches: {batch_count}")
    
    # Check database
    with db_manager.get_session() as session:
        from sqlalchemy import text
        tx_count = session.exec(text("SELECT COUNT(*) FROM unified_transactions")).first()
        committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
        address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
        # Count unique addresses manually since PostgreSQL doesn't support COUNT(DISTINCT multiple_columns)
        all_addresses = session.exec(text("SELECT street_1, city, state, zip_code FROM unified_addresses")).all()
        unique_addresses = set()
        for addr in all_addresses:
            unique_addresses.add((addr.street_1, addr.city, addr.state, addr.zip_code))
        unique_address_count = len(unique_addresses)
        print(f"  📋 Transactions in database: {tx_count}")
        print(f"  📋 Committees in database: {committee_count}")
        print(f"  📋 Total addresses: {address_count}")
        print(f"  📋 Unique addresses: {unique_address_count}")

def process_batch(records, db_manager, progress, record_task):
    """Process a batch of records"""
    batch_success = 0
    batch_errors = 0
    
    with db_manager.get_session() as session:
        for i, record in enumerate(records):
            try:
                # Use unified processor
                transaction = unified_sql_processor.process_record(record, "oklahoma")
                
                if transaction:
                    # Save committee first if it exists
                    if transaction.committee:
                        session.merge(transaction.committee)
                        session.flush()
                    
                    # Save addresses for all persons with deduplication
                    for tx_person in transaction.persons:
                        if tx_person.person and tx_person.person.address:
                            # Check if address already exists in database
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
    # Test with 500 records first
    optimized_loader(batch_size=50, max_records=500) 