#!/usr/bin/env python3
"""
Test the unified loader with a small sample of data.
"""

from pathlib import Path
from rich.console import Console
from sqlalchemy import text

from app.states.postgres_config import create_postgres_database_manager, PostgresConfig
from app.funcs.csv_reader import FileReader
from app.core.unified_sqlmodels import unified_sql_processor

def test_unified_loader():
    """Test the unified loader with a small sample."""
    console = Console()
    
    console.print("\n[bold blue]Testing Unified Loader[/bold blue]")
    console.print("=" * 30)
    
    # Setup PostgreSQL
    try:
        config = PostgresConfig()
        console.print(f"[green]✅ Connected to database: {config.database}[/green]")
        
        db_manager = create_postgres_database_manager()
        console.print("[green]✅ Database manager created successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Setup failed: {e}[/red]")
        return
    
    # Find a file with data to test with
    data_path = Path("tmp")
    if not data_path.exists():
        console.print(f"[red]❌ Data directory {data_path} does not exist![/red]")
        return
    
    # Look for a file in Oklahoma
    oklahoma_path = data_path / "oklahoma"
    if not oklahoma_path.exists():
        console.print(f"[red]❌ Oklahoma directory does not exist![/red]")
        return
    
    # Find a file with data
    files = list(oklahoma_path.glob("*.csv"))
    if not files:
        console.print(f"[red]❌ No CSV files found in Oklahoma![/red]")
        return
    
    # Try to find a file with data by checking file sizes
    test_file = None
    for file in files:
        if file.stat().st_size > 1000:  # Look for files larger than 1KB
            test_file = file
            break
    
    if not test_file:
        console.print(f"[red]❌ No files with data found![/red]")
        return
    
    console.print(f"[blue]Testing with file: {test_file.name}[/blue]")
    
    # Read the file
    try:
        file_reader = FileReader()
        records = list(file_reader.read_csv(test_file))
        
        console.print(f"[green]✅ Read {len(records)} records from {test_file.name}[/green]")
        
        if len(records) == 0:
            console.print(f"[red]❌ No records found in {test_file.name}[/red]")
            return
        
        # Test with first 5 records
        test_records = records[:5]
        console.print(f"[blue]Testing with {len(test_records)} records...[/blue]")
        
        # Show sample record structure
        console.print(f"[blue]Sample record structure:[/blue]")
        sample_keys = list(test_records[0].keys())[:10]  # Show first 10 keys
        console.print(f"  Keys: {sample_keys}")
        
        success_count = 0
        
        for i, record in enumerate(test_records):
            try:
                # Add metadata
                record['state'] = 'oklahoma'
                record['file_origin'] = test_file.name
                
                # Process record
                transaction = unified_sql_processor.process_record(record, 'oklahoma')
                
                if transaction:
                    # Save to database
                    with db_manager.get_session() as session:
                        session.add(transaction)
                        session.commit()
                    
                    console.print(f"[green]✅ Successfully processed record {i+1}[/green]")
                    success_count += 1
                else:
                    console.print(f"[yellow]⚠️ Record {i+1} returned None[/yellow]")
                    
            except Exception as e:
                console.print(f"[red]❌ Error processing record {i+1}: {e}[/red]")
                continue
        
        console.print(f"\n[bold green]Test Results:[/bold green]")
        console.print(f"  📊 Records tested: {len(test_records)}")
        console.print(f"  ✅ Successful: {success_count}")
        console.print(f"  ❌ Failed: {len(test_records) - success_count}")
        
        if success_count > 0:
            # Verify data was saved
            with db_manager.get_session() as session:
                result = session.exec(text("SELECT COUNT(*) FROM unified_transactions"))
                total_count = result.first()[0]
                console.print(f"[green]✅ Total transactions in database: {total_count}[/green]")
                
                # Show sample data
                result = session.exec(text("SELECT id, transaction_id, amount, state FROM unified_transactions ORDER BY id DESC LIMIT 3"))
                console.print(f"[blue]Sample transactions:[/blue]")
                for row in result:
                    console.print(f"  • ID: {row[0]}, Transaction ID: {row[1]}, Amount: {row[2]}, State: {row[3]}")
        
    except Exception as e:
        console.print(f"[red]❌ Error reading file: {e}[/red]")

if __name__ == "__main__":
    test_unified_loader() 