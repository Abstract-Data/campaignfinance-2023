#!/usr/bin/env python3
"""
Load actual campaign finance data into PostgreSQL tables using unified models.
"""

from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from sqlalchemy import text
from icecream import ic

from app.states.postgres_config import create_postgres_database_manager, PostgresConfig
from app.funcs.csv_reader import FileReader
from app.core.unified_sqlmodels import unified_sql_processor

def load_actual_data_to_postgres():
    """Load actual data into PostgreSQL tables using unified models."""
    console = Console()
    
    console.print("\n[bold blue]Loading Actual Data to PostgreSQL[/bold blue]")
    console.print("=" * 45)
    
    # Setup PostgreSQL
    try:
        config = PostgresConfig()
        console.print(f"[green]✅ Connected to database: {config.database}[/green]")
        
        db_manager = create_postgres_database_manager()
        console.print("[green]✅ Database manager created successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Setup failed: {e}[/red]")
        return
    
    # Verify tables exist
    console.print(f"\n[bold blue]Verifying database tables...[/bold blue]")
    try:
        with db_manager.get_session() as session:
            result = session.exec(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
            tables = [row[0] for row in result]
            
            console.print(f"[green]✅ Found {len(tables)} tables:[/green]")
            for table in tables:
                console.print(f"  • {table}")
                
    except Exception as e:
        console.print(f"[red]❌ Database verification failed: {e}[/red]")
        return
    
    # Data directory
    data_path = Path("tmp")
    if not data_path.exists():
        console.print(f"[red]❌ Data directory {data_path} does not exist![/red]")
        return
    
    # Find state directories
    state_dirs = [d for d in data_path.iterdir() if d.is_dir()]
    if not state_dirs:
        console.print(f"[red]❌ No state directories found in {data_path}[/red]")
        return
    
    console.print(f"\n[green]Found state directories:[/green]")
    for state_dir in state_dirs:
        console.print(f"  • {state_dir.name}")
    
    # Load data for each state
    total_files = 0
    total_transactions = 0
    total_persons = 0
    total_committees = 0
    
    for state_dir in state_dirs:
        state = state_dir.name
        console.print(f"\n[bold blue]Loading {state.upper()} data into database...[/bold blue]")
        
        # Find all data files
        data_files = []
        for pattern in ["*.parquet", "*.csv"]:
            data_files.extend(state_dir.glob(pattern))
        
        if not data_files:
            console.print(f"[yellow]No data files found in {state_dir}[/yellow]")
            continue
        
        console.print(f"Found {len(data_files)} files")
        
        # Process each file
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task(f"Loading {state} data...", total=len(data_files))
            
            for file_path in data_files:
                try:
                    progress.update(task, description=f"Loading {file_path.name}")
                    
                    # Read the file
                    file_reader = FileReader()
                    if file_path.suffix.lower() == '.parquet':
                        records = list(file_reader.read_parquet(file_path))
                    else:
                        records = list(file_reader.read_csv(file_path))
                    
                    console.print(f"  📁 {file_path.name}: {len(records)} records")
                    
                    # Process records in batches to avoid memory issues
                    batch_size = 1000
                    file_transactions = 0
                    
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        
                        # Add state and file origin to each record
                        for record in batch:
                            record['state'] = state
                            record['file_origin'] = file_path.name
                        
                        # Process batch using unified processor
                        try:
                            transactions = unified_sql_processor.process_records(batch, state)
                            
                            # Save transactions to database
                            if transactions:
                                with db_manager.get_session() as session:
                                    for transaction in transactions:
                                        session.add(transaction)
                                    session.commit()
                                
                                file_transactions += len(transactions)
                                
                        except Exception as e:
                            console.print(f"[red]Error processing batch: {e}[/red]")
                            continue
                    
                    console.print(f"    ✅ Loaded {file_transactions} transactions")
                    total_transactions += file_transactions
                    total_files += 1
                    
                    progress.advance(task)
                    
                except Exception as e:
                    console.print(f"[red]Error processing {file_path}: {e}[/red]")
                    progress.advance(task)
                    continue
    
    # Summary
    console.print(f"\n[bold green]✅ Data loading complete![/bold green]")
    console.print(f"  📁 Files processed: {total_files}")
    console.print(f"  💰 Transactions loaded: {total_transactions:,}")
    console.print(f"  👥 Persons created: {total_persons:,}")
    console.print(f"  🏛️ Committees created: {total_committees:,}")
    console.print(f"\n[bold green]Your data is now in PostgreSQL![/bold green]")
    console.print(f"Database: {config.database}")
    console.print(f"Connect with: psql {config.database}")
    
    # Verify data was loaded
    console.print(f"\n[bold blue]Verifying loaded data...[/bold blue]")
    try:
        with db_manager.get_session() as session:
            # Check transaction count
            result = session.exec(text("SELECT COUNT(*) FROM unified_transactions"))
            transaction_count = result.first()[0]
            console.print(f"[green]✅ Transactions in database: {transaction_count:,}[/green]")
            
            # Check by state
            result = session.exec(text("SELECT state, COUNT(*) FROM unified_transactions GROUP BY state"))
            for row in result:
                console.print(f"  • {row[0]}: {row[1]:,} transactions")
                
    except Exception as e:
        console.print(f"[red]❌ Data verification failed: {e}[/red]")
    
    # Show some example queries
    console.print(f"\n[yellow]Example queries:[/yellow]")
    console.print(f"  SELECT COUNT(*) FROM unified_transactions;")
    console.print(f"  SELECT state, COUNT(*) FROM unified_transactions GROUP BY state;")
    console.print(f"  SELECT * FROM unified_transactions LIMIT 5;")

if __name__ == "__main__":
    load_actual_data_to_postgres() 