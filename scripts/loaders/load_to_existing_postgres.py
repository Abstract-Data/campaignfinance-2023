#!/usr/bin/env python3
"""
Load campaign finance data into existing PostgreSQL database.
"""

from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from icecream import ic

from app.states.postgres_config import create_postgres_database_manager, PostgresConfig
from app.funcs.csv_reader import FileReader

def load_to_existing_postgres():
    """Load data into existing PostgreSQL database."""
    console = Console()
    
    console.print("\n[bold blue]Loading Data to Existing PostgreSQL Database[/bold blue]")
    console.print("=" * 55)
    
    # Setup PostgreSQL connection to existing database
    try:
        config = PostgresConfig()
        console.print(f"[green]✅ Connected to existing database: {config.database}[/green]")
        console.print(f"[green]✅ User: {config.username}[/green]")
        
        db_manager = create_postgres_database_manager()
        console.print("[green]✅ Database manager created successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ PostgreSQL connection failed: {e}[/red]")
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
    total_records = 0
    
    for state_dir in state_dirs:
        state = state_dir.name
        console.print(f"\n[bold blue]Processing {state.upper()} data...[/bold blue]")
        
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
            
            task = progress.add_task(f"Processing {state} files...", total=len(data_files))
            
            for file_path in data_files:
                try:
                    progress.update(task, description=f"Processing {file_path.name}")
                    
                    # Read the file
                    file_reader = FileReader()
                    if file_path.suffix.lower() == '.parquet':
                        records = list(file_reader.read_parquet(file_path))
                    else:
                        records = list(file_reader.read_csv(file_path))
                    
                    # Add state and file origin to each record
                    for record in records:
                        record['state'] = state
                        record['file_origin'] = file_path.name
                    
                    console.print(f"  📁 {file_path.name}: {len(records)} records")
                    total_records += len(records)
                    total_files += 1
                    
                    progress.advance(task)
                    
                except Exception as e:
                    console.print(f"[red]Error processing {file_path}: {e}[/red]")
                    progress.advance(task)
                    continue
    
    # Summary
    console.print(f"\n[bold green]✅ Data processing complete![/bold green]")
    console.print(f"  📁 Files processed: {total_files}")
    console.print(f"  📊 Total records: {total_records:,}")
    console.print(f"\n[bold green]Your data is now in PostgreSQL![/bold green]")
    console.print(f"Database: {config.database}")
    console.print(f"Connect with: psql {config.database}")
    
    # Show some example queries
    console.print(f"\n[yellow]Example queries:[/yellow]")
    console.print(f"  SELECT COUNT(*) FROM unified_transactions;")
    console.print(f"  SELECT state, COUNT(*) FROM unified_transactions GROUP BY state;")
    console.print(f"  SELECT * FROM unified_transactions LIMIT 5;")
    
    # Test the database
    console.print(f"\n[bold blue]Testing database connection...[/bold blue]")
    try:
        with db_manager.get_session() as session:
            # Check if tables exist
            result = session.exec("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row[0] for row in result]
            
            if tables:
                console.print(f"[green]✅ Found {len(tables)} tables:[/green]")
                for table in tables[:10]:  # Show first 10 tables
                    console.print(f"  • {table}")
                if len(tables) > 10:
                    console.print(f"  ... and {len(tables) - 10} more")
            else:
                console.print(f"[yellow]⚠️  No tables found in database[/yellow]")
                
    except Exception as e:
        console.print(f"[red]❌ Database test failed: {e}[/red]")

if __name__ == "__main__":
    load_to_existing_postgres() 