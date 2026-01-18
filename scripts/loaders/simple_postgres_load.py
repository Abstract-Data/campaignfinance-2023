#!/usr/bin/env python3
"""
Simple PostgreSQL data loader - basic data loading without complex officer linking.
"""

from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from icecream import ic

from app.states.postgres_config import create_postgres_database_manager, PostgresConfig
from app.funcs.csv_reader import FileReader

def simple_load_to_postgres():
    """Simple data loading into PostgreSQL."""
    console = Console()
    
    console.print("\n[bold blue]Simple PostgreSQL Data Loader[/bold blue]")
    console.print("=" * 40)
    
    # Setup PostgreSQL
    try:
        config = PostgresConfig()
        db_manager = create_postgres_database_manager()
        console.print("[green]✅ PostgreSQL connected successfully![/green]")
    except Exception as e:
        console.print(f"[red]❌ PostgreSQL setup failed: {e}[/red]")
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
    
    console.print(f"[green]Found state directories:[/green]")
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
    console.print(f"Database: campaign_finance")
    console.print(f"Connect with: psql campaign_finance")
    
    # Show some example queries
    console.print(f"\n[yellow]Example queries:[/yellow]")
    console.print(f"  SELECT COUNT(*) FROM unified_transactions;")
    console.print(f"  SELECT state, COUNT(*) FROM unified_transactions GROUP BY state;")
    console.print(f"  SELECT * FROM unified_transactions LIMIT 5;")

if __name__ == "__main__":
    simple_load_to_postgres() 