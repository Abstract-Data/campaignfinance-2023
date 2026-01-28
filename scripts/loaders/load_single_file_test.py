#!/usr/bin/env python3
"""
Test loader that processes just one file with a record limit.
"""

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from app.states.postgres_state_loader import load_state_data_to_postgres

def load_single_file_test():
    """Load just one file to test the pipeline"""
    console = Console()
    
    console.print(Panel.fit(
        "[bold blue]Single File Test Loader[/bold blue]\n"
        "Testing with one file and limited records",
        border_style="blue"
    ))
    
    # Test with Oklahoma 2020 file only
    try:
        console.print("[yellow]Testing with Oklahoma 2020 data (limited records)...[/yellow]")
        
        # We'll modify the loader to process just one file
        from app.core.unified_state_loader import UnifiedStateLoader
        from app.states.postgres_config import create_postgres_database_manager
        
        # Create a custom loader that only processes one file
        db_manager = create_postgres_database_manager()
        loader = UnifiedStateLoader("oklahoma", Path("tmp"))
        loader.db_manager = db_manager
        
        # Find the 2020 file
        files = loader._discover_data_files()
        test_file = None
        for file in files:
            if "2020" in file.name:
                test_file = file
                break
        
        if not test_file:
            console.print("[red]❌ No 2020 file found[/red]")
            return
        
        console.print(f"[green]✅ Testing with file: {test_file.name}[/green]")
        
        # Process just this file with a record limit
        file_stats = loader._process_data_file(test_file, auto_link_officers=False)
        
        # Display results
        console.print(f"\n[bold green]✅ File Processing Complete![/bold green]")
        
        table = Table(title=f"Test Results - {test_file.name}")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green")
        
        table.add_row("Records Processed", str(file_stats.get('records_processed', 0)))
        table.add_row("Transactions Created", str(file_stats.get('transactions_created', 0)))
        table.add_row("Persons Created", str(file_stats.get('persons_created', 0)))
        table.add_row("Committees Created", str(file_stats.get('committees_created', 0)))
        table.add_row("Addresses Created", str(file_stats.get('addresses_created', 0)))
        table.add_row("Errors", str(file_stats.get('errors', 0)))
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        raise

if __name__ == "__main__":
    load_single_file_test() 