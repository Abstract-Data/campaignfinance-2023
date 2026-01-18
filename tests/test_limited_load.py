#!/usr/bin/env python3
"""
Test script that loads just a few records to test the pipeline.
"""

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from app.states.postgres_state_loader import load_state_data_to_postgres

def test_limited_load():
    """Test with limited records"""
    console = Console()
    
    console.print(Panel.fit(
        "[bold blue]Limited Record Test Loader[/bold blue]\n"
        "Testing with just 100 records",
        border_style="blue"
    ))
    
    try:
        console.print("[yellow]Testing with Oklahoma data (100 records)...[/yellow]")
        
        # We need to modify the loader to accept max_records
        from app.states.unified_state_loader import UnifiedStateLoader
        from app.states.postgres_config import create_postgres_database_manager
        
        # Create a custom loader
        db_manager = create_postgres_database_manager()
        loader = UnifiedStateLoader("oklahoma", Path("tmp"))
        loader.db_manager = db_manager
        
        # Load with record limit
        result = loader.load_state_data(
            auto_link_officers=False,  # Disable for faster processing
            create_relationships=False,  # Disable for faster processing
            max_records=100  # Limit to 100 records
        )
        
        # Display results
        console.print(f"\n[bold green]✅ Limited Load Complete![/bold green]")
        
        table = Table(title="Test Results - 100 Records")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green")
        
        table.add_row("Total Records", str(result.get('total_records', 0)))
        table.add_row("Transactions", str(result.get('transactions_created', 0)))
        table.add_row("Persons", str(result.get('persons_created', 0)))
        table.add_row("Committees", str(result.get('committees_created', 0)))
        table.add_row("Addresses", str(result.get('addresses_created', 0)))
        table.add_row("Errors", str(result.get('errors', 0)))
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        raise

if __name__ == "__main__":
    test_limited_load() 