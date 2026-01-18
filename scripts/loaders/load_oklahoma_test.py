#!/usr/bin/env python3
"""
Load Oklahoma data as a test of the full pipeline.
"""

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from app.states.postgres_state_loader import load_state_data_to_postgres

def load_oklahoma_test():
    """Load Oklahoma data as a test"""
    console = Console()
    
    console.print(Panel.fit(
        "[bold blue]Oklahoma Data Load Test[/bold blue]\n"
        "Testing the full pipeline with Oklahoma data",
        border_style="blue"
    ))
    
    # Load Oklahoma data with a reasonable limit
    try:
        # Create a custom loader with limits
        from app.states.unified_state_loader import UnifiedStateLoader
        from app.states.postgres_config import create_postgres_database_manager
        
        # Create PostgreSQL database manager
        db_manager = create_postgres_database_manager()
        
        # Create loader with limits
        loader = UnifiedStateLoader("oklahoma", Path("tmp"))
        loader.db_manager = db_manager
        
        # Set limits for testing
        loader.max_records_per_file = 1000  # Only process 1000 records per file
        loader.max_files = 3  # Only process 3 files
        
        console.print(f"[yellow]Testing with limits: {loader.max_records_per_file} records per file, {loader.max_files} files[/yellow]")
        
        result = loader.load_state_data(
            auto_link_officers=True,
            create_relationships=True,
            progress_callback=lambda current, total, message: console.print(f"[blue]{message}[/blue]")
        )
        
        # Display results
        console.print(f"\n[bold green]✅ Oklahoma Load Complete![/bold green]")
        
        table = Table(title="Oklahoma Load Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green")
        
        table.add_row("Total Records", str(result.get('total_records', 0)))
        table.add_row("Transactions", str(result.get('transactions_created', 0)))
        table.add_row("Persons", str(result.get('persons_created', 0)))
        table.add_row("Committees", str(result.get('committees_created', 0)))
        table.add_row("Addresses", str(result.get('addresses_created', 0)))
        table.add_row("Errors", str(result.get('errors', 0)))
        
        console.print(table)
        
        # Show some sample data
        console.print(f"\n[bold blue]Sample Data:[/bold blue]")
        console.print(f"• Transactions: {result.get('transactions_created', 0)}")
        console.print(f"• Persons: {result.get('persons_created', 0)}")
        console.print(f"• Committees: {result.get('committees_created', 0)}")
        console.print(f"• Addresses: {result.get('addresses_created', 0)}")
        
    except Exception as e:
        console.print(f"[red]❌ Error loading Oklahoma data: {e}[/red]")
        raise

if __name__ == "__main__":
    load_oklahoma_test() 