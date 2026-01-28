#!/usr/bin/env python3
"""
Batched data loader for campaign finance data.
Processes files in smaller chunks with batch commits for efficiency.
"""

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
import time

def load_data_batched():
    """Load campaign finance data in batches"""
    console = Console()
    
    console.print(Panel.fit(
        "[bold blue]Batched Campaign Finance Data Loader[/bold blue]\n"
        "Loading data in efficient batches",
        border_style="blue"
    ))
    
    # Find all state directories
    tmp_dir = Path("tmp")
    state_dirs = [d for d in tmp_dir.iterdir() if d.is_dir() and d.name not in [".git", "__pycache__"]]
    
    if not state_dirs:
        console.print("[red]❌ No state directories found in tmp/[/red]")
        return
    
    console.print(f"[green]Found {len(state_dirs)} state directories:[/green]")
    for state_dir in state_dirs:
        console.print(f"  • {state_dir.name}")
    
    # Load each state
    for state_dir in state_dirs:
        state_name = state_dir.name
        console.print(f"\n[bold yellow]Loading {state_name.upper()} data...[/bold yellow]")
        
        try:
            # Import here to avoid circular imports
            from app.core.unified_state_loader import UnifiedStateLoader
            from app.states.postgres_config import create_postgres_database_manager
            
            # Create loader
            db_manager = create_postgres_database_manager()
            loader = UnifiedStateLoader(state_name, tmp_dir)
            loader.db_manager = db_manager
            
            # Load with limited records for testing
            console.print(f"[blue]Loading first 1000 records from {state_name}...[/blue]")
            
            result = loader.load_state_data(
                auto_link_officers=False,  # Disable for faster processing
                create_relationships=False,  # Disable for faster processing
                progress_callback=None,
                max_records=1000  # Limit to 1000 records
            )
            
            # Display state summary
            table = Table(title=f"{state_name.upper()} Load Results")
            table.add_column("Metric", style="cyan")
            table.add_column("Count", style="green")
            
            summary = result.get('summary', {})
            table.add_row("Files Processed", str(summary.get('total_files_processed', 0)))
            table.add_row("Transactions", str(summary.get('total_transactions', 0)))
            table.add_row("Persons", str(summary.get('total_persons', 0)))
            table.add_row("Committees", str(summary.get('total_committees', 0)))
            table.add_row("Addresses", str(summary.get('total_addresses', 0)))
            table.add_row("Errors", str(len(result.get('errors', []))))
            
            console.print(table)
            
        except Exception as e:
            console.print(f"[red]❌ Error loading {state_name}: {e}[/red]")
    
    # Final summary
    console.print(f"\n[bold green]🎉 Batch Load Complete![/bold green]")
    
    # Show database summary
    try:
        from app.states.postgres_config import create_postgres_database_manager
        db_manager = create_postgres_database_manager()
        
        with db_manager.get_session() as session:
            from sqlalchemy import text
            
            # Get counts
            tx_count = session.exec(text("SELECT COUNT(*) FROM unified_transactions")).first()
            person_count = session.exec(text("SELECT COUNT(*) FROM unified_persons")).first()
            committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
            address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
            
            summary_table = Table(title="Database Summary")
            summary_table.add_column("Table", style="cyan")
            summary_table.add_column("Count", style="green")
            
            summary_table.add_row("Transactions", str(tx_count))
            summary_table.add_row("Persons", str(person_count))
            summary_table.add_row("Committees", str(committee_count))
            summary_table.add_row("Addresses", str(address_count))
            
            console.print(summary_table)
            
    except Exception as e:
        console.print(f"[yellow]Could not display database summary: {e}[/yellow]")

if __name__ == "__main__":
    load_data_batched() 