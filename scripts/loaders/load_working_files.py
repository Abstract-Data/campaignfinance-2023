#!/usr/bin/env python3
"""
Load only working files (contribution files) to avoid field mapping issues.
"""

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

def load_working_files():
    """Load only files that we know work"""
    console = Console()
    
    console.print(Panel.fit(
        "[bold blue]Working Files Loader[/bold blue]\n"
        "Loading only contribution files that we know work",
        border_style="blue"
    ))
    
    try:
        # Import here to avoid circular imports
        from app.states.unified_state_loader import UnifiedStateLoader
        from app.states.postgres_config import create_postgres_database_manager
        
        # Create loader for Oklahoma
        db_manager = create_postgres_database_manager()
        loader = UnifiedStateLoader("oklahoma", Path("tmp"))
        loader.db_manager = db_manager
        
        # Find only contribution files
        all_files = loader._discover_data_files()
        contribution_files = [f for f in all_files if "contribution" in f.name.lower()]
        
        console.print(f"[green]Found {len(contribution_files)} contribution files:[/green]")
        for file in contribution_files:
            console.print(f"  • {file.name}")
        
        # Process only contribution files
        console.print(f"\n[bold yellow]Processing contribution files...[/bold yellow]")
        
        total_records = 0
        total_transactions = 0
        total_persons = 0
        total_committees = 0
        total_addresses = 0
        total_errors = 0
        
        for file_path in contribution_files:
            try:
                console.print(f"[blue]Processing {file_path.name}...[/blue]")
                
                # Process this file
                file_stats = loader._process_data_file(file_path, auto_link_officers=False)
                
                total_records += file_stats.get('records_processed', 0)
                total_transactions += file_stats.get('transactions', 0)
                total_persons += file_stats.get('persons', 0)
                total_committees += file_stats.get('committees', 0)
                total_addresses += file_stats.get('addresses', 0)
                total_errors += len(file_stats.get('errors', []))
                
                console.print(f"  ✅ {file_stats.get('transactions', 0)} transactions created")
                
            except Exception as e:
                console.print(f"  ❌ Error processing {file_path.name}: {e}")
                total_errors += 1
        
        # Display results
        console.print(f"\n[bold green]✅ Contribution Files Load Complete![/bold green]")
        
        table = Table(title="Contribution Files Results")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green")
        
        table.add_row("Files Processed", str(len(contribution_files)))
        table.add_row("Total Records", str(total_records))
        table.add_row("Transactions", str(total_transactions))
        table.add_row("Persons", str(total_persons))
        table.add_row("Committees", str(total_committees))
        table.add_row("Addresses", str(total_addresses))
        table.add_row("Errors", str(total_errors))
        
        console.print(table)
        
        # Show database summary
        console.print(f"\n[bold blue]Database Summary:[/bold blue]")
        try:
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
        
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        raise

if __name__ == "__main__":
    load_working_files() 