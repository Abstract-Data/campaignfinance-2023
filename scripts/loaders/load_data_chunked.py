#!/usr/bin/env python3
"""
Chunked data loader for campaign finance data.
Processes data in smaller batches to avoid long processing times.
"""

from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
import time

from app.states.postgres_state_loader import load_state_data_to_postgres

def load_data_chunked():
    """Load campaign finance data in chunks"""
    console = Console()
    
    console.print(Panel.fit(
        "[bold blue]Chunked Campaign Finance Data Loader[/bold blue]\n"
        "Loading data in manageable chunks",
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
    
    total_records = 0
    total_errors = 0
    state_results = {}
    
    # Load each state
    for state_dir in state_dirs:
        state_name = state_dir.name
        console.print(f"\n[bold yellow]Loading {state_name.upper()} data...[/bold yellow]")
        
        try:
            # Load the state data
            result = load_state_data_to_postgres(
                state=state_name,
                data_directory=tmp_dir,
                auto_link_officers=True,
                create_relationships=True,
                progress_callback=None
            )
            
            state_results[state_name] = result
            total_records += result.get('total_records', 0)
            total_errors += result.get('errors', 0)
            
            # Display state summary
            table = Table(title=f"{state_name.upper()} Load Results")
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
            console.print(f"[red]❌ Error loading {state_name}: {e}[/red]")
            total_errors += 1
    
    # Final summary
    console.print(f"\n[bold green]🎉 Data Load Complete![/bold green]")
    
    summary_table = Table(title="Overall Summary")
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Count", style="green")
    
    summary_table.add_row("States Processed", str(len(state_results)))
    summary_table.add_row("Total Records", str(total_records))
    summary_table.add_row("Total Errors", str(total_errors))
    summary_table.add_row("Success Rate", f"{((total_records - total_errors) / total_records * 100):.1f}%" if total_records > 0 else "N/A")
    
    console.print(summary_table)

if __name__ == "__main__":
    load_data_chunked() 