#!/usr/bin/env python3
"""
Quick PostgreSQL data loader - uses default settings.
"""

from pathlib import Path
from rich.console import Console

from app.states.postgres_state_loader import (
    load_state_data_to_postgres, 
    display_postgres_summary,
    PostgresConfig
)

def main():
    """Load data into PostgreSQL with default settings."""
    console = Console()
    
    console.print("\n[bold blue]Quick PostgreSQL Data Loader[/bold blue]")
    console.print("=" * 40)
    
    # Use default PostgreSQL config
    config = PostgresConfig()
    console.print("[green]Using default PostgreSQL settings[/green]")
    
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
    
    # Load all states
    all_summaries = []
    
    for state_dir in state_dirs:
        state = state_dir.name
        console.print(f"\n[bold blue]Loading {state.upper()} data...[/bold blue]")
        
        try:
            # Progress callback
            def progress_callback(file_stats):
                console.print(f"  📁 {file_stats['file']}: {file_stats['transactions']} transactions")
            
            # Load the data
            summary = load_state_data_to_postgres(
                state=state,
                data_directory=data_path,
                auto_link_officers=True,
                create_relationships=True,
                progress_callback=progress_callback,
                postgres_config=config
            )
            
            all_summaries.append(summary)
            display_postgres_summary(summary, console)
            
        except Exception as e:
            console.print(f"[red]❌ Error loading {state} data: {e}[/red]")
            continue
    
    # Final summary
    if all_summaries:
        total_transactions = sum(s['summary']['total_transactions'] for s in all_summaries)
        total_persons = sum(s['summary']['total_persons'] for s in all_summaries)
        total_committees = sum(s['summary']['total_committees'] for s in all_summaries)
        
        console.print(f"\n[bold green]✅ Successfully loaded data for {len(all_summaries)} states![/bold green]")
        console.print(f"  📊 Total transactions: {total_transactions:,}")
        console.print(f"  👥 Total persons: {total_persons:,}")
        console.print(f"  🏛️ Total committees: {total_committees:,}")
        
        console.print(f"\n[bold green]Your data is now in PostgreSQL![/bold green]")
        console.print(f"Database: campaign_finance")
        console.print(f"Connect with: psql campaign_finance")
        
    else:
        console.print("[red]❌ No data was successfully loaded.[/red]")

if __name__ == "__main__":
    main() 