#!/usr/bin/env python3
"""
Load campaign finance data into PostgreSQL database.
"""

from pathlib import Path
from rich.console import Console
from rich.prompt import Prompt, Confirm
import os

from app.states.postgres_state_loader import (
    load_state_data_to_postgres, 
    display_postgres_summary,
    test_postgres_connection,
    PostgresConfig
)

def setup_postgres_config():
    """Interactive setup for PostgreSQL configuration."""
    console = Console()
    
    console.print("\n[bold blue]PostgreSQL Setup[/bold blue]")
    console.print("=" * 30)
    
    # Check if .env file exists
    env_file = Path(".env")
    if env_file.exists():
        console.print("[green]✓ .env file found[/green]")
        use_env = Confirm.ask("Use existing .env file?", default=True)
        if use_env:
            return PostgresConfig()
    
    # Interactive configuration
    console.print("\n[yellow]Please provide PostgreSQL connection details:[/yellow]")
    
    host = Prompt.ask("Host", default="localhost")
    port = int(Prompt.ask("Port", default="5432"))
    database = Prompt.ask("Database name", default="campaign_finance")
    username = Prompt.ask("Username", default="postgres")
    password = Prompt.ask("Password", password=True, default="")
    
    # Create .env file
    env_content = f"""POSTGRES_HOST={host}
POSTGRES_PORT={port}
POSTGRES_DB={database}
POSTGRES_USER={username}
POSTGRES_PASSWORD={password}
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    console.print("[green]✓ Created .env file[/green]")
    
    return PostgresConfig()

def main():
    """Main function to load data into PostgreSQL."""
    console = Console()
    
    console.print("\n[bold blue]Campaign Finance PostgreSQL Loader[/bold blue]")
    console.print("=" * 50)
    
    # Step 1: Setup PostgreSQL
    console.print("\n[bold green]Step 1: PostgreSQL Setup[/bold green]")
    console.print("-" * 30)
    
    try:
        config = setup_postgres_config()
        
        # Test connection
        if not test_postgres_connection():
            console.print("[red]❌ PostgreSQL connection failed![/red]")
            console.print("\n[yellow]Please ensure PostgreSQL is running:[/yellow]")
            console.print("1. Install: brew install postgresql")
            console.print("2. Start: brew services start postgresql")
            console.print("3. Create user: createuser -s postgres")
            return
        
        console.print("[green]✅ PostgreSQL connection successful![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Setup failed: {e}[/red]")
        return
    
    # Step 2: Select data directory
    console.print("\n[bold green]Step 2: Data Directory[/bold green]")
    console.print("-" * 30)
    
    data_dir = Prompt.ask("Data directory", default="tmp")
    data_path = Path(data_dir)
    
    if not data_path.exists():
        console.print(f"[red]❌ Data directory {data_path} does not exist![/red]")
        return
    
    # List available states
    state_dirs = [d for d in data_path.iterdir() if d.is_dir()]
    if not state_dirs:
        console.print(f"[red]❌ No state directories found in {data_path}[/red]")
        return
    
    console.print(f"[green]✓ Found state directories:[/green]")
    for state_dir in state_dirs:
        console.print(f"  • {state_dir.name}")
    
    # Step 3: Select states to load
    console.print("\n[bold green]Step 3: Select States to Load[/bold green]")
    console.print("-" * 30)
    
    states_to_load = []
    for state_dir in state_dirs:
        state_name = state_dir.name
        if Confirm.ask(f"Load {state_name.upper()} data?", default=True):
            states_to_load.append(state_name)
    
    if not states_to_load:
        console.print("[yellow]No states selected for loading.[/yellow]")
        return
    
    # Step 4: Load data
    console.print("\n[bold green]Step 4: Loading Data[/bold green]")
    console.print("-" * 30)
    
    all_summaries = []
    
    for state in states_to_load:
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
    
    # Step 5: Final summary
    console.print("\n[bold green]Step 5: Final Summary[/bold green]")
    console.print("-" * 30)
    
    if all_summaries:
        total_transactions = sum(s['summary']['total_transactions'] for s in all_summaries)
        total_persons = sum(s['summary']['total_persons'] for s in all_summaries)
        total_committees = sum(s['summary']['total_committees'] for s in all_summaries)
        
        console.print(f"[green]✅ Successfully loaded data for {len(all_summaries)} states:[/green]")
        console.print(f"  📊 Total transactions: {total_transactions:,}")
        console.print(f"  👥 Total persons: {total_persons:,}")
        console.print(f"  🏛️ Total committees: {total_committees:,}")
        
        console.print(f"\n[bold green]Your data is now in PostgreSQL![/bold green]")
        console.print(f"Database: campaign_finance")
        console.print(f"Tables: unified_transactions, unified_persons, unified_committees, etc.")
        
        console.print(f"\n[yellow]Next steps:[/yellow]")
        console.print(f"1. Connect to your PostgreSQL database")
        console.print(f"2. Run queries on the unified tables")
        console.print(f"3. Use the database manager for advanced queries")
        
    else:
        console.print("[red]❌ No data was successfully loaded.[/red]")

if __name__ == "__main__":
    main() 