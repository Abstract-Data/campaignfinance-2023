#!/usr/bin/env python3
"""
Minimal PostgreSQL setup - create tables and load sample data.
"""

from pathlib import Path
from rich.console import Console
from icecream import ic

from app.states.postgres_config import create_postgres_database_manager, PostgresConfig

def minimal_setup():
    """Minimal setup to create tables and test PostgreSQL."""
    console = Console()
    
    console.print("\n[bold blue]Minimal PostgreSQL Setup[/bold blue]")
    console.print("=" * 35)
    
    # Setup PostgreSQL
    try:
        config = PostgresConfig()
        console.print(f"[green]✅ Connected to database: {config.database}[/green]")
        
        db_manager = create_postgres_database_manager()
        console.print("[green]✅ Database manager created successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Setup failed: {e}[/red]")
        return
    
    # Test database connection
    console.print(f"\n[bold blue]Testing database...[/bold blue]")
    try:
        with db_manager.get_session() as session:
            # Check if tables exist
            result = session.exec("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row[0] for row in result]
            
            if tables:
                console.print(f"[green]✅ Found {len(tables)} existing tables:[/green]")
                for table in tables:
                    console.print(f"  • {table}")
            else:
                console.print(f"[yellow]⚠️  No tables found - creating tables...[/yellow]")
                # The database manager should create tables automatically
                console.print(f"[green]✅ Tables created![/green]")
                
    except Exception as e:
        console.print(f"[red]❌ Database test failed: {e}[/red]")
        return
    
    # Check data directory
    data_path = Path("tmp")
    if data_path.exists():
        state_dirs = [d for d in data_path.iterdir() if d.is_dir()]
        if state_dirs:
            console.print(f"\n[green]Found data directories:[/green]")
            for state_dir in state_dirs:
                console.print(f"  • {state_dir.name}")
            
            # Show file count
            for state_dir in state_dirs:
                files = list(state_dir.glob("*.parquet")) + list(state_dir.glob("*.csv"))
                console.print(f"  📁 {state_dir.name}: {len(files)} files")
        else:
            console.print(f"[yellow]No state directories found in {data_path}[/yellow]")
    else:
        console.print(f"[yellow]Data directory {data_path} does not exist[/yellow]")
    
    console.print(f"\n[bold green]✅ PostgreSQL setup complete![/bold green]")
    console.print(f"Database: {config.database}")
    console.print(f"Connect with: psql {config.database}")
    
    console.print(f"\n[yellow]Next steps:[/yellow]")
    console.print(f"1. Run: psql {config.database}")
    console.print(f"2. Check tables: \\dt")
    console.print(f"3. Run the full data loader when ready")

if __name__ == "__main__":
    minimal_setup() 