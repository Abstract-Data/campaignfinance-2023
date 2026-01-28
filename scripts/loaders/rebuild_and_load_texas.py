#!/usr/bin/env python3
"""
Script to completely rebuild the campaign finance database from scratch.
Drops all tables, recreates schema, and loads Texas campaign finance data.
"""

import sys
from pathlib import Path
from sqlmodel import SQLModel, create_engine, Session, text
from sqlalchemy import inspect
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

# Add app to path
sys.path.insert(0, str(Path(__file__).parent / "app"))

from app.states.postgres_config import PostgresConfig

console = Console()

def drop_all_tables(engine):
    """Drop all tables in the database."""
    console.print("\n[bold yellow]Step 1: Dropping all existing tables...[/bold yellow]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Dropping tables...", total=None)
        
        with engine.begin() as conn:
            # Drop texas schema if exists (CASCADE removes all dependent objects)
            console.print("  • Dropping texas schema...")
            conn.execute(text("DROP SCHEMA IF EXISTS texas CASCADE"))
            
            # Get all tables in public schema
            inspector = inspect(engine)
            tables = inspector.get_table_names(schema='public')
            
            if tables:
                console.print(f"  • Found {len(tables)} tables in public schema")
                for table in tables:
                    console.print(f"    - Dropping {table}")
                    conn.execute(text(f'DROP TABLE IF EXISTS public."{table}" CASCADE'))
            
            conn.commit()
        
        progress.update(task, completed=True)
    
    console.print("[green]✓ All tables dropped successfully[/green]")

def create_schema(engine):
    """Create necessary schemas."""
    console.print("\n[bold yellow]Step 2: Creating schemas...[/bold yellow]")
    
    with engine.begin() as conn:
        # Note: Using unified schema in public, not texas-specific schema
        console.print("  • Using public schema for unified models")
        conn.commit()
    
    console.print("[green]✓ Schema setup complete[/green]")

def create_tables(engine):
    """Create all tables using SQLModel metadata."""
    console.print("\n[bold yellow]Step 3: Creating all tables...[/bold yellow]")
    
    # Import all models to ensure they're registered
    console.print("  • Importing all database models...")
    
    # Import unified models (these are what the production loader uses)
    try:
        from app.core.unified_sqlmodels import (
            UnifiedTransaction, UnifiedPerson, UnifiedAddress, UnifiedCommittee,
            UnifiedTransactionPerson, UnifiedTransactionVersion,
            UnifiedPersonVersion, UnifiedCommitteeVersion, UnifiedAddressVersion,
            UnifiedCommitteePerson, UnifiedCommitteePersonVersion, State,
            FileOrigin
        )
        console.print("  • Unified models imported")
    except ImportError as e:
        console.print(f"[yellow]  • Warning: Could not import unified models: {e}[/yellow]")
    
    # Note: Skipping Texas-specific models due to schema inconsistencies
    # The production_loader.py uses unified models, not Texas-specific schema
    console.print("  • Skipping Texas-specific models (using unified schema)")
    
    # Create all tables
    console.print("  • Creating tables from metadata...")
    SQLModel.metadata.create_all(engine)
    
    # Verify tables were created
    inspector = inspect(engine)
    texas_tables = inspector.get_table_names(schema='texas')
    public_tables = inspector.get_table_names(schema='public')
    
    console.print(f"[green]✓ Created {len(texas_tables)} tables in texas schema[/green]")
    console.print(f"[green]✓ Created {len(public_tables)} tables in public schema[/green]")
    
    if texas_tables:
        console.print("  Texas tables:")
        for table in sorted(texas_tables):
            console.print(f"    - {table}")

def load_texas_data(engine):
    """Load Texas campaign finance data."""
    console.print("\n[bold yellow]Step 4: Loading Texas data...[/bold yellow]")
    
    # Check for data files
    data_dir = Path("tmp/texas")
    if not data_dir.exists():
        console.print("[yellow]⚠ No Texas data directory found at tmp/texas[/yellow]")
        console.print("[yellow]  Download data first using: python app/states/texas/texas_downloader.py[/yellow]")
        return
    
    # Find CSV and Parquet files
    csv_files = list(data_dir.glob("*.csv"))
    parquet_files = list(data_dir.glob("*.parquet"))
    
    if not csv_files and not parquet_files:
        console.print("[yellow]⚠ No data files found in tmp/texas[/yellow]")
        return
    
    console.print(f"  • Found {len(csv_files)} CSV files and {len(parquet_files)} Parquet files")
    
    # TODO: Implement data loading
    # This would use the production loader or custom loading logic
    console.print("[yellow]  • Data loading not yet implemented in this script[/yellow]")
    console.print("[yellow]  • Use production_loader.py to load data after schema is created[/yellow]")

def display_summary(engine):
    """Display summary of database state."""
    console.print("\n[bold cyan]Database Summary:[/bold cyan]")
    
    inspector = inspect(engine)
    
    # Show schemas
    with engine.connect() as conn:
        result = conn.execute(text("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('texas', 'public')"))
        schemas = [row[0] for row in result]
    
    console.print(f"\n  Schemas: {', '.join(schemas)}")
    
    # Show tables
    for schema in schemas:
        tables = inspector.get_table_names(schema=schema)
        if tables:
            console.print(f"\n  {schema} schema ({len(tables)} tables):")
            for table in sorted(tables):
                console.print(f"    - {table}")

def main():
    """Main execution function."""
    console.print("[bold cyan]Campaign Finance Database Rebuild[/bold cyan]")
    console.print("=" * 60)
    
    # Get PostgreSQL configuration
    config = PostgresConfig()
    console.print(f"\nDatabase: {config.database}")
    console.print(f"Host: {config.host}:{config.port}")
    console.print(f"User: {config.username}\n")
    
    # Validate connection
    if not config.validate_connection():
        console.print("[bold red]✗ Failed to connect to PostgreSQL[/bold red]")
        console.print("\nPlease check:")
        console.print("  1. PostgreSQL is running: brew services start postgresql")
        console.print("  2. Database exists: psql -l | grep campaign_finance")
        console.print("  3.Credentials are correct in .env file")
        return 1
    
    console.print("[green]✓ PostgreSQL connection successful[/green]")
    
    # Create engine
    engine = create_engine(config.database_url, echo=False)
    
    try:
        # Execute rebuild steps
        drop_all_tables(engine)
        create_schema(engine)
        create_tables(engine)
        load_texas_data(engine)
        display_summary(engine)
        
        console.print("\n[bold green]✓ Database rebuild complete![/bold green]")
        console.print("\nNext steps:")
        console.print("  1. Load Texas data: python production_loader.py")
        console.print("  2. Verify data: python data_summary.py")
        
        return 0
        
    except Exception as e:
        console.print(f"\n[bold red]✗ Error during rebuild: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        engine.dispose()

if __name__ == "__main__":
    sys.exit(main())
