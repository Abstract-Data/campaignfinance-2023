#!/usr/bin/env python3
"""
Recreate the unified tables with the correct schema.
"""

from rich.console import Console
from sqlalchemy import text, create_engine
from sqlmodel import SQLModel

from app.states.postgres_config import PostgresConfig
# Import unified SQLModels to ensure metadata is registered
from app.states import unified_sqlmodels  # noqa: F401

def recreate_unified_tables():
    """Drop and recreate all unified tables with correct schema."""
    console = Console()
    
    console.print("\n[bold blue]Recreating Unified Tables[/bold blue]")
    console.print("=" * 35)
    
    # Setup PostgreSQL
    try:
        config = PostgresConfig()
        console.print(f"[green]✅ Connected to database: {config.database}[/green]")
        
        # Create engine
        engine = create_engine(config.database_url)
        console.print("[green]✅ Database engine created successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Setup failed: {e}[/red]")
        return
    
    # Drop existing unified tables
    console.print("\n[bold blue]Dropping existing unified tables...[/bold blue]")
    try:
        with engine.connect() as conn:
            # Drop tables in reverse dependency order
            tables_to_drop = [
                'unified_committee_person_versions',
                'unified_transaction_versions', 
                'unified_person_versions',
                'unified_committee_versions',
                'unified_address_versions',
                'unified_campaign_entities',
                'unified_entity_associations',
                'unified_transaction_persons',
                'unified_committee_persons',
                'unified_contributions',
                'unified_loans',
                'unified_transactions',
                'file_origins',
                'unified_campaigns',
                'unified_entities',
                'unified_persons',
                'unified_committees',
                'unified_addresses',
                'states'
            ]
            
            for table in tables_to_drop:
                try:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    console.print(f"[green]✅ Dropped {table}[/green]")
                except Exception as e:
                    console.print(f"[yellow]⚠️ Could not drop {table}: {e}[/yellow]")
            
            conn.commit()
            
    except Exception as e:
        console.print(f"[red]❌ Error dropping tables: {e}[/red]")
        return
    
    # Create new tables with correct schema
    console.print("\n[bold blue]Creating new unified tables...[/bold blue]")
    try:
        # Create all tables
        SQLModel.metadata.create_all(engine)
        console.print("[green]✅ All unified tables created successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Error creating tables: {e}[/red]")
        return
    
    # Verify tables were created
    console.print("\n[bold blue]Verifying new tables...[/bold blue]")
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'unified_%' ORDER BY table_name"))
            tables = [row[0] for row in result]
            
            console.print(f"[green]✅ Found {len(tables)} unified tables:[/green]")
            for table in tables:
                console.print(f"  • {table}")
            
            extra_result = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name IN ('states', 'file_origins') "
                    "ORDER BY table_name"
                )
            )
            extras = [row[0] for row in extra_result]
            if extras:
                console.print("[green]✅ Additional reference tables:[/green]")
                for table in extras:
                    console.print(f"  • {table}")
                
    except Exception as e:
        console.print(f"[red]❌ Error verifying tables: {e}[/red]")
        return
    
    console.print("\n[bold green]✅ Table recreation complete![/bold green]")
    console.print("All unified tables have been recreated with the correct schema.")

if __name__ == "__main__":
    recreate_unified_tables() 