#!/usr/bin/env python3
"""
PostgreSQL-enabled state loader for campaign finance data.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
from icecream import ic
from rich.console import Console
from rich.table import Table

from .postgres_config import create_postgres_database_manager, PostgresConfig
from .unified_state_loader import UnifiedStateLoader


class PostgresStateLoader(UnifiedStateLoader):
    """
    PostgreSQL-enabled state loader that uses PostgreSQL database.
    """
    
    def __init__(self, state: str, data_directory: Path, postgres_config: Optional[PostgresConfig] = None):
        """
        Initialize the PostgreSQL state loader.
        
        Args:
            state: State name (e.g., 'texas', 'oklahoma')
            data_directory: Directory containing state data folders
            postgres_config: Optional PostgreSQL configuration
        """
        # Create PostgreSQL database manager
        if postgres_config:
            # Use custom config
            self.db_manager = create_postgres_database_manager()
        else:
            # Use default config
            self.db_manager = create_postgres_database_manager()
        
        # Initialize the parent class with the PostgreSQL database manager
        super().__init__(state, data_directory)
        
        # Override the database manager to use PostgreSQL
        self.db_manager = self.db_manager


def load_state_data_to_postgres(state: str, 
                               data_directory: Path,
                               auto_link_officers: bool = True,
                               create_relationships: bool = True,
                               progress_callback: Optional[callable] = None,
                               postgres_config: Optional[PostgresConfig] = None) -> Dict[str, Any]:
    """
    Load state data into PostgreSQL database with full automation.
    
    Args:
        state: State name (e.g., 'texas', 'oklahoma')
        data_directory: Directory containing state data folders
        auto_link_officers: Whether to automatically link transactions to officers
        create_relationships: Whether to create committee-person relationships
        progress_callback: Optional callback for progress updates
        postgres_config: Optional PostgreSQL configuration
        
    Returns:
        Summary report of the loading process
    """
    console = Console()
    
    # Validate PostgreSQL connection first
    if postgres_config:
        config = postgres_config
    else:
        config = PostgresConfig()
    
    console.print(f"\n[bold blue]Setting up PostgreSQL for {state.upper()} data...[/bold blue]")
    
    # Print connection info
    config.print_connection_info()
    
    # Validate connection
    if not config.validate_connection():
        console.print("[red]❌ PostgreSQL connection failed![/red]")
        console.print("\n[yellow]Please ensure PostgreSQL is running and configured correctly:[/yellow]")
        console.print("1. Install PostgreSQL: brew install postgresql")
        console.print("2. Start PostgreSQL: brew services start postgresql")
        console.print("3. Create a .env file with your settings")
        raise Exception("PostgreSQL connection failed")
    
    console.print("[green]✅ PostgreSQL connection successful![/green]")
    
    # Create the PostgreSQL state loader
    loader = PostgresStateLoader(state, data_directory, postgres_config)
    
    # Load the data
    console.print(f"\n[bold green]Loading {state.upper()} data into PostgreSQL...[/bold green]")
    
    summary = loader.load_state_data(
        auto_link_officers=auto_link_officers,
        create_relationships=create_relationships,
        progress_callback=progress_callback
    )
    
    return summary


def display_postgres_summary(summary: dict, console: Console):
    """Display a formatted summary of the PostgreSQL loading results."""
    
    # Create summary table
    table = Table(title=f"📊 PostgreSQL Load Summary: {summary['state']}")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta", justify="right")
    
    stats = summary['summary']
    table.add_row("📁 Files Processed", str(stats['total_files_processed']))
    table.add_row("💰 Transactions Created", str(stats['total_transactions']))
    table.add_row("👥 Persons Created", str(stats['total_persons']))
    table.add_row("🏛️ Committees Created", str(stats['total_committees']))
    table.add_row("🔗 Committee Relationships", str(stats['total_relationships']))
    table.add_row("📎 Transaction Links", str(stats['total_links']))
    table.add_row("❌ Errors", str(stats['error_count']))
    
    console.print(table)
    
    # Show database info
    console.print(f"\n[bold green]Database Information:[/bold green]")
    console.print(f"  🗄️  Database: campaign_finance")
    console.print(f"  📊 Tables: unified_transactions, unified_persons, unified_committees, etc.")
    console.print(f"  🔗 Relationships: Committee officers, transaction links, versioning")
    
    # Show any errors
    if summary['errors']:
        console.print(f"\n[red]⚠️  {len(summary['errors'])} errors encountered[/red]")
        for error in summary['errors'][:3]:  # Show first 3 errors
            console.print(f"  • {error}")


def test_postgres_connection():
    """Test PostgreSQL connection and configuration."""
    console = Console()
    
    console.print("\n[bold blue]PostgreSQL Connection Test[/bold blue]")
    console.print("=" * 40)
    
    try:
        config = PostgresConfig()
        config.print_connection_info()
        
        if config.validate_connection():
            console.print("[green]✅ PostgreSQL connection successful![/green]")
            return True
        else:
            console.print("[red]❌ PostgreSQL connection failed![/red]")
            return False
            
    except Exception as e:
        console.print(f"[red]❌ Error testing PostgreSQL connection: {e}[/red]")
        return False


if __name__ == "__main__":
    # Test PostgreSQL connection
    if test_postgres_connection():
        print("\nPostgreSQL is ready for data loading!")
    else:
        print("\nPlease configure PostgreSQL before loading data.") 