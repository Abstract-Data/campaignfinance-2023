#!/usr/bin/env python3
"""
Test script for the Unified State Loader - demonstrates one-line state data loading.
"""

from pathlib import Path
from icecream import ic
from rich.console import Console
from rich.table import Table

from app.states.unified_state_loader import load_state_data


def test_state_loader():
    """Test the unified state loader with a simple one-line call."""
    
    console = Console()
    
    # Example usage - this is all you need to do!
    console.print("\n[bold blue]Unified State Loader Demo[/bold blue]")
    console.print("=" * 50)
    
    # Define your data directory
    data_directory = Path("tmp")  # Adjust this to your actual data directory
    
    # Example 1: Load Texas data with full automation
    console.print("\n[bold green]Example 1: Loading Texas Data[/bold green]")
    console.print("-" * 30)
    
    try:
        # This single call does everything:
        # - Reads all Texas data files
        # - Creates unified models
        # - Extracts committee officers
        # - Creates committee-person relationships
        # - Links transactions to officers
        # - Provides comprehensive reporting
        
        texas_summary = load_state_data(
            state="texas",
            data_directory=data_directory,
            auto_link_officers=True,
            create_relationships=True
        )
        
        # Display results
        display_summary(texas_summary, console)
        
    except Exception as e:
        console.print(f"[red]Error loading Texas data: {e}[/red]")
    
    # Example 2: Load Oklahoma data with custom options
    console.print("\n[bold green]Example 2: Loading Oklahoma Data[/bold green]")
    console.print("-" * 30)
    
    try:
        # You can customize the behavior
        oklahoma_summary = load_state_data(
            state="oklahoma",
            data_directory=data_directory,
            auto_link_officers=False,  # Don't auto-link (do it manually later)
            create_relationships=True  # But do create relationships
        )
        
        display_summary(oklahoma_summary, console)
        
    except Exception as e:
        console.print(f"[red]Error loading Oklahoma data: {e}[/red]")
    
    # Example 3: Progress callback
    console.print("\n[bold green]Example 3: With Progress Callback[/bold green]")
    console.print("-" * 30)
    
    def progress_callback(file_stats):
        """Custom progress callback function."""
        console.print(f"  ✓ {file_stats['file']}: {file_stats['transactions']} transactions")
    
    try:
        # With custom progress tracking
        summary_with_progress = load_state_data(
            state="texas",
            data_directory=data_directory,
            auto_link_officers=True,
            create_relationships=True,
            progress_callback=progress_callback
        )
        
        console.print(f"\n[green]Completed with progress tracking![/green]")
        
    except Exception as e:
        console.print(f"[red]Error with progress callback: {e}[/red]")


def display_summary(summary: dict, console):
    """Display a formatted summary of the loading results."""
    
    # Create summary table
    table = Table(title=f"State Load Summary: {summary['state']}")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="magenta")
    
    stats = summary['summary']
    table.add_row("Files Processed", str(stats['total_files_processed']))
    table.add_row("Transactions Created", str(stats['total_transactions']))
    table.add_row("Persons Created", str(stats['total_persons']))
    table.add_row("Committees Created", str(stats['total_committees']))
    table.add_row("Committee Relationships", str(stats['total_relationships']))
    table.add_row("Transaction Links", str(stats['total_links']))
    table.add_row("Errors", str(stats['error_count']))
    
    console.print(table)
    
    # Show any errors
    if summary['errors']:
        console.print("\n[red]Errors encountered:[/red]")
        for error in summary['errors'][:5]:  # Show first 5 errors
            console.print(f"  • {error}")
        if len(summary['errors']) > 5:
            console.print(f"  ... and {len(summary['errors']) - 5} more errors")


def demonstrate_simple_usage():
    """Show the simplest possible usage."""
    
    console = Console()
    console.print("\n[bold yellow]Simplest Usage Example:[/bold yellow]")
    console.print("=" * 40)
    
    console.print("""
# Just one line of code to load a state:

from app.states.unified_state_loader import load_state_data

# Load Texas data with full automation
summary = load_state_data("texas", Path("tmp"))

# That's it! Everything is automatically:
# ✓ Files discovered and read
# ✓ Models created and saved
# ✓ Committee officers extracted
# ✓ Relationships established
# ✓ Transactions linked to officers
# ✓ Comprehensive reporting provided
""")
    
    console.print("\n[bold green]Available Options:[/bold green]")
    console.print("""
load_state_data(
    state="texas",                    # State name
    data_directory=Path("tmp"),       # Data directory
    auto_link_officers=True,          # Auto-link transactions to officers
    create_relationships=True,        # Create committee-person relationships
    progress_callback=my_callback     # Optional progress tracking
)
""")


if __name__ == "__main__":
    # Show the simple usage first
    demonstrate_simple_usage()
    
    # Then run the actual test (if you have data)
    # test_state_loader() 