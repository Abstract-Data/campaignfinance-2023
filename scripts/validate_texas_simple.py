#!/usr/bin/env python3
"""
Simple validation of Texas parquet files - validates data types and required fields
without triggering SQLAlchemy ORM relationship mapping issues.
"""

import polars as pl
from pathlib import Path
from datetime import date, datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from pydantic import BaseModel, field_validator, model_validator, ValidationError

console = Console()


# ============================================
# Simple Pydantic Validators (no SQLAlchemy)
# ============================================

class TECContributionSimple(BaseModel):
    """Simple validation for contribution records"""
    recordType: str
    formTypeCd: str
    schedFormTypeCd: str
    reportInfoIdent: int
    receivedDt: date
    filerIdent: int
    filerTypeCd: str
    filerName: str
    contributionInfoId: int
    contributionDt: date
    contributionAmount: float
    contributorPersentTypeCd: str
    contributorNameOrganization: Optional[str] = None
    contributorNameLast: Optional[str] = None
    contributorNameFirst: Optional[str] = None
    
    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"', "null", "None"]:
                values[k] = None
        return values
    
    @model_validator(mode="before")
    @classmethod
    def parse_dates(cls, values):
        for date_field in ['receivedDt', 'contributionDt']:
            if date_field in values and values[date_field]:
                v = values[date_field]
                if isinstance(v, str):
                    try:
                        values[date_field] = datetime.strptime(v, "%Y%m%d").date()
                    except ValueError:
                        try:
                            values[date_field] = datetime.strptime(v, "%Y-%m-%d").date()
                        except ValueError:
                            pass
        return values

    @model_validator(mode="before")
    @classmethod
    def check_contributor_name(cls, values):
        contrib_type = values.get("contributorPersentTypeCd")
        if contrib_type == "INDIVIDUAL":
            if not values.get("contributorNameLast"):
                raise ValueError("contributorNameLast required for INDIVIDUAL")
        elif contrib_type == "ENTITY":
            if not values.get("contributorNameOrganization"):
                raise ValueError("contributorNameOrganization required for ENTITY")
        return values


class TECExpenseSimple(BaseModel):
    """Simple validation for expense records"""
    recordType: str
    formTypeCd: str
    schedFormTypeCd: str
    reportInfoIdent: int
    receivedDt: date
    filerIdent: int
    filerTypeCd: str
    filerName: str
    expendInfoId: int
    expendDt: date
    expendAmount: float
    expendDescr: Optional[str] = None
    payeePersentTypeCd: str
    payeeNameOrganization: Optional[str] = None
    payeeNameLast: Optional[str] = None
    payeeNameFirst: Optional[str] = None
    
    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"', "null", "None"]:
                values[k] = None
        return values
    
    @model_validator(mode="before")
    @classmethod
    def parse_dates(cls, values):
        for date_field in ['receivedDt', 'expendDt']:
            if date_field in values and values[date_field]:
                v = values[date_field]
                if isinstance(v, str):
                    try:
                        values[date_field] = datetime.strptime(v, "%Y%m%d").date()
                    except ValueError:
                        try:
                            values[date_field] = datetime.strptime(v, "%Y-%m-%d").date()
                        except ValueError:
                            pass
        return values


class TECFilerSimple(BaseModel):
    """Simple validation for filer records"""
    recordType: str
    filerIdent: int
    filerTypeCd: str
    filerName: str
    
    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"', "null", "None"]:
                values[k] = None
        return values


class TECFinalReportSimple(BaseModel):
    """Simple validation for final report records"""
    recordType: str
    filerIdent: int
    filerName: str
    
    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"', "null", "None"]:
                values[k] = None
        return values


class TECTravelSimple(BaseModel):
    """Simple validation for travel records"""
    recordType: str
    filerIdent: int
    filerName: str
    
    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"', "null", "None"]:
                values[k] = None
        return values


class TECCandidateSimple(BaseModel):
    """Simple validation for candidate records"""
    recordType: str
    filerIdent: int
    filerName: str
    
    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"', "null", "None"]:
                values[k] = None
        return values


class TECDebtSimple(BaseModel):
    """Simple validation for debt records"""
    recordType: str
    filerIdent: int
    filerName: str
    
    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", '"', "null", "None"]:
                values[k] = None
        return values


# File prefix to validator mapping
FILE_VALIDATORS: Dict[str, type] = {
    "contribs": TECContributionSimple,
    "cont_ss": TECContributionSimple,
    "cont_t": TECContributionSimple,
    "expend": TECExpenseSimple,
    "expn_t": TECExpenseSimple,
    "filers": TECFilerSimple,
    "finals": TECFinalReportSimple,
    "travel": TECTravelSimple,
    "cand": TECCandidateSimple,
    "debts": TECDebtSimple,
}


@dataclass
class ValidationStats:
    """Statistics for a single file validation"""
    file_name: str
    total_records: int = 0
    passed: int = 0
    failed: int = 0
    error_types: Dict[str, int] = field(default_factory=dict)
    sample_errors: List[Dict] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.passed / self.total_records) * 100


def get_file_prefix(file_path: Path) -> str:
    """Extract the file type prefix from a parquet file name."""
    stem = file_path.stem
    parts = stem.rsplit('_', 1)
    if len(parts) == 2 and parts[1].endswith('w'):
        return parts[0]
    return stem


def validate_file(file_path: Path, validator: type, sample_size: Optional[int] = None) -> ValidationStats:
    """Validate all records in a parquet file using the specified validator."""
    stats = ValidationStats(file_name=file_path.name)
    
    try:
        df = pl.read_parquet(file_path)
        records = df.to_dicts()
        
        if sample_size and len(records) > sample_size:
            console.print(f"  [yellow]Sampling {sample_size:,} of {len(records):,} records[/yellow]")
            import random
            records = random.sample(records, sample_size)
        
        stats.total_records = len(records)
        
        for record in records:
            try:
                validated = validator.model_validate(record)
                stats.passed += 1
            except ValidationError as e:
                stats.failed += 1
                errors = e.errors()
                for error in errors:
                    error_type = error.get('type', 'unknown')
                    stats.error_types[error_type] = stats.error_types.get(error_type, 0) + 1
                
                if len(stats.sample_errors) < 5:
                    stats.sample_errors.append({
                        'record_sample': {k: str(v)[:50] for k, v in list(record.items())[:5]},
                        'errors': [str(e)[:200] for e in errors[:2]]
                    })
            except Exception as e:
                stats.failed += 1
                error_type = type(e).__name__
                stats.error_types[error_type] = stats.error_types.get(error_type, 0) + 1
                
                if len(stats.sample_errors) < 5:
                    stats.sample_errors.append({
                        'error': str(e)[:200]
                    })
                
    except Exception as e:
        console.print(f"  [red]Error reading file: {e}[/red]")
        stats.error_types['FileReadError'] = 1
    
    return stats


def main():
    """Main validation function."""
    texas_folder = Path("tmp/texas")
    
    if not texas_folder.exists():
        console.print("[red]Error: tmp/texas folder not found[/red]")
        return
    
    parquet_files = list(texas_folder.glob("*.parquet"))
    if not parquet_files:
        console.print("[red]Error: No parquet files found in tmp/texas[/red]")
        return
    
    console.print(Panel.fit(
        f"[bold blue]Texas Data File Validation (Simple)[/bold blue]\n"
        f"Found {len(parquet_files)} parquet files\n"
        f"Validators available: {len(FILE_VALIDATORS)}",
        border_style="blue"
    ))
    
    all_stats: List[ValidationStats] = []
    skipped_files: List[str] = []
    
    # Use command line arg or default to quick validation
    import sys
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        choice = "3"  # Quick mode default
    
    sample_size = None
    if choice == "2":
        sample_size = 10000
    elif choice == "3":
        sample_size = 1000
    
    console.print(f"\n[green]Using sample size: {sample_size or 'Full'}[/green]\n")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Validating files...", total=len(parquet_files))
        
        for file_path in sorted(parquet_files):
            prefix = get_file_prefix(file_path)
            validator = FILE_VALIDATORS.get(prefix)
            
            if validator is None:
                skipped_files.append(f"{file_path.name} (no validator for '{prefix}')")
                progress.advance(task)
                continue
            
            progress.update(task, description=f"Validating {file_path.name}...")
            
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            console.print(f"\n[cyan]Processing {file_path.name}[/cyan] ({file_size_mb:.1f} MB)")
            console.print(f"  Using validator: {validator.__name__}")
            
            stats = validate_file(file_path, validator, sample_size)
            all_stats.append(stats)
            
            if stats.success_rate >= 90:
                status_color = "green"
            elif stats.success_rate >= 70:
                status_color = "yellow"
            else:
                status_color = "red"
            
            console.print(f"  [{status_color}]Result: {stats.passed:,}/{stats.total_records:,} passed ({stats.success_rate:.1f}%)[/{status_color}]")
            
            if stats.error_types:
                top_errors = dict(sorted(stats.error_types.items(), key=lambda x: -x[1])[:3])
                console.print(f"  Top errors: {top_errors}")
            
            progress.advance(task)
    
    # Summary table
    console.print("\n")
    summary_table = Table(title="📊 Validation Summary", show_header=True, header_style="bold blue")
    summary_table.add_column("File", style="cyan")
    summary_table.add_column("Validator", style="magenta")
    summary_table.add_column("Total", justify="right")
    summary_table.add_column("Passed", justify="right", style="green")
    summary_table.add_column("Failed", justify="right", style="red")
    summary_table.add_column("Success %", justify="right")
    
    total_records = 0
    total_passed = 0
    total_failed = 0
    
    for stats in all_stats:
        prefix = get_file_prefix(Path(stats.file_name))
        validator = FILE_VALIDATORS.get(prefix)
        validator_name = validator.__name__ if validator else "Unknown"
        
        success_rate = f"{stats.success_rate:.1f}%"
        if stats.success_rate >= 90:
            success_rate = f"[green]{success_rate}[/green]"
        elif stats.success_rate >= 70:
            success_rate = f"[yellow]{success_rate}[/yellow]"
        else:
            success_rate = f"[red]{success_rate}[/red]"
        
        summary_table.add_row(
            stats.file_name,
            validator_name,
            f"{stats.total_records:,}",
            f"{stats.passed:,}",
            f"{stats.failed:,}",
            success_rate
        )
        
        total_records += stats.total_records
        total_passed += stats.passed
        total_failed += stats.failed
    
    summary_table.add_section()
    overall_rate = (total_passed / total_records * 100) if total_records > 0 else 0
    summary_table.add_row(
        "[bold]TOTAL[/bold]",
        "",
        f"[bold]{total_records:,}[/bold]",
        f"[bold green]{total_passed:,}[/bold green]",
        f"[bold red]{total_failed:,}[/bold red]",
        f"[bold]{overall_rate:.1f}%[/bold]"
    )
    
    console.print(summary_table)
    
    # Skipped files
    if skipped_files:
        console.print("\n[yellow]⚠️ Skipped files (no validator):[/yellow]")
        for f in skipped_files:
            console.print(f"  - {f}")
    
    # Error summary
    console.print("\n[bold]📋 Error Type Summary:[/bold]")
    all_error_types: Dict[str, int] = {}
    for stats in all_stats:
        for error_type, count in stats.error_types.items():
            all_error_types[error_type] = all_error_types.get(error_type, 0) + count
    
    if all_error_types:
        error_table = Table(show_header=True, header_style="bold red")
        error_table.add_column("Error Type")
        error_table.add_column("Count", justify="right")
        
        for error_type, count in sorted(all_error_types.items(), key=lambda x: -x[1])[:15]:
            error_table.add_row(error_type, f"{count:,}")
        
        console.print(error_table)
    else:
        console.print("[green]No errors found![/green]")
    
    # Sample errors
    console.print("\n[bold]🔍 Sample Errors (first few from each file with failures):[/bold]")
    for stats in all_stats:
        if stats.sample_errors:
            console.print(f"\n[cyan]{stats.file_name}:[/cyan]")
            for i, err in enumerate(stats.sample_errors[:2], 1):
                console.print(f"  Error {i}: {err}")


if __name__ == "__main__":
    main()
