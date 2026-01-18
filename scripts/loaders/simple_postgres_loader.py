#!/usr/bin/env python3
"""
Simple PostgreSQL loader that loads raw campaign finance data without complex relationships.
"""

from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from sqlalchemy import text, create_engine, Column, String, Date, Numeric, Boolean, Text as SQLText, Integer, MetaData, Table
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker
import json
from datetime import datetime
from decimal import Decimal
import uuid

from app.states.postgres_config import PostgresConfig

def create_simple_tables(engine):
    """Create simple tables for raw data storage"""
    metadata = MetaData()
    
    # Simple raw transactions table
    raw_transactions = Table(
        'raw_transactions', metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', String(36), unique=True, nullable=False),
        Column('state', String(50), nullable=False, index=True),
        Column('file_origin', String(255), nullable=False),
        Column('transaction_id', String(255)),
        Column('amount', Numeric(15, 2)),
        Column('transaction_date', Date),
        Column('description', SQLText),
        Column('transaction_type', String(50)),
        Column('filed_date', Date),
        Column('amended', Boolean, default=False),
        Column('download_date', String(50)),
        Column('raw_data', SQLText),  # JSON string
        Column('created_at', String(50), default=lambda: datetime.utcnow().isoformat()),
    )
    
    # Simple raw persons table
    raw_persons = Table(
        'raw_persons', metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', String(36), unique=True, nullable=False),
        Column('state', String(50), nullable=False, index=True),
        Column('file_origin', String(255), nullable=False),
        Column('first_name', String(255)),
        Column('last_name', String(255)),
        Column('middle_name', String(255)),
        Column('suffix', String(50)),
        Column('organization', String(500)),
        Column('employer', String(500)),
        Column('occupation', String(500)),
        Column('job_title', String(500)),
        Column('person_type', String(50)),
        Column('raw_data', SQLText),  # JSON string
        Column('created_at', String(50), default=lambda: datetime.utcnow().isoformat()),
    )
    
    # Simple raw committees table
    raw_committees = Table(
        'raw_committees', metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', String(36), unique=True, nullable=False),
        Column('state', String(50), nullable=False, index=True),
        Column('file_origin', String(255), nullable=False),
        Column('name', String(500)),
        Column('committee_type', String(255)),
        Column('filer_id', String(255)),
        Column('raw_data', SQLText),  # JSON string
        Column('created_at', String(50), default=lambda: datetime.utcnow().isoformat()),
    )
    
    # Create tables
    metadata.create_all(engine)
    return raw_transactions, raw_persons, raw_committees

def load_simple_data_to_postgres():
    """Load raw data into simple PostgreSQL tables."""
    console = Console()
    
    console.print("\n[bold blue]Loading Raw Data to PostgreSQL (Simple Mode)[/bold blue]")
    console.print("=" * 55)
    
    # Setup PostgreSQL
    try:
        config = PostgresConfig()
        console.print(f"[green]✅ Connected to database: {config.database}[/green]")
        
        # Create engine and session
        engine = create_engine(config.database_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        console.print("[green]✅ Database connection created successfully![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ Setup failed: {e}[/red]")
        return
    
    # Create simple tables
    console.print(f"\n[bold blue]Creating simple tables...[/bold blue]")
    try:
        raw_transactions, raw_persons, raw_committees = create_simple_tables(engine)
        console.print("[green]✅ Simple tables created successfully![/green]")
    except Exception as e:
        console.print(f"[red]❌ Table creation failed: {e}[/red]")
        return
    
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
    
    console.print(f"\n[green]Found state directories:[/green]")
    for state_dir in state_dirs:
        console.print(f"  • {state_dir.name}")
    
    # Load data for each state
    total_files = 0
    total_transactions = 0
    total_persons = 0
    total_committees = 0
    
    for state_dir in state_dirs:
        state = state_dir.name
        console.print(f"\n[bold blue]Loading {state.upper()} raw data...[/bold blue]")
        
        # Find all data files
        data_files = []
        for pattern in ["*.parquet", "*.csv"]:
            data_files.extend(state_dir.glob(pattern))
        
        if not data_files:
            console.print(f"[yellow]No data files found in {state_dir}[/yellow]")
            continue
        
        console.print(f"Found {len(data_files)} files")
        
        # Process each file
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task(f"Loading {state} data...", total=len(data_files))
            
            for file_path in data_files:
                try:
                    progress.update(task, description=f"Loading {file_path.name}")
                    
                    # Read the file
                    from app.funcs.csv_reader import FileReader
                    file_reader = FileReader()
                    
                    if file_path.suffix.lower() == '.parquet':
                        records = list(file_reader.read_parquet(file_path))
                    else:
                        records = list(file_reader.read_csv(file_path))
                    
                    console.print(f"  📁 {file_path.name}: {len(records)} records")
                    
                    # Process records in batches
                    batch_size = 1000
                    file_transactions = 0
                    
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        
                        # Add metadata to each record
                        for record in batch:
                            record['state'] = state
                            record['file_origin'] = file_path.name
                        
                        # Insert batch into raw_transactions table
                        try:
                            with SessionLocal() as session:
                                for record in batch:
                                    # Extract basic transaction info
                                    transaction_data = {
                                        'uuid': str(uuid.uuid4()),
                                        'state': state,
                                        'file_origin': file_path.name,
                                        'transaction_id': record.get('transaction_id') or record.get('id'),
                                        'amount': record.get('amount'),
                                        'transaction_date': record.get('transaction_date') or record.get('date'),
                                        'description': record.get('description') or record.get('purpose'),
                                        'transaction_type': record.get('transaction_type'),
                                        'filed_date': record.get('filed_date'),
                                        'amended': record.get('amended', False),
                                        'download_date': record.get('download_date'),
                                        'raw_data': json.dumps(record, default=str),
                                        'created_at': datetime.utcnow().isoformat()
                                    }
                                    
                                    # Insert transaction
                                    session.execute(raw_transactions.insert().values(**transaction_data))
                                    file_transactions += 1
                                    
                                    # Extract person info if available
                                    person_fields = ['first_name', 'last_name', 'middle_name', 'suffix', 
                                                   'organization', 'employer', 'occupation', 'job_title']
                                    person_data = {k: record.get(k) for k in person_fields if record.get(k)}
                                    
                                    if person_data:
                                        person_data.update({
                                            'uuid': str(uuid.uuid4()),
                                            'state': state,
                                            'file_origin': file_path.name,
                                            'person_type': record.get('person_type', 'unknown'),
                                            'raw_data': json.dumps(record, default=str),
                                            'created_at': datetime.utcnow().isoformat()
                                        })
                                        session.execute(raw_persons.insert().values(**person_data))
                                        total_persons += 1
                                    
                                    # Extract committee info if available
                                    committee_fields = ['committee_name', 'committee_type', 'filer_id']
                                    committee_data = {k: record.get(k) for k in committee_fields if record.get(k)}
                                    
                                    if committee_data:
                                        committee_data.update({
                                            'uuid': str(uuid.uuid4()),
                                            'state': state,
                                            'file_origin': file_path.name,
                                            'name': committee_data.pop('committee_name', None),
                                            'raw_data': json.dumps(record, default=str),
                                            'created_at': datetime.utcnow().isoformat()
                                        })
                                        session.execute(raw_committees.insert().values(**committee_data))
                                        total_committees += 1
                                
                                session.commit()
                                
                        except Exception as e:
                            console.print(f"[red]Error processing batch: {e}[/red]")
                            continue
                    
                    console.print(f"    ✅ Loaded {file_transactions} transactions")
                    total_transactions += file_transactions
                    total_files += 1
                    
                    progress.advance(task)
                    
                except Exception as e:
                    console.print(f"[red]Error processing {file_path}: {e}[/red]")
                    progress.advance(task)
                    continue
    
    # Summary
    console.print(f"\n[bold green]✅ Raw data loading complete![/bold green]")
    console.print(f"  📁 Files processed: {total_files}")
    console.print(f"  💰 Transactions loaded: {total_transactions:,}")
    console.print(f"  👥 Persons extracted: {total_persons:,}")
    console.print(f"  🏛️ Committees extracted: {total_committees:,}")
    console.print(f"\n[bold green]Your raw data is now in PostgreSQL![/bold green]")
    console.print(f"Database: {config.database}")
    console.print(f"Connect with: psql {config.database}")
    
    # Verify data was loaded
    console.print(f"\n[bold blue]Verifying loaded data...[/bold blue]")
    try:
        with SessionLocal() as session:
            # Check transaction count
            result = session.execute(text("SELECT COUNT(*) FROM raw_transactions"))
            transaction_count = result.scalar()
            console.print(f"[green]✅ Raw transactions in database: {transaction_count:,}[/green]")
            
            # Check by state
            result = session.execute(text("SELECT state, COUNT(*) FROM raw_transactions GROUP BY state"))
            for row in result:
                console.print(f"  • {row[0]}: {row[1]:,} transactions")
                
    except Exception as e:
        console.print(f"[red]❌ Data verification failed: {e}[/red]")
    
    # Show some example queries
    console.print(f"\n[yellow]Example queries:[/yellow]")
    console.print(f"  SELECT COUNT(*) FROM raw_transactions;")
    console.print(f"  SELECT state, COUNT(*) FROM raw_transactions GROUP BY state;")
    console.print(f"  SELECT * FROM raw_transactions LIMIT 5;")
    console.print(f"  SELECT * FROM raw_persons LIMIT 5;")
    console.print(f"  SELECT * FROM raw_committees LIMIT 5;")

if __name__ == "__main__":
    load_simple_data_to_postgres() 