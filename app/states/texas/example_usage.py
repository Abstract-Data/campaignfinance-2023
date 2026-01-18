"""
Example usage of the normalized Texas Ethics Commission data models.

This script demonstrates how to:
1. Load raw CSV data
2. Process it through the normalized models
3. Save to database with proper person and address deduplication
"""

import pandas as pd
from sqlmodel import Session, create_engine, SQLModel
from .normalized_models import *
from .normalized_processor import NormalizedTECProcessor
from pathlib import Path

def create_database_engine(database_url: str = "sqlite:///texas_campaign_finance.db"):
    """Create database engine and tables."""
    engine = create_engine(database_url, echo=True)
    SQLModel.metadata.create_all(engine)
    return engine

def load_csv_data(file_path: str) -> pd.DataFrame:
    """Load CSV data from file."""
    return pd.read_csv(file_path, low_memory=False)

def process_contributions_example():
    """Example of processing contribution data."""
    
    # Setup database
    engine = create_database_engine()
    
    with Session(engine) as session:
        # Create processor
        processor = NormalizedTECProcessor(session)
        
        # Load contribution data (example path)
        contrib_file = Path("tmp/texas/contribs_01.csv")
        if contrib_file.exists():
            df = load_csv_data(str(contrib_file))
            
            # Process contributions
            print(f"Processing {len(df)} contribution records...")
            contributions = processor.process_csv_file(df, 'contribution')
            
            # Save to database
            processor.save_records(contributions)
            print(f"Saved {len(contributions)} contribution records")
        
        # Load expenditure data
        expend_file = Path("tmp/texas/expend_01.csv")
        if expend_file.exists():
            df = load_csv_data(str(expend_file))
            
            # Process expenditures
            print(f"Processing {len(df)} expenditure records...")
            expenditures = processor.process_csv_file(df, 'expenditure')
            
            # Save to database
            processor.save_records(expenditures)
            print(f"Saved {len(expenditures)} expenditure records")
        
        # Load filer data
        filer_file = Path("tmp/texas/filers.csv")
        if filer_file.exists():
            df = load_csv_data(str(filer_file))
            
            # Process filers
            print(f"Processing {len(df)} filer records...")
            filers = processor.process_csv_file(df, 'filer')
            
            # Save to database
            processor.save_records(filers)
            print(f"Saved {len(filers)} filer records")

def query_normalized_data_example():
    """Example of querying the normalized data."""
    
    engine = create_database_engine()
    
    with Session(engine) as session:
        # Query all contributions for a specific person
        from sqlmodel import select
        
        # Find a person by name
        person_stmt = select(TECPerson).where(TECPerson.name_last == "SMITH")
        person = session.exec(person_stmt).first()
        
        if person:
            print(f"Found person: {person.name_first} {person.name_last}")
            
            # Get all contributions from this person
            contrib_stmt = select(TECContribution).where(TECContribution.contributor_id == person.id)
            contributions = session.exec(contrib_stmt).all()
            
            print(f"Found {len(contributions)} contributions from this person")
            
            total_amount = sum(c.contribution_amount or 0 for c in contributions)
            print(f"Total contribution amount: ${total_amount:,.2f}")
            
            # Get all campaigns this person contributed to
            campaigns = set(c.filer_name for c in contributions if c.filer_name)
            print(f"Campaigns contributed to: {list(campaigns)}")
        
        # Query all people at a specific address
        address_stmt = select(TECAddress).where(TECAddress.city == "AUSTIN")
        austin_addresses = session.exec(address_stmt).all()
        
        print(f"Found {len(austin_addresses)} addresses in Austin")
        
        for addr in austin_addresses[:5]:  # Show first 5
            # Get people at this address
            people_stmt = select(TECPerson).where(TECPerson.address_id == addr.id)
            people = session.exec(people_stmt).all()
            
            print(f"Address: {addr.street_addr1}, {addr.city}, {addr.state_cd}")
            print(f"People at this address: {len(people)}")
            
            for person in people:
                print(f"  - {person.name_first} {person.name_last} ({person.person_type})")

def analyze_contributions_by_location():
    """Example analysis of contributions by geographic location."""
    
    engine = create_database_engine()
    
    with Session(engine) as session:
        from sqlmodel import select
        from sqlalchemy import func
        
        # Get contribution totals by city
        stmt = select(
            TECAddress.city,
            func.sum(TECContribution.contribution_amount).label('total_amount'),
            func.count(TECContribution.id).label('contribution_count')
        ).join(
            TECContribution, TECContribution.contributor_address_id == TECAddress.id
        ).where(
            TECAddress.city.is_not(None)
        ).group_by(
            TECAddress.city
        ).order_by(
            func.sum(TECContribution.contribution_amount).desc()
        ).limit(10)
        
        results = session.exec(stmt).all()
        
        print("Top 10 cities by total contributions:")
        for city, total_amount, count in results:
            print(f"{city}: ${total_amount:,.2f} ({count} contributions)")

def find_duplicate_persons():
    """Example of finding potential duplicate persons."""
    
    engine = create_database_engine()
    
    with Session(engine) as session:
        from sqlmodel import select
        
        # Find persons with similar names (potential duplicates)
        stmt = select(TECPerson).where(
            TECPerson.name_last.like("SMITH%")
        ).order_by(TECPerson.name_last, TECPerson.name_first)
        
        smiths = session.exec(stmt).all()
        
        print(f"Found {len(smiths)} persons with last name starting with 'SMITH'")
        
        # Group by similar names
        name_groups = {}
        for person in smiths:
            key = f"{person.name_last}_{person.name_first}"
            if key not in name_groups:
                name_groups[key] = []
            name_groups[key].append(person)
        
        # Show potential duplicates
        for name_key, persons in name_groups.items():
            if len(persons) > 1:
                print(f"\nPotential duplicates for {name_key}:")
                for person in persons:
                    print(f"  - ID: {person.id}, Hash: {person.person_hash[:8]}...")
                    if person.address:
                        print(f"    Address: {person.address.street_addr1}, {person.address.city}")

if __name__ == "__main__":
    print("Texas Ethics Commission Normalized Data Processing Example")
    print("=" * 60)
    
    # Process data
    print("\n1. Processing raw CSV data...")
    process_contributions_example()
    
    # Query examples
    print("\n2. Querying normalized data...")
    query_normalized_data_example()
    
    # Analysis examples
    print("\n3. Analyzing contributions by location...")
    analyze_contributions_by_location()
    
    # Duplicate detection
    print("\n4. Finding potential duplicate persons...")
    find_duplicate_persons()
    
    print("\nExample completed!") 