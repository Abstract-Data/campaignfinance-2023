#!/usr/bin/env python3
"""
Debug script to test committee creation.
"""

from app.states.unified_sqlmodels import UnifiedSQLModelBuilder
from app.states.postgres_config import create_postgres_database_manager

def debug_committee():
    """Debug committee creation"""
    print("Testing committee creation...")
    
    # Sample record
    record = {
        "Receipt ID": "1009948",
        "Org ID": "9908", 
        "Committee Name": "",
        "Committee Type": "Candidate Committee",
        "Candidate Name": "MARCUS L MCENTIRE"
    }
    
    # Create builder
    builder = UnifiedSQLModelBuilder("oklahoma")
    
    # Try to build committee
    print(f"Building committee from record: {record}")
    committee = builder.build_committee(record)
    
    if committee:
        print(f"✅ Committee created: {committee.filer_id} - {committee.name}")
        
        # Try to save to database
        try:
            db_manager = create_postgres_database_manager()
            with db_manager.get_session() as session:
                session.merge(committee)
                session.commit()
                print("✅ Committee saved to database")
        except Exception as e:
            print(f"❌ Error saving committee: {e}")
    else:
        print("❌ No committee created")

if __name__ == "__main__":
    debug_committee() 