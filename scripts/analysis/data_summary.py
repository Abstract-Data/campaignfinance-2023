#!/usr/bin/env python3
"""
Data summary script to show current progress.
"""

from app.states.postgres_config import create_postgres_database_manager

def data_summary():
    """Show current data summary"""
    print("📊 Campaign Finance Data Summary")
    print("=" * 50)
    
    db_manager = create_postgres_database_manager()
    with db_manager.get_session() as session:
        from sqlalchemy import text
        
        # Get counts
        tx_count = session.exec(text("SELECT COUNT(*) FROM unified_transactions")).first()
        committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
        address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
        
        print(f"📋 Total Transactions: {tx_count[0]}")
        print(f"🏛️ Total Committees: {committee_count[0]}")
        print(f"📍 Total Addresses: {address_count[0]}")
        
        # Get committee details
        print(f"\n🏛️ Committees:")
        committees = session.exec(text("SELECT filer_id, name FROM unified_committees ORDER BY filer_id")).all()
        for committee in committees:
            print(f"  • {committee.filer_id}: {committee.name}")
        
        # Get address details
        print(f"\n📍 Addresses:")
        addresses = session.exec(text("SELECT id, street_1, city, state, zip_code FROM unified_addresses ORDER BY id")).all()
        for addr in addresses:
            print(f"  • ID {addr.id}: {addr.street_1}, {addr.city}, {addr.state} {addr.zip_code}")
        
        # Count unique addresses
        unique_addresses = set()
        for addr in addresses:
            if addr.street_1 and addr.city:
                unique_addresses.add((addr.street_1, addr.city, addr.state, addr.zip_code))
        
        print(f"\n📊 Deduplication Summary:")
        print(f"  • Total addresses: {address_count[0]}")
        print(f"  • Unique addresses: {len(unique_addresses)}")
        print(f"  • Duplicates eliminated: {address_count[0] - len(unique_addresses)}")
        
        # Sample transactions
        print(f"\n💳 Sample Transactions:")
        sample_txs = session.exec(text("SELECT id, transaction_id, committee_id, amount FROM unified_transactions ORDER BY id DESC LIMIT 5")).all()
        for tx in sample_txs:
            print(f"  • ID {tx.id}: {tx.transaction_id} (Committee: {tx.committee_id}, Amount: ${tx.amount})")

if __name__ == "__main__":
    data_summary() 