"""
Integration module for using unified models with existing state processors.
"""

from typing import Dict, Any, List, Generator
from pathlib import Path
from icecream import ic

from .unified_models import unified_processor, UnifiedTransaction
from .unified_field_library import field_library


class UnifiedStateProcessor:
    """
    Unified processor that can handle data from any state using the unified models.
    """
    
    def __init__(self):
        self.processor = unified_processor
    
    def process_state_data(self, state: str, data_source: Any) -> List[UnifiedTransaction]:
        """
        Process data from any state into unified transactions.
        
        Args:
            state: State identifier (e.g., 'texas', 'oklahoma')
            data_source: Can be a file path, list of records, or generator
            
        Returns:
            List of unified transactions
        """
        if isinstance(data_source, (str, Path)):
            # Handle file path
            file_path = Path(data_source)
            return self.processor.process_file(file_path, state)
        
        elif isinstance(data_source, list):
            # Handle list of records
            return self.processor.process_records(data_source, state)
        
        elif hasattr(data_source, '__iter__'):
            # Handle generator or iterator
            records = list(data_source)
            return self.processor.process_records(records, state)
        
        else:
            raise ValueError(f"Unsupported data source type: {type(data_source)}")
    
    def process_multiple_states(self, state_data: Dict[str, Any]) -> Dict[str, List[UnifiedTransaction]]:
        """
        Process data from multiple states at once.
        
        Args:
            state_data: Dictionary mapping state names to data sources
            
        Returns:
            Dictionary mapping state names to lists of unified transactions
        """
        results = {}
        
        for state, data_source in state_data.items():
            ic(f"Processing {state} data...")
            try:
                transactions = self.process_state_data(state, data_source)
                results[state] = transactions
                ic(f"Processed {len(transactions)} transactions from {state}")
            except Exception as e:
                ic(f"Error processing {state}: {e}")
                results[state] = []
        
        return results
    
    def get_cross_state_analysis(self, all_transactions: Dict[str, List[UnifiedTransaction]]) -> Dict[str, Any]:
        """
        Perform cross-state analysis on unified transactions.
        
        Args:
            all_transactions: Dictionary mapping state names to transaction lists
            
        Returns:
            Analysis results
        """
        analysis = {
            "total_transactions": 0,
            "total_amount": 0,
            "by_state": {},
            "by_type": {},
            "by_month": {},
            "top_contributors": {},
            "top_committees": {}
        }
        
        # Flatten all transactions
        all_tx = []
        for state, transactions in all_transactions.items():
            all_tx.extend(transactions)
            analysis["by_state"][state] = {
                "count": len(transactions),
                "total_amount": sum(tx.amount for tx in transactions if tx.amount),
                "types": {}
            }
        
        analysis["total_transactions"] = len(all_tx)
        analysis["total_amount"] = sum(tx.amount for tx in all_tx if tx.amount)
        
        # Analyze by transaction type
        for tx in all_tx:
            tx_type = tx.transaction_type.value
            if tx_type not in analysis["by_type"]:
                analysis["by_type"][tx_type] = {"count": 0, "total_amount": 0}
            analysis["by_type"][tx_type]["count"] += 1
            if tx.amount:
                analysis["by_type"][tx_type]["total_amount"] += tx.amount
        
        # Analyze by month
        for tx in all_tx:
            if tx.transaction_date:
                month_key = f"{tx.transaction_date.year}-{tx.transaction_date.month:02d}"
                if month_key not in analysis["by_month"]:
                    analysis["by_month"][month_key] = {"count": 0, "total_amount": 0}
                analysis["by_month"][month_key]["count"] += 1
                if tx.amount:
                    analysis["by_month"][month_key]["total_amount"] += tx.amount
        
        # Find top contributors
        contributor_totals = {}
        for tx in all_tx:
            if tx.contributor and tx.amount:
                contributor_key = tx.contributor.full_name
                if contributor_key not in contributor_totals:
                    contributor_totals[contributor_key] = 0
                contributor_totals[contributor_key] += tx.amount
        
        analysis["top_contributors"] = dict(
            sorted(contributor_totals.items(), key=lambda x: x[1], reverse=True)[:10]
        )
        
        # Find top committees
        committee_totals = {}
        for tx in all_tx:
            if tx.committee and tx.amount:
                committee_key = tx.committee.name
                if committee_key not in committee_totals:
                    committee_totals[committee_key] = 0
                committee_totals[committee_key] += tx.amount
        
        analysis["top_committees"] = dict(
            sorted(committee_totals.items(), key=lambda x: x[1], reverse=True)[:10]
        )
        
        return analysis


def integrate_with_texas_downloader():
    """
    Example of integrating unified models with existing Texas downloader.
    """
    try:
        from .texas.texas_downloader import TECDownloader
        
        # Use existing Texas downloader
        texas_data = TECDownloader.read(parquet=True)
        
        # Convert to unified format
        unified_processor = UnifiedStateProcessor()
        
        # Process Texas data
        texas_transactions = []
        for category, records in texas_data.items():
            ic(f"Processing Texas category: {category}")
            transactions = unified_processor.processor.process_records(records, "texas")
            texas_transactions.extend(transactions)
        
        ic(f"Converted {len(texas_transactions)} Texas records to unified format")
        
        return texas_transactions
        
    except ImportError:
        ic("Texas downloader not available")
        return []


def integrate_with_oklahoma_processor():
    """
    Example of integrating unified models with existing Oklahoma processor.
    """
    try:
        from .oklahoma.oklahoma import OklahomaProcessor
        
        # Use existing Oklahoma processor
        oklahoma_processor = OklahomaProcessor()
        
        # Get Oklahoma data
        oklahoma_data = oklahoma_processor.read()
        
        # Convert to unified format
        unified_processor = UnifiedStateProcessor()
        
        # Process Oklahoma data
        oklahoma_transactions = unified_processor.processor.process_records(oklahoma_data, "oklahoma")
        
        ic(f"Converted {len(oklahoma_transactions)} Oklahoma records to unified format")
        
        return oklahoma_transactions
        
    except ImportError:
        ic("Oklahoma processor not available")
        return []


def create_unified_database_schema():
    """
    Create a unified database schema that can store data from any state.
    """
    schema = """
    -- Unified Campaign Finance Database Schema
    
    CREATE TABLE unified_transactions (
        id SERIAL PRIMARY KEY,
        transaction_id VARCHAR(255),
        amount DECIMAL(15,2),
        transaction_date DATE,
        description TEXT,
        transaction_type VARCHAR(50),
        state VARCHAR(50),
        file_origin VARCHAR(255),
        download_date VARCHAR(50),
        filed_date DATE,
        amended BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE unified_persons (
        id SERIAL PRIMARY KEY,
        transaction_id INTEGER REFERENCES unified_transactions(id),
        person_role VARCHAR(50), -- 'contributor', 'recipient', 'payee'
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        middle_name VARCHAR(100),
        suffix VARCHAR(20),
        organization VARCHAR(255),
        employer VARCHAR(255),
        occupation VARCHAR(255),
        person_type VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE unified_addresses (
        id SERIAL PRIMARY KEY,
        person_id INTEGER REFERENCES unified_persons(id),
        street_1 VARCHAR(255),
        street_2 VARCHAR(255),
        city VARCHAR(100),
        state VARCHAR(10),
        zip_code VARCHAR(20),
        country VARCHAR(50),
        county VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE unified_committees (
        id SERIAL PRIMARY KEY,
        transaction_id INTEGER REFERENCES unified_transactions(id),
        name VARCHAR(255),
        committee_type VARCHAR(100),
        filer_id VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Indexes for performance
    CREATE INDEX idx_transactions_state ON unified_transactions(state);
    CREATE INDEX idx_transactions_type ON unified_transactions(transaction_type);
    CREATE INDEX idx_transactions_date ON unified_transactions(transaction_date);
    CREATE INDEX idx_transactions_amount ON unified_transactions(amount);
    CREATE INDEX idx_persons_role ON unified_persons(person_role);
    CREATE INDEX idx_persons_name ON unified_persons(last_name, first_name);
    """
    
    return schema


def export_to_unified_format(transactions: List[UnifiedTransaction], output_path: Path):
    """
    Export unified transactions to a standardized format (JSON, CSV, etc.).
    
    Args:
        transactions: List of unified transactions
        output_path: Path to save the exported data
    """
    import json
    from datetime import date
    
    # Convert transactions to JSON-serializable format
    export_data = []
    
    for tx in transactions:
        tx_dict = {
            "transaction_id": tx.transaction_id,
            "amount": float(tx.amount) if tx.amount else None,
            "transaction_date": tx.transaction_date.isoformat() if tx.transaction_date else None,
            "description": tx.description,
            "transaction_type": tx.transaction_type.value,
            "state": tx.state,
            "file_origin": tx.file_origin,
            "download_date": tx.download_date,
            "filed_date": tx.filed_date.isoformat() if tx.filed_date else None,
            "amended": tx.amended,
            "contributor": None,
            "recipient": None,
            "payee": None,
            "committee": None
        }
        
        # Add person data
        if tx.contributor:
            tx_dict["contributor"] = {
                "full_name": tx.contributor.full_name,
                "first_name": tx.contributor.first_name,
                "last_name": tx.contributor.last_name,
                "employer": tx.contributor.employer,
                "occupation": tx.contributor.occupation,
                "person_type": tx.contributor.person_type.value,
                "address": {
                    "street_1": tx.contributor.address.street_1 if tx.contributor.address else None,
                    "city": tx.contributor.address.city if tx.contributor.address else None,
                    "state": tx.contributor.address.state if tx.contributor.address else None,
                    "zip_code": tx.contributor.address.zip_code if tx.contributor.address else None
                } if tx.contributor.address else None
            }
        
        if tx.recipient:
            tx_dict["recipient"] = {
                "full_name": tx.recipient.full_name,
                "first_name": tx.recipient.first_name,
                "last_name": tx.recipient.last_name,
                "employer": tx.recipient.employer,
                "occupation": tx.recipient.occupation,
                "person_type": tx.recipient.person_type.value
            }
        
        if tx.payee:
            tx_dict["payee"] = {
                "full_name": tx.payee.full_name,
                "first_name": tx.payee.first_name,
                "last_name": tx.payee.last_name,
                "employer": tx.payee.employer,
                "occupation": tx.payee.occupation,
                "person_type": tx.payee.person_type.value
            }
        
        if tx.committee:
            tx_dict["committee"] = {
                "name": tx.committee.name,
                "committee_type": tx.committee.committee_type,
                "filer_id": tx.committee.filer_id
            }
        
        export_data.append(tx_dict)
    
    # Save to file
    with open(output_path, 'w') as f:
        json.dump(export_data, f, indent=2)
    
    ic(f"Exported {len(transactions)} transactions to {output_path}")


# Global unified processor instance
unified_state_processor = UnifiedStateProcessor() 