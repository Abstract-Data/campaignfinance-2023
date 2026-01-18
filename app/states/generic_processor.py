from typing import Dict, List, Any, Optional, Tuple
from sqlmodel import Session, select
from .generic_models import (
    CampaignAddress, CampaignPerson, CampaignCommittee, CampaignContribution, 
    CampaignExpenditure, CampaignLoan, CampaignReport
)
from .generic_normalization import GenericNormalizer
import pandas as pd
from datetime import date
from decimal import Decimal

class GenericCampaignProcessor:
    """
    Generic processor for campaign finance data across different states.
    Handles person and address deduplication with state-specific field mapping.
    """
    
    def __init__(self, session: Session):
        self.session = session
        self.normalizer = GenericNormalizer()
        self.address_cache: Dict[str, int] = {}  # hash -> id
        self.person_cache: Dict[str, int] = {}   # hash -> id
        self.committee_cache: Dict[str, int] = {}  # committee_id -> id
    
    def get_or_create_address(self, address_data: Dict[str, Any], state: str = 'generic') -> Optional[int]:
        """
        Get existing address ID or create new address record.
        
        Args:
            address_data: Raw address data
            state: State for field mapping
            
        Returns:
            Address ID or None if no valid address data
        """
        if not address_data or not any(address_data.values()):
            return None
        
        # Normalize and hash the address
        normalized = self.normalizer.normalize_address(address_data, state)
        address_hash = self.normalizer.generate_address_hash(normalized)
        
        # Check cache first
        if address_hash in self.address_cache:
            return self.address_cache[address_hash]
        
        # Check database
        stmt = select(CampaignAddress).where(CampaignAddress.address_hash == address_hash)
        existing = self.session.exec(stmt).first()
        
        if existing:
            self.address_cache[address_hash] = existing.id
            return existing.id
        
        # Create new address
        new_address = CampaignAddress(
            address_hash=address_hash,
            **normalized
        )
        self.session.add(new_address)
        self.session.flush()  # Get the ID
        
        self.address_cache[address_hash] = new_address.id
        return new_address.id
    
    def get_or_create_person(self, person_data: Dict[str, Any], state: str = 'generic') -> Optional[int]:
        """
        Get existing person ID or create new person record.
        
        Args:
            person_data: Raw person data
            state: State for field mapping
            
        Returns:
            Person ID or None if no valid person data
        """
        if not person_data or not any(person_data.values()):
            return None
        
        # Normalize and hash the person
        normalized = self.normalizer.normalize_person(person_data, state)
        person_hash = self.normalizer.generate_person_hash(normalized)
        
        # Check cache first
        if person_hash in self.person_cache:
            return self.person_cache[person_hash]
        
        # Check database
        stmt = select(CampaignPerson).where(CampaignPerson.person_hash == person_hash)
        existing = self.session.exec(stmt).first()
        
        if existing:
            self.person_cache[person_hash] = existing.id
            return existing.id
        
        # Create new person
        new_person = CampaignPerson(
            person_hash=person_hash,
            **normalized
        )
        self.session.add(new_person)
        self.session.flush()  # Get the ID
        
        self.person_cache[person_hash] = new_person.id
        return new_person.id
    
    def get_or_create_committee(self, committee_data: Dict[str, Any], state: str = 'generic') -> Optional[int]:
        """
        Get existing committee ID or create new committee record.
        
        Args:
            committee_data: Raw committee data
            state: State for field mapping
            
        Returns:
            Committee ID or None if no valid committee data
        """
        if not committee_data:
            return None
        
        # Extract committee ID
        committee_id = None
        if state == 'oklahoma':
            committee_id = committee_data.get('Org ID')
        elif state == 'texas':
            committee_id = committee_data.get('filerIdent')
        else:
            # Generic fallback
            committee_id = committee_data.get('committee_id') or committee_data.get('Org ID')
        
        if not committee_id:
            return None
        
        # Check cache first
        if committee_id in self.committee_cache:
            return self.committee_cache[committee_id]
        
        # Check database
        stmt = select(CampaignCommittee).where(CampaignCommittee.committee_id == committee_id)
        existing = self.session.exec(stmt).first()
        
        if existing:
            self.committee_cache[committee_id] = existing.id
            return existing.id
        
        # Extract committee information
        committee_name = None
        committee_type = None
        candidate_name = None
        
        if state == 'oklahoma':
            committee_name = committee_data.get('Committee Name')
            committee_type = committee_data.get('Committee Type')
            candidate_name = committee_data.get('Candidate Name')
        elif state == 'texas':
            committee_name = committee_data.get('filerName')
            committee_type = committee_data.get('filerTypeCd')
            candidate_name = committee_data.get('candidateName')
        
        # Create new committee
        new_committee = CampaignCommittee(
            committee_id=committee_id,
            committee_name=committee_name or "Unknown Committee",
            committee_type=committee_type or "Unknown",
            state=state.upper(),
            candidate_name=candidate_name,
            # Add other fields as needed
        )
        self.session.add(new_committee)
        self.session.flush()  # Get the ID
        
        self.committee_cache[committee_id] = new_committee.id
        return new_committee.id
    
    def process_contribution(self, record: Dict[str, Any], state: str = 'generic') -> CampaignContribution:
        """Process a contribution record."""
        # Extract person and address data
        person_data = self._extract_person_data(record, state)
        address_data = self._extract_address_data(record, state)
        
        # Get or create normalized records
        contributor_id = self.get_or_create_person(person_data, state)
        contributor_address_id = self.get_or_create_address(address_data, state)
        committee_id = self.get_or_create_committee(record, state)
        
        # Normalize transaction data
        transaction_data = self.normalizer.normalize_transaction(record, 'contribution', state)
        
        # Create contribution record
        contribution = CampaignContribution(
            transaction_id=transaction_data.get('transaction_id', ''),
            contribution_type=transaction_data.get('type', 'Unknown'),
            contribution_date=transaction_data.get('date', date.today()),
            filed_date=transaction_data.get('filed_date'),
            amount=transaction_data.get('amount', Decimal('0')),
            description=transaction_data.get('description'),
            source_type=transaction_data.get('source_type'),
            amended=transaction_data.get('amended'),
            committee_id=committee_id,
            contributor_id=contributor_id,
            contributor_address_id=contributor_address_id,
            state=state.upper(),
            raw_data=record
        )
        
        return contribution
    
    def process_expenditure(self, record: Dict[str, Any], state: str = 'generic') -> CampaignExpenditure:
        """Process an expenditure record."""
        # Extract person and address data
        person_data = self._extract_person_data(record, state)
        address_data = self._extract_address_data(record, state)
        
        # Get or create normalized records
        payee_id = self.get_or_create_person(person_data, state)
        payee_address_id = self.get_or_create_address(address_data, state)
        committee_id = self.get_or_create_committee(record, state)
        
        # Normalize transaction data
        transaction_data = self.normalizer.normalize_transaction(record, 'expenditure', state)
        
        # Create expenditure record
        expenditure = CampaignExpenditure(
            transaction_id=transaction_data.get('transaction_id', ''),
            expenditure_type=transaction_data.get('type', 'Unknown'),
            expenditure_date=transaction_data.get('date', date.today()),
            filed_date=transaction_data.get('filed_date'),
            amount=transaction_data.get('amount', Decimal('0')),
            description=transaction_data.get('description'),
            purpose=transaction_data.get('purpose'),
            amended=transaction_data.get('amended'),
            committee_id=committee_id,
            payee_id=payee_id,
            payee_address_id=payee_address_id,
            state=state.upper(),
            raw_data=record
        )
        
        return expenditure
    
    def process_loan(self, record: Dict[str, Any], state: str = 'generic') -> CampaignLoan:
        """Process a loan record."""
        # Extract person and address data
        person_data = self._extract_person_data(record, state)
        address_data = self._extract_address_data(record, state)
        
        # Get or create normalized records
        lender_id = self.get_or_create_person(person_data, state)
        lender_address_id = self.get_or_create_address(address_data, state)
        committee_id = self.get_or_create_committee(record, state)
        
        # Normalize transaction data
        transaction_data = self.normalizer.normalize_transaction(record, 'loan', state)
        
        # Create loan record
        loan = CampaignLoan(
            transaction_id=transaction_data.get('transaction_id', ''),
            loan_type=transaction_data.get('type', 'Unknown'),
            loan_date=transaction_data.get('date', date.today()),
            filed_date=transaction_data.get('filed_date'),
            amount=transaction_data.get('amount', Decimal('0')),
            description=transaction_data.get('description'),
            amended=transaction_data.get('amended'),
            committee_id=committee_id,
            lender_id=lender_id,
            lender_address_id=lender_address_id,
            state=state.upper(),
            raw_data=record
        )
        
        return loan
    
    def _extract_person_data(self, record: Dict[str, Any], state: str) -> Dict[str, Any]:
        """Extract person data from record based on state."""
        person_data = {}
        
        if state == 'oklahoma':
            # Oklahoma uses Last Name, First Name, Middle Name, Suffix
            person_data.update({
                'Last Name': record.get('Last Name'),
                'First Name': record.get('First Name'),
                'Middle Name': record.get('Middle Name'),
                'Suffix': record.get('Suffix'),
                'Employer': record.get('Employer'),
                'Occupation': record.get('Occupation'),
            })
        elif state == 'texas':
            # Texas uses various prefixed fields
            # This would need to be expanded based on the specific record type
            # For now, using generic approach
            for key, value in record.items():
                if any(prefix in key for prefix in ['contributor', 'payee', 'filer']):
                    person_data[key] = value
        else:
            # Generic approach - copy all fields that might be person-related
            person_fields = ['name', 'first', 'last', 'middle', 'suffix', 'employer', 'occupation']
            for key, value in record.items():
                if any(field in key.lower() for field in person_fields):
                    person_data[key] = value
        
        return person_data
    
    def _extract_address_data(self, record: Dict[str, Any], state: str) -> Dict[str, Any]:
        """Extract address data from record based on state."""
        address_data = {}
        
        if state == 'oklahoma':
            # Oklahoma uses Address 1, Address 2, City, State, Zip
            address_data.update({
                'Address 1': record.get('Address 1'),
                'Address 2': record.get('Address 2'),
                'City': record.get('City'),
                'State': record.get('State'),
                'Zip': record.get('Zip'),
            })
        elif state == 'texas':
            # Texas uses various prefixed fields
            # This would need to be expanded based on the specific record type
            for key, value in record.items():
                if any(prefix in key for prefix in ['contributor', 'payee', 'filer']) and any(addr_field in key for addr_field in ['Addr', 'City', 'State', 'Postal', 'Country']):
                    address_data[key] = value
        else:
            # Generic approach - copy all fields that might be address-related
            address_fields = ['address', 'city', 'state', 'zip', 'postal', 'country']
            for key, value in record.items():
                if any(field in key.lower() for field in address_fields):
                    address_data[key] = value
        
        return address_data
    
    def process_csv_file(self, df: pd.DataFrame, transaction_type: str, state: str = 'generic') -> List[Any]:
        """
        Process a CSV DataFrame and return list of normalized models.
        
        Args:
            df: Pandas DataFrame with raw data
            transaction_type: Type of transaction ('contribution', 'expenditure', 'loan')
            state: State for field mapping
            
        Returns:
            List of processed model instances
        """
        records = []
        
        for _, row in df.iterrows():
            record = row.to_dict()
            
            try:
                if transaction_type == 'contribution':
                    processed = self.process_contribution(record, state)
                elif transaction_type == 'expenditure':
                    processed = self.process_expenditure(record, state)
                elif transaction_type == 'loan':
                    processed = self.process_loan(record, state)
                else:
                    # Handle other transaction types as needed
                    continue
                
                records.append(processed)
                
            except Exception as e:
                print(f"Error processing record: {e}")
                continue
        
        return records
    
    def save_records(self, records: List[Any]) -> None:
        """Save processed records to database."""
        for record in records:
            self.session.add(record)
        
        self.session.commit()
    
    def clear_cache(self) -> None:
        """Clear all caches."""
        self.address_cache.clear()
        self.person_cache.clear()
        self.committee_cache.clear()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            'addresses_processed': len(self.address_cache),
            'persons_processed': len(self.person_cache),
            'committees_processed': len(self.committee_cache),
        } 