"""
Unified Models for Campaign Finance Data

These models can handle data from any state by automatically mapping
state-specific fields to unified fields using the field library.
"""

from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
import re
from pathlib import Path

from .unified_field_library import field_library, FieldCategory, FieldType


class TransactionType(Enum):
    """Types of campaign finance transactions"""
    CONTRIBUTION = "contribution"
    EXPENDITURE = "expenditure"
    LOAN = "loan"
    PLEDGE = "pledge"
    REFUND = "refund"
    TRANSFER = "transfer"
    OTHER = "other"


class PersonType(Enum):
    """Types of persons in campaign finance data"""
    INDIVIDUAL = "individual"
    ORGANIZATION = "organization"
    COMMITTEE = "committee"
    CANDIDATE = "candidate"
    UNKNOWN = "unknown"


@dataclass
class UnifiedAddress:
    """Unified address model"""
    street_1: Optional[str] = None
    street_2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    country: Optional[str] = None
    county: Optional[str] = None
    
    def __post_init__(self):
        """Normalize address data after initialization"""
        if self.state:
            self.state = self.state.upper().strip()
        if self.city:
            self.city = self.city.strip()
        if self.zip_code:
            self.zip_code = str(self.zip_code).strip()


@dataclass
class UnifiedPerson:
    """Unified person model"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    middle_name: Optional[str] = None
    suffix: Optional[str] = None
    organization: Optional[str] = None
    employer: Optional[str] = None
    occupation: Optional[str] = None
    job_title: Optional[str] = None
    person_type: PersonType = PersonType.UNKNOWN
    address: Optional[UnifiedAddress] = None
    
    def __post_init__(self):
        """Normalize person data after initialization"""
        if self.first_name:
            self.first_name = self.first_name.strip()
        if self.last_name:
            self.last_name = self.last_name.strip()
        if self.organization:
            self.organization = self.organization.strip()
        if self.employer:
            self.employer = self.employer.strip()
        if self.occupation:
            self.occupation = self.occupation.strip()
    
    @property
    def full_name(self) -> str:
        """Get the full name of the person"""
        parts = []
        if self.first_name:
            parts.append(self.first_name)
        if self.middle_name:
            parts.append(self.middle_name)
        if self.last_name:
            parts.append(self.last_name)
        if self.suffix:
            parts.append(self.suffix)
        
        if parts:
            return " ".join(parts)
        elif self.organization:
            return self.organization
        else:
            return "Unknown"


@dataclass
class UnifiedCommittee:
    """Unified committee model"""
    name: Optional[str] = None
    committee_type: Optional[str] = None
    filer_id: Optional[str] = None
    address: Optional[UnifiedAddress] = None
    
    def __post_init__(self):
        """Normalize committee data after initialization"""
        if self.name:
            self.name = self.name.strip()
        if self.committee_type:
            self.committee_type = self.committee_type.strip()


@dataclass
class UnifiedTransaction:
    """Unified transaction model for all campaign finance transactions"""
    # Core transaction fields
    transaction_id: Optional[str] = None
    amount: Optional[Decimal] = None
    transaction_date: Optional[date] = None
    description: Optional[str] = None
    transaction_type: TransactionType = TransactionType.OTHER
    
    # Person/Entity fields
    contributor: Optional[UnifiedPerson] = None
    recipient: Optional[UnifiedPerson] = None
    payee: Optional[UnifiedPerson] = None
    
    # Committee fields
    committee: Optional[UnifiedCommittee] = None
    
    # Administrative fields
    filed_date: Optional[date] = None
    amended: bool = False
    
    # Metadata fields
    state: Optional[str] = None
    file_origin: Optional[str] = None
    download_date: Optional[str] = None
    
    # Raw data for debugging
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Normalize transaction data after initialization"""
        if self.description:
            self.description = self.description.strip()
        if self.transaction_id:
            self.transaction_id = str(self.transaction_id).strip()


class UnifiedModelBuilder:
    """
    Builder class that creates unified models from state-specific data
    by automatically mapping fields using the field library.
    """
    
    def __init__(self, state: str):
        self.state = state
        self.field_mappings = {
            mapping.state_field: mapping.unified_field
            for mapping in field_library.get_state_mappings(state)
        }
    
    def build_transaction(self, raw_data: Dict[str, Any]) -> UnifiedTransaction:
        """
        Build a unified transaction from raw state-specific data.
        
        Args:
            raw_data: Dictionary containing state-specific field data
            
        Returns:
            UnifiedTransaction object with normalized data
        """
        # Initialize the transaction
        transaction = UnifiedTransaction(
            state=self.state,
            raw_data=raw_data.copy()
        )
        
        # Map core transaction fields
        transaction.transaction_id = self._get_field_value(raw_data, "transaction_id")
        transaction.amount = self._parse_amount(self._get_field_value(raw_data, "amount"))
        transaction.transaction_date = self._parse_date(self._get_field_value(raw_data, "transaction_date"))
        transaction.description = self._get_field_value(raw_data, "description")
        transaction.transaction_type = self._determine_transaction_type(raw_data)
        
        # Map person fields
        transaction.contributor = self._build_person(raw_data, "contributor")
        transaction.recipient = self._build_person(raw_data, "recipient")
        transaction.payee = self._build_person(raw_data, "payee")
        
        # Map committee fields
        transaction.committee = self._build_committee(raw_data)
        
        # Map administrative fields
        transaction.filed_date = self._parse_date(self._get_field_value(raw_data, "filed_date"))
        transaction.amended = self._parse_boolean(self._get_field_value(raw_data, "amended"))
        
        # Map metadata fields
        transaction.file_origin = raw_data.get("file_origin")
        transaction.download_date = raw_data.get("download_date")
        
        return transaction
    
    def _get_field_value(self, raw_data: Dict[str, Any], unified_field: str) -> Optional[Any]:
        """Get the value for a unified field from raw data"""
        # First try direct mapping
        for state_field, mapped_field in self.field_mappings.items():
            if mapped_field == unified_field and state_field in raw_data:
                return raw_data[state_field]
        
        # If no direct mapping, try fuzzy matching
        for field_name, value in raw_data.items():
            if self._fuzzy_match(field_name, unified_field):
                return value
        
        return None
    
    def _fuzzy_match(self, state_field: str, unified_field: str) -> bool:
        """Check if a state field roughly matches a unified field"""
        state_normalized = self._normalize_field_name(state_field)
        unified_normalized = self._normalize_field_name(unified_field)
        
        # Check for partial matches
        if unified_normalized in state_normalized or state_normalized in unified_normalized:
            return True
        
        # Check for word overlap
        state_words = set(state_normalized.split('_'))
        unified_words = set(unified_normalized.split('_'))
        
        if state_words and unified_words:
            overlap = len(state_words.intersection(unified_words))
            return overlap > 0
        
        return False
    
    def _normalize_field_name(self, field_name: str) -> str:
        """Normalize a field name for comparison"""
        normalized = field_name.lower()
        normalized = re.sub(r'[^a-z0-9]', '_', normalized)
        normalized = re.sub(r'_+', '_', normalized)
        return normalized.strip('_')
    
    def _build_person(self, raw_data: Dict[str, Any], person_role: str) -> Optional[UnifiedPerson]:
        """Build a unified person from raw data"""
        # Look for person fields with the given role
        person_data = {}
        
        # Map person fields
        person_fields = {
            "first_name": f"{person_role}_first_name",
            "last_name": f"{person_role}_last_name", 
            "middle_name": f"{person_role}_middle_name",
            "suffix": f"{person_role}_suffix",
            "organization": f"{person_role}_organization",
            "employer": f"{person_role}_employer",
            "occupation": f"{person_role}_occupation"
        }
        
        for unified_field, role_field in person_fields.items():
            value = self._get_field_value(raw_data, role_field)
            if value:
                person_data[unified_field] = value
        
        # If we found any person data, create the person
        if person_data:
            # Determine person type
            person_type = PersonType.UNKNOWN
            if person_data.get("organization"):
                person_type = PersonType.ORGANIZATION
            elif person_data.get("first_name") or person_data.get("last_name"):
                person_type = PersonType.INDIVIDUAL
            
            # Build address
            address = self._build_address(raw_data, person_role)
            
            return UnifiedPerson(
                **person_data,
                person_type=person_type,
                address=address
            )
        
        return None
    
    def _build_address(self, raw_data: Dict[str, Any], entity_role: str) -> Optional[UnifiedAddress]:
        """Build a unified address from raw data"""
        address_data = {}
        
        # Map address fields
        address_fields = {
            "street_1": f"{entity_role}_address_street_1",
            "street_2": f"{entity_role}_address_street_2",
            "city": f"{entity_role}_address_city",
            "state": f"{entity_role}_address_state",
            "zip_code": f"{entity_role}_address_zip",
            "country": f"{entity_role}_address_country",
            "county": f"{entity_role}_address_county"
        }
        
        for unified_field, role_field in address_fields.items():
            value = self._get_field_value(raw_data, role_field)
            if value:
                address_data[unified_field] = value
        
        # If we found any address data, create the address
        if address_data:
            return UnifiedAddress(**address_data)
        
        return None
    
    def _build_committee(self, raw_data: Dict[str, Any]) -> Optional[UnifiedCommittee]:
        """Build a unified committee from raw data"""
        committee_data = {}
        
        # Map committee fields
        committee_fields = {
            "name": "committee_name",
            "committee_type": "committee_type",
            "filer_id": "committee_filer_id"
        }
        
        for unified_field, field_name in committee_fields.items():
            value = self._get_field_value(raw_data, field_name)
            if value:
                committee_data[unified_field] = value
        
        # If we found any committee data, create the committee
        if committee_data:
            return UnifiedCommittee(**committee_data)
        
        return None
    
    def _parse_amount(self, value: Any) -> Optional[Decimal]:
        """Parse an amount value to Decimal"""
        if value is None:
            return None
        
        try:
            # Remove currency symbols and commas
            if isinstance(value, str):
                value = re.sub(r'[^\d.-]', '', value)
            
            return Decimal(str(value))
        except (ValueError, TypeError):
            return None
    
    def _parse_date(self, value: Any) -> Optional[date]:
        """Parse a date value"""
        if value is None:
            return None
        
        try:
            if isinstance(value, str):
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d']:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            elif isinstance(value, (date, datetime)):
                return value.date() if isinstance(value, datetime) else value
            
            return None
        except (ValueError, TypeError):
            return None
    
    def _parse_boolean(self, value: Any) -> bool:
        """Parse a boolean value"""
        if value is None:
            return False
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            return value.lower() in ['true', 'yes', 'y', '1', 't']
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        return False
    
    def _determine_transaction_type(self, raw_data: Dict[str, Any]) -> TransactionType:
        """Determine the transaction type from raw data"""
        # Check for explicit transaction type
        type_value = self._get_field_value(raw_data, "transaction_type")
        if type_value:
            type_str = str(type_value).lower()
            for transaction_type in TransactionType:
                if transaction_type.value in type_str:
                    return transaction_type
        
        # Infer from field names
        field_names = [k.lower() for k in raw_data.keys()]
        
        if any('contribution' in name for name in field_names):
            return TransactionType.CONTRIBUTION
        elif any('expenditure' in name for name in field_names):
            return TransactionType.EXPENDITURE
        elif any('loan' in name for name in field_names):
            return TransactionType.LOAN
        elif any('pledge' in name for name in field_names):
            return TransactionType.PLEDGE
        elif any('refund' in name for name in field_names):
            return TransactionType.REFUND
        elif any('transfer' in name for name in field_names):
            return TransactionType.TRANSFER
        
        return TransactionType.OTHER


class UnifiedDataProcessor:
    """
    High-level processor for converting state-specific data to unified models.
    """
    
    def __init__(self):
        self.builders = {}
    
    def get_builder(self, state: str) -> UnifiedModelBuilder:
        """Get or create a model builder for a specific state"""
        if state not in self.builders:
            self.builders[state] = UnifiedModelBuilder(state)
        return self.builders[state]
    
    def process_record(self, raw_data: Dict[str, Any], state: str) -> UnifiedTransaction:
        """
        Process a single record from any state into a unified transaction.
        
        Args:
            raw_data: Raw data dictionary from the state
            state: State identifier (e.g., 'texas', 'oklahoma')
            
        Returns:
            UnifiedTransaction object
        """
        builder = self.get_builder(state)
        return builder.build_transaction(raw_data)
    
    def process_records(self, records: List[Dict[str, Any]], state: str) -> List[UnifiedTransaction]:
        """
        Process multiple records from any state into unified transactions.
        
        Args:
            records: List of raw data dictionaries
            state: State identifier
            
        Returns:
            List of UnifiedTransaction objects
        """
        builder = self.get_builder(state)
        return [builder.build_transaction(record) for record in records]
    
    def process_file(self, file_path: Path, state: str) -> List[UnifiedTransaction]:
        """
        Process an entire file from any state into unified transactions.
        
        Args:
            file_path: Path to the data file
            state: State identifier
            
        Returns:
            List of UnifiedTransaction objects
        """
        from ..funcs.csv_reader import FileReader
        
        reader = FileReader()
        records = []
        
        if file_path.suffix.lower() == '.parquet':
            records = list(reader.read_parquet(file_path))
        elif file_path.suffix.lower() == '.csv':
            records = list(reader.read_csv(file_path))
        
        return self.process_records(records, state)


# Global processor instance
unified_processor = UnifiedDataProcessor() 