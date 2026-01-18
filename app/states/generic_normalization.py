import hashlib
import re
from typing import Dict, Any, Optional, List, Tuple
from datetime import date, datetime
from decimal import Decimal
from .generic_models import CampaignAddress, CampaignPerson, CampaignCommittee, CampaignContribution, CampaignExpenditure, CampaignLoan

class GenericNormalizer:
    """
    Generic normalizer for campaign finance data across different states.
    Handles field mapping, data cleaning, and normalization.
    """
    
    def __init__(self):
        # Common field mappings for different states
        self.field_mappings = {
            'texas': {
                'address': {
                    'street_addr1': ['contributorStreetAddr1', 'payeeStreetAddr1', 'filerStreetAddr1'],
                    'street_addr2': ['contributorStreetAddr2', 'payeeStreetAddr2', 'filerStreetAddr2'],
                    'city': ['contributorStreetCity', 'payeeStreetCity', 'filerStreetCity'],
                    'state': ['contributorStreetStateCd', 'payeeStreetStateCd', 'filerStreetStateCd'],
                    'zip_code': ['contributorStreetPostalCode', 'payeeStreetPostalCode', 'filerStreetPostalCode'],
                    'country': ['contributorStreetCountryCd', 'payeeStreetCountryCd', 'filerStreetCountryCd'],
                    'county': ['contributorStreetCountyCd', 'payeeStreetCountyCd', 'filerStreetCountyCd'],
                },
                'person': {
                    'name_organization': ['contributorNameOrganization', 'payeeNameOrganization', 'filerNameOrganization'],
                    'name_last': ['contributorNameLast', 'payeeNameLast', 'filerNameLast'],
                    'name_first': ['contributorNameFirst', 'payeeNameFirst', 'filerNameFirst'],
                    'name_middle': ['contributorNameShort', 'payeeNameShort', 'filerNameShort'],
                    'name_suffix': ['contributorNameSuffixCd', 'payeeNameSuffixCd', 'filerNameSuffixCd'],
                    'name_prefix': ['contributorNamePrefixCd', 'payeeNamePrefixCd', 'filerNamePrefixCd'],
                    'employer': ['contributorEmployer', 'payeeEmployer', 'filerEmployer'],
                    'occupation': ['contributorOccupation', 'payeeOccupation', 'filerOccupation'],
                    'job_title': ['contributorJobTitle', 'payeeJobTitle', 'filerJobTitle'],
                    'pac_fein': ['contributorPacFein', 'payeePacFein', 'filerPacFein'],
                },
                'transaction': {
                    'transaction_id': ['contributionInfoId', 'expendInfoId', 'loanInfoId'],
                    'amount': ['contributionAmount', 'expendAmount', 'loanAmount'],
                    'date': ['contributionDt', 'expendDt', 'loanDt'],
                    'description': ['contributionDescr', 'expendDescr', 'loanDescr'],
                    'filed_date': ['receivedDt'],
                }
            },
            'oklahoma': {
                'address': {
                    'address_1': ['Address 1'],
                    'address_2': ['Address 2'],
                    'city': ['City'],
                    'state': ['State'],
                    'zip_code': ['Zip'],
                },
                'person': {
                    'name_organization': ['Last Name'],  # For entities, Last Name contains org name
                    'name_last': ['Last Name'],
                    'name_first': ['First Name'],
                    'name_middle': ['Middle Name'],
                    'name_suffix': ['Suffix'],
                    'employer': ['Employer'],
                    'occupation': ['Occupation'],
                },
                'transaction': {
                    'transaction_id': ['Receipt ID', 'Expenditure ID'],
                    'amount': ['Receipt Amount', 'Expenditure Amount'],
                    'date': ['Receipt Date', 'Expenditure Date'],
                    'description': ['Description'],
                    'filed_date': ['Filed Date'],
                    'type': ['Receipt Type', 'Expenditure Type'],
                    'source_type': ['Receipt Source Type'],
                    'purpose': ['Purpose'],
                    'amended': ['Amended'],
                }
            }
        }
    
    def normalize_address(self, address_data: Dict[str, Any], state: str = 'generic') -> Dict[str, Any]:
        """
        Normalize address data to standard format.
        
        Args:
            address_data: Raw address data
            state: State for field mapping
            
        Returns:
            Normalized address dictionary
        """
        normalized = {}
        
        # Get field mappings for state
        mappings = self.field_mappings.get(state, {}).get('address', {})
        
        # Map and normalize address fields
        if 'address_1' in mappings:
            for field in mappings['address_1']:
                if field in address_data and address_data[field]:
                    normalized['address_1'] = self._clean_string(address_data[field])
                    break
        
        if 'address_2' in mappings:
            for field in mappings['address_2']:
                if field in address_data and address_data[field]:
                    normalized['address_2'] = self._clean_string(address_data[field])
                    break
        
        if 'city' in mappings:
            for field in mappings['city']:
                if field in address_data and address_data[field]:
                    normalized['city'] = self._clean_string(address_data[field]).upper()
                    break
        
        if 'state' in mappings:
            for field in mappings['state']:
                if field in address_data and address_data[field]:
                    state_code = self._normalize_state_code(address_data[field])
                    if state_code:
                        normalized['state'] = state_code
                    break
        
        if 'zip_code' in mappings:
            for field in mappings['zip_code']:
                if field in address_data and address_data[field]:
                    normalized['zip_code'] = self._normalize_zip_code(address_data[field])
                    break
        
        if 'country' in mappings:
            for field in mappings['country']:
                if field in address_data and address_data[field]:
                    normalized['country'] = self._normalize_country_code(address_data[field])
                    break
        
        if 'county' in mappings:
            for field in mappings['county']:
                if field in address_data and address_data[field]:
                    normalized['county'] = self._clean_string(address_data[field]).upper()
                    break
        
        return normalized
    
    def normalize_person(self, person_data: Dict[str, Any], state: str = 'generic') -> Dict[str, Any]:
        """
        Normalize person data to standard format.
        
        Args:
            person_data: Raw person data
            state: State for field mapping
            
        Returns:
            Normalized person dictionary
        """
        normalized = {}
        
        # Get field mappings for state
        mappings = self.field_mappings.get(state, {}).get('person', {})
        
        # Map and normalize person fields
        if 'name_organization' in mappings:
            for field in mappings['name_organization']:
                if field in person_data and person_data[field]:
                    normalized['name_organization'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'name_last' in mappings:
            for field in mappings['name_last']:
                if field in person_data and person_data[field]:
                    normalized['name_last'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'name_first' in mappings:
            for field in mappings['name_first']:
                if field in person_data and person_data[field]:
                    normalized['name_first'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'name_middle' in mappings:
            for field in mappings['name_middle']:
                if field in person_data and person_data[field]:
                    normalized['name_middle'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'name_suffix' in mappings:
            for field in mappings['name_suffix']:
                if field in person_data and person_data[field]:
                    normalized['name_suffix'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'name_prefix' in mappings:
            for field in mappings['name_prefix']:
                if field in person_data and person_data[field]:
                    normalized['name_prefix'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'employer' in mappings:
            for field in mappings['employer']:
                if field in person_data and person_data[field]:
                    normalized['employer'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'occupation' in mappings:
            for field in mappings['occupation']:
                if field in person_data and person_data[field]:
                    normalized['occupation'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'job_title' in mappings:
            for field in mappings['job_title']:
                if field in person_data and person_data[field]:
                    normalized['job_title'] = self._clean_string(person_data[field]).upper()
                    break
        
        if 'pac_fein' in mappings:
            for field in mappings['pac_fein']:
                if field in person_data and person_data[field]:
                    normalized['pac_fein'] = self._clean_fein(person_data[field])
                    break
        
        # Determine person type
        normalized['person_type'] = self._determine_person_type(person_data, state)
        
        return normalized
    
    def normalize_transaction(self, transaction_data: Dict[str, Any], transaction_type: str, state: str = 'generic') -> Dict[str, Any]:
        """
        Normalize transaction data to standard format.
        
        Args:
            transaction_data: Raw transaction data
            transaction_type: Type of transaction (contribution, expenditure, loan)
            state: State for field mapping
            
        Returns:
            Normalized transaction dictionary
        """
        normalized = {}
        
        # Get field mappings for state
        mappings = self.field_mappings.get(state, {}).get('transaction', {})
        
        # Map and normalize transaction fields
        if 'transaction_id' in mappings:
            for field in mappings['transaction_id']:
                if field in transaction_data and transaction_data[field]:
                    normalized['transaction_id'] = str(transaction_data[field])
                    break
        
        if 'amount' in mappings:
            for field in mappings['amount']:
                if field in transaction_data and transaction_data[field]:
                    normalized['amount'] = self._normalize_amount(transaction_data[field])
                    break
        
        if 'date' in mappings:
            for field in mappings['date']:
                if field in transaction_data and transaction_data[field]:
                    normalized['date'] = self._normalize_date(transaction_data[field])
                    break
        
        if 'description' in mappings:
            for field in mappings['description']:
                if field in transaction_data and transaction_data[field]:
                    normalized['description'] = self._clean_string(transaction_data[field])
                    break
        
        if 'filed_date' in mappings:
            for field in mappings['filed_date']:
                if field in transaction_data and transaction_data[field]:
                    normalized['filed_date'] = self._normalize_date(transaction_data[field])
                    break
        
        if 'type' in mappings:
            for field in mappings['type']:
                if field in transaction_data and transaction_data[field]:
                    normalized['type'] = self._clean_string(transaction_data[field])
                    break
        
        if 'source_type' in mappings:
            for field in mappings['source_type']:
                if field in transaction_data and transaction_data[field]:
                    normalized['source_type'] = self._clean_string(transaction_data[field])
                    break
        
        if 'purpose' in mappings:
            for field in mappings['purpose']:
                if field in transaction_data and transaction_data[field]:
                    normalized['purpose'] = self._clean_string(transaction_data[field])
                    break
        
        if 'amended' in mappings:
            for field in mappings['amended']:
                if field in transaction_data and transaction_data[field]:
                    normalized['amended'] = self._normalize_boolean(transaction_data[field])
                    break
        
        # Add transaction type and state
        normalized['transaction_type'] = transaction_type
        normalized['state'] = state.upper()
        
        return normalized
    
    def generate_address_hash(self, address_data: Dict[str, Any]) -> str:
        """Generate hash for address deduplication."""
        canonical_parts = []
        
        if address_data.get('address_1'):
            canonical_parts.append(f"addr1:{address_data['address_1']}")
        if address_data.get('address_2'):
            canonical_parts.append(f"addr2:{address_data['address_2']}")
        if address_data.get('city'):
            canonical_parts.append(f"city:{address_data['city']}")
        if address_data.get('state'):
            canonical_parts.append(f"state:{address_data['state']}")
        if address_data.get('zip_code'):
            canonical_parts.append(f"zip:{address_data['zip_code']}")
        if address_data.get('country'):
            canonical_parts.append(f"country:{address_data['country']}")
        
        canonical_string = "|".join(sorted(canonical_parts))
        return hashlib.sha256(canonical_string.encode('utf-8')).hexdigest()
    
    def generate_person_hash(self, person_data: Dict[str, Any]) -> str:
        """Generate hash for person deduplication."""
        canonical_parts = []
        
        if person_data.get('person_type'):
            canonical_parts.append(f"type:{person_data['person_type']}")
        
        if person_data.get('person_type') == 'ENTITY':
            if person_data.get('name_organization'):
                canonical_parts.append(f"org:{person_data['name_organization']}")
        else:  # INDIVIDUAL
            if person_data.get('name_last'):
                canonical_parts.append(f"last:{person_data['name_last']}")
            if person_data.get('name_first'):
                canonical_parts.append(f"first:{person_data['name_first']}")
            if person_data.get('name_suffix'):
                canonical_parts.append(f"suffix:{person_data['name_suffix']}")
        
        if person_data.get('employer'):
            canonical_parts.append(f"employer:{person_data['employer']}")
        if person_data.get('occupation'):
            canonical_parts.append(f"occupation:{person_data['occupation']}")
        if person_data.get('pac_fein'):
            canonical_parts.append(f"fein:{person_data['pac_fein']}")
        
        canonical_string = "|".join(sorted(canonical_parts))
        return hashlib.sha256(canonical_string.encode('utf-8')).hexdigest()
    
    def _clean_string(self, value: Any) -> str:
        """Clean and normalize string values."""
        if value is None:
            return ""
        return str(value).strip()
    
    def _normalize_state_code(self, value: Any) -> Optional[str]:
        """Normalize state codes to 2-letter format."""
        if not value:
            return None
        
        state = str(value).strip().upper()
        state_mapping = {
            'TEXAS': 'TX', 'CALIFORNIA': 'CA', 'NEW YORK': 'NY', 'FLORIDA': 'FL',
            'ILLINOIS': 'IL', 'PENNSYLVANIA': 'PA', 'OHIO': 'OH', 'GEORGIA': 'GA',
            'NORTH CAROLINA': 'NC', 'MICHIGAN': 'MI', 'OKLAHOMA': 'OK'
        }
        return state_mapping.get(state, state)
    
    def _normalize_country_code(self, value: Any) -> Optional[str]:
        """Normalize country codes."""
        if not value:
            return None
        
        country = str(value).strip().upper()
        country_mapping = {
            'UNITED STATES': 'USA', 'US': 'USA', 'U.S.': 'USA', 'U.S.A.': 'USA',
            'AMERICA': 'USA', 'UNITED STATES OF AMERICA': 'USA'
        }
        return country_mapping.get(country, country)
    
    def _normalize_zip_code(self, value: Any) -> str:
        """Normalize ZIP codes."""
        if not value:
            return ""
        
        zip_code = re.sub(r'[^\d]', '', str(value))
        if len(zip_code) == 9:  # ZIP+4 format
            return f"{zip_code[:5]}-{zip_code[5:]}"
        return zip_code
    
    def _normalize_amount(self, value: Any) -> Decimal:
        """Normalize monetary amounts."""
        if value is None:
            return Decimal('0')
        
        if isinstance(value, Decimal):
            return value
        
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        
        # Remove currency symbols and commas
        cleaned = re.sub(r'[^\d.-]', '', str(value).strip())
        if cleaned:
            try:
                return Decimal(cleaned)
            except (ValueError, TypeError):
                pass
        
        return Decimal('0')
    
    def _normalize_date(self, value: Any) -> Optional[date]:
        """Normalize date values."""
        if value is None:
            return None
        
        if isinstance(value, date):
            return value
        
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                # Try common formats
                formats = ['%Y%m%d', '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%Y']
                for fmt in formats:
                    try:
                        return datetime.strptime(cleaned, fmt).date()
                    except ValueError:
                        continue
        
        return None
    
    def _normalize_boolean(self, value: Any) -> Optional[bool]:
        """Normalize boolean values."""
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            cleaned = value.strip().upper()
            if cleaned in ['Y', 'YES', 'TRUE', '1']:
                return True
            elif cleaned in ['N', 'NO', 'FALSE', '0']:
                return False
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        return None
    
    def _clean_fein(self, value: Any) -> str:
        """Clean Federal Employer Identification Number."""
        if not value:
            return ""
        return re.sub(r'[^\d]', '', str(value))
    
    def _determine_person_type(self, person_data: Dict[str, Any], state: str) -> str:
        """Determine person type based on available data."""
        # Check for organization name first
        org_fields = ['name_organization', 'Last Name']  # Last Name can contain org names
        for field in org_fields:
            if field in person_data and person_data[field]:
                value = str(person_data[field]).strip().upper()
                # Common entity indicators
                if any(indicator in value for indicator in ['INC', 'LLC', 'CORP', 'COMPANY', 'ASSOCIATION', 'PAC', 'COMMITTEE', 'BANK', 'UNION']):
                    return 'ENTITY'
                # Check for non-individual indicators
                if value in ['NON-ITEMIZED CONTRIBUTOR', 'NON-ITEMIZED RECIPIENT']:
                    return 'ENTITY'
        
        # Check for individual name fields
        name_fields = ['name_first', 'First Name', 'name_last', 'Last Name']
        for field in name_fields:
            if field in person_data and person_data[field]:
                return 'INDIVIDUAL'
        
        # Default to individual if we can't determine
        return 'INDIVIDUAL' 