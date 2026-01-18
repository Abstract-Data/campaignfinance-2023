import hashlib
from typing import Dict, Any, Optional, Tuple
from datetime import date
from decimal import Decimal
import re

def normalize_address_data(address_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize address data for consistent storage and comparison.
    
    Args:
        address_data: Dictionary containing address fields
        
    Returns:
        Normalized address dictionary
    """
    normalized = {}
    
    # Normalize street address
    if address_data.get('street_addr1'):
        normalized['street_addr1'] = address_data['street_addr1'].strip().upper()
    if address_data.get('street_addr2'):
        normalized['street_addr2'] = address_data['street_addr2'].strip().upper()
    
    # Normalize city
    if address_data.get('city'):
        normalized['city'] = address_data['city'].strip().upper()
    
    # Normalize state (ensure 2-letter code)
    if address_data.get('state_cd'):
        state = address_data['state_cd'].strip().upper()
        # Map common state variations to standard codes
        state_mapping = {
            'TEXAS': 'TX',
            'CALIFORNIA': 'CA',
            'NEW YORK': 'NY',
            'FLORIDA': 'FL',
            'ILLINOIS': 'IL',
            # Add more as needed
        }
        normalized['state_cd'] = state_mapping.get(state, state)
    
    # Normalize county
    if address_data.get('county_cd'):
        normalized['county_cd'] = address_data['county_cd'].strip().upper()
    
    # Normalize country
    if address_data.get('country_cd'):
        country = address_data['country_cd'].strip().upper()
        country_mapping = {
            'UNITED STATES': 'USA',
            'US': 'USA',
            'U.S.': 'USA',
            'U.S.A.': 'USA',
            'AMERICA': 'USA',
        }
        normalized['country_cd'] = country_mapping.get(country, country)
    
    # Normalize postal code
    if address_data.get('postal_code'):
        postal = re.sub(r'[^\d]', '', address_data['postal_code'])
        if len(postal) == 9:  # ZIP+4 format
            normalized['postal_code'] = f"{postal[:5]}-{postal[5:]}"
        else:
            normalized['postal_code'] = postal
    
    # Normalize region
    if address_data.get('region'):
        normalized['region'] = address_data['region'].strip().upper()
    
    # Normalize mailing address fields (same logic as street address)
    mailing_fields = ['mailing_addr1', 'mailing_addr2', 'mailing_city', 
                     'mailing_state_cd', 'mailing_county_cd', 'mailing_country_cd',
                     'mailing_postal_code', 'mailing_region']
    
    for field in mailing_fields:
        if address_data.get(field):
            if field.endswith('_cd') and field != 'mailing_postal_code':
                # Apply same normalization as street address codes
                value = address_data[field].strip().upper()
                if field == 'mailing_state_cd':
                    state_mapping = {'TEXAS': 'TX', 'CALIFORNIA': 'CA', 'NEW YORK': 'NY'}
                    normalized[field] = state_mapping.get(value, value)
                elif field == 'mailing_country_cd':
                    country_mapping = {'UNITED STATES': 'USA', 'US': 'USA', 'U.S.': 'USA'}
                    normalized[field] = country_mapping.get(value, value)
                else:
                    normalized[field] = value
            elif field == 'mailing_postal_code':
                postal = re.sub(r'[^\d]', '', address_data[field])
                if len(postal) == 9:
                    normalized[field] = f"{postal[:5]}-{postal[5:]}"
                else:
                    normalized[field] = postal
            else:
                normalized[field] = address_data[field].strip().upper()
    
    # Normalize phone fields
    if address_data.get('primary_phone_number'):
        phone = re.sub(r'[^\d]', '', address_data['primary_phone_number'])
        if len(phone) == 10:
            normalized['primary_phone_number'] = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        else:
            normalized['primary_phone_number'] = phone
    
    if address_data.get('primary_phone_ext'):
        normalized['primary_phone_ext'] = address_data['primary_phone_ext'].strip()
    
    # Boolean flags
    if 'primary_usa_phone_flag' in address_data:
        normalized['primary_usa_phone_flag'] = address_data['primary_usa_phone_flag']
    
    return normalized

def generate_address_hash(address_data: Dict[str, Any]) -> str:
    """
    Generate a hash for address deduplication.
    
    Args:
        address_data: Normalized address data
        
    Returns:
        SHA256 hash string
    """
    # Create a canonical string representation
    canonical_parts = []
    
    # Street address
    if address_data.get('street_addr1'):
        canonical_parts.append(f"street1:{address_data['street_addr1']}")
    if address_data.get('street_addr2'):
        canonical_parts.append(f"street2:{address_data['street_addr2']}")
    
    # City, State, Postal Code
    if address_data.get('city'):
        canonical_parts.append(f"city:{address_data['city']}")
    if address_data.get('state_cd'):
        canonical_parts.append(f"state:{address_data['state_cd']}")
    if address_data.get('postal_code'):
        canonical_parts.append(f"postal:{address_data['postal_code']}")
    
    # Country
    if address_data.get('country_cd'):
        canonical_parts.append(f"country:{address_data['country_cd']}")
    
    # County
    if address_data.get('county_cd'):
        canonical_parts.append(f"county:{address_data['county_cd']}")
    
    # Phone (if available)
    if address_data.get('primary_phone_number'):
        canonical_parts.append(f"phone:{address_data['primary_phone_number']}")
    
    canonical_string = "|".join(sorted(canonical_parts))
    return hashlib.sha256(canonical_string.encode('utf-8')).hexdigest()

def normalize_person_data(person_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize person data for consistent storage and comparison.
    
    Args:
        person_data: Dictionary containing person fields
        
    Returns:
        Normalized person dictionary
    """
    normalized = {}
    
    # Person type
    if person_data.get('person_type'):
        normalized['person_type'] = person_data['person_type'].strip().upper()
    
    # Normalize name fields
    name_fields = ['name_organization', 'name_last', 'name_first', 'name_short']
    for field in name_fields:
        if person_data.get(field):
            normalized[field] = person_data[field].strip().upper()
    
    # Normalize name codes
    code_fields = ['name_prefix_cd', 'name_suffix_cd']
    for field in code_fields:
        if person_data.get(field):
            normalized[field] = person_data[field].strip().upper()
    
    # Normalize employment fields
    employment_fields = ['employer', 'occupation', 'job_title']
    for field in employment_fields:
        if person_data.get(field):
            normalized[field] = person_data[field].strip().upper()
    
    # Normalize PAC fields
    if person_data.get('pac_fein'):
        fein = re.sub(r'[^\d]', '', person_data['pac_fein'])
        normalized['pac_fein'] = fein
    
    if 'oos_pac_flag' in person_data:
        normalized['oos_pac_flag'] = person_data['oos_pac_flag']
    
    # Normalize law firm fields
    law_firm_fields = ['law_firm_name', 'spouse_law_firm_name', 
                      'parent1_law_firm_name', 'parent2_law_firm_name']
    for field in law_firm_fields:
        if person_data.get(field):
            normalized[field] = person_data[field].strip().upper()
    
    return normalized

def generate_person_hash(person_data: Dict[str, Any]) -> str:
    """
    Generate a hash for person deduplication.
    
    Args:
        person_data: Normalized person data
        
    Returns:
        SHA256 hash string
    """
    canonical_parts = []
    
    # Person type
    if person_data.get('person_type'):
        canonical_parts.append(f"type:{person_data['person_type']}")
    
    # Name fields (different logic for individual vs entity)
    if person_data.get('person_type') == 'ENTITY':
        if person_data.get('name_organization'):
            canonical_parts.append(f"org:{person_data['name_organization']}")
    else:  # INDIVIDUAL
        if person_data.get('name_last'):
            canonical_parts.append(f"last:{person_data['name_last']}")
        if person_data.get('name_first'):
            canonical_parts.append(f"first:{person_data['name_first']}")
        if person_data.get('name_suffix_cd'):
            canonical_parts.append(f"suffix:{person_data['name_suffix_cd']}")
    
    # Employment information
    if person_data.get('employer'):
        canonical_parts.append(f"employer:{person_data['employer']}")
    if person_data.get('occupation'):
        canonical_parts.append(f"occupation:{person_data['occupation']}")
    
    # PAC information
    if person_data.get('pac_fein'):
        canonical_parts.append(f"fein:{person_data['pac_fein']}")
    
    # Law firm information
    if person_data.get('law_firm_name'):
        canonical_parts.append(f"lawfirm:{person_data['law_firm_name']}")
    
    canonical_string = "|".join(sorted(canonical_parts))
    return hashlib.sha256(canonical_string.encode('utf-8')).hexdigest()

def extract_address_from_record(record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Extract address fields from a record with optional prefix.
    
    Args:
        record: Record dictionary
        prefix: Prefix for address fields (e.g., "contributor", "payee")
        
    Returns:
        Dictionary containing address fields
    """
    address_fields = [
        'street_addr1', 'street_addr2', 'city', 'state_cd', 'county_cd',
        'country_cd', 'postal_code', 'region',
        'mailing_addr1', 'mailing_addr2', 'mailing_city', 'mailing_state_cd',
        'mailing_county_cd', 'mailing_country_cd', 'mailing_postal_code', 'mailing_region',
        'primary_usa_phone_flag', 'primary_phone_number', 'primary_phone_ext'
    ]
    
    address_data = {}
    for field in address_fields:
        if prefix:
            prefixed_field = f"{prefix}_{field}"
            if prefixed_field in record:
                address_data[field] = record[prefixed_field]
        else:
            if field in record:
                address_data[field] = record[field]
    
    return address_data

def extract_person_from_record(record: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    """
    Extract person fields from a record with optional prefix.
    
    Args:
        record: Record dictionary
        prefix: Prefix for person fields (e.g., "contributor", "payee")
        
    Returns:
        Dictionary containing person fields
    """
    person_fields = [
        'person_type', 'name_organization', 'name_last', 'name_first',
        'name_prefix_cd', 'name_suffix_cd', 'name_short',
        'employer', 'occupation', 'job_title',
        'pac_fein', 'oos_pac_flag',
        'law_firm_name', 'spouse_law_firm_name', 'parent1_law_firm_name', 'parent2_law_firm_name'
    ]
    
    person_data = {}
    for field in person_fields:
        if prefix:
            prefixed_field = f"{prefix}_{field}"
            if prefixed_field in record:
                person_data[field] = record[prefixed_field]
        else:
            if field in record:
                person_data[field] = record[field]
    
    return person_data

def clean_boolean_field(value: Any) -> Optional[bool]:
    """
    Clean boolean fields from various input formats.
    
    Args:
        value: Input value
        
    Returns:
        Boolean value or None
    """
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

def clean_decimal_field(value: Any) -> Optional[Decimal]:
    """
    Clean decimal fields from various input formats.
    
    Args:
        value: Input value
        
    Returns:
        Decimal value or None
    """
    if value is None:
        return None
    
    if isinstance(value, Decimal):
        return value
    
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    
    if isinstance(value, str):
        # Remove currency symbols and commas
        cleaned = re.sub(r'[^\d.-]', '', value.strip())
        if cleaned:
            try:
                return Decimal(cleaned)
            except (ValueError, TypeError):
                pass
    
    return None

def clean_date_field(value: Any) -> Optional[date]:
    """
    Clean date fields from various input formats.
    
    Args:
        value: Input value
        
    Returns:
        Date value or None
    """
    if value is None:
        return None
    
    if isinstance(value, date):
        return value
    
    if isinstance(value, str):
        # Handle various date formats
        cleaned = value.strip()
        if cleaned:
            # Try common formats
            formats = ['%Y%m%d', '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
            for fmt in formats:
                try:
                    return date.strptime(cleaned, fmt)
                except ValueError:
                    continue
    
    return None 