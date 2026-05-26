"""
Unified Field Library for Campaign Finance Data

This module provides a comprehensive mapping system for campaign finance fields
across different states, enabling consistent data processing and analysis.
"""

from typing import Dict, List, Set, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
from pathlib import Path


class FieldCategory(Enum):
    """Semantic categories for campaign finance fields"""
    # Core transaction fields
    TRANSACTION_ID = "transaction_id"
    AMOUNT = "amount"
    DATE = "date"
    DESCRIPTION = "description"
    TYPE = "type"
    STATUS = "status"
    
    # Person/Entity fields
    PERSON_NAME = "person_name"
    PERSON_ORGANIZATION = "person_organization"
    PERSON_ADDRESS = "person_address"
    PERSON_CONTACT = "person_contact"
    PERSON_EMPLOYMENT = "person_employment"
    PERSON_IDENTIFICATION = "person_identification"
    
    # Committee/Organization fields
    COMMITTEE_NAME = "committee_name"
    COMMITTEE_TYPE = "committee_type"
    COMMITTEE_ADDRESS = "committee_address"
    COMMITTEE_CONTACT = "committee_contact"
    COMMITTEE_IDENTIFICATION = "committee_identification"
    
    # Campaign/Election fields
    CANDIDATE_NAME = "candidate_name"
    OFFICE_SOUGHT = "office_sought"
    OFFICE_HELD = "office_held"
    ELECTION_INFO = "election_info"
    DISTRICT_INFO = "district_info"
    
    # Financial fields
    LOAN_INFO = "loan_info"
    PLEDGE_INFO = "pledge_info"
    ASSET_INFO = "asset_info"
    DEBT_INFO = "debt_info"
    EXPENSE_CATEGORY = "expense_category"
    
    # Administrative fields
    FILING_INFO = "filing_info"
    REPORT_INFO = "report_info"
    AMENDMENT_INFO = "amendment_info"
    VERIFICATION_INFO = "verification_info"
    
    # Metadata fields
    SOURCE_INFO = "source_info"
    PROCESSING_INFO = "processing_info"
    VALIDATION_INFO = "validation_info"


class FieldType(Enum):
    """Data types for fields"""
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    CODE = "code"
    IDENTIFIER = "identifier"


@dataclass
class FieldDefinition:
    """Definition for a unified field"""
    name: str
    category: FieldCategory
    field_type: FieldType
    description: str
    examples: List[str] = field(default_factory=list)
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    normalization_rules: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StateFieldMapping:
    """Mapping from state-specific fields to unified fields"""
    state: str
    state_field: str
    unified_field: str
    confidence: float = 1.0  # 0.0 to 1.0
    notes: str = ""


_OFFICER_FIELD_REGISTRY: dict[str, dict[str, list[str]]] = {
    "texas": {
        "treasurer_name": [
            "treasurer_name",
            "treasurer",
            "treasurer_first_name",
            "treasurer_last_name",
        ],
        "chair_name": ["chair_name", "chair", "chair_first_name", "chair_last_name"],
        "committee_id": ["filer_id", "committee_id", "filer_number"],
        "committee_name": ["committee_name", "filer_name", "committee_title"],
    },
    "oklahoma": {
        "treasurer_name": ["treasurer_name", "treasurer"],
        "chair_name": ["chair_name", "chair"],
        "committee_id": ["committee_id", "filer_id"],
        "committee_name": ["committee_name", "committee_title"],
    },
}


class UnifiedFieldLibrary:
    """
    Unified field library for campaign finance data across all states.
    Provides field mapping, categorization, and normalization capabilities.
    """
    
    def __init__(self):
        self.unified_fields: Dict[str, FieldDefinition] = {}
        self.state_mappings: Dict[str, List[StateFieldMapping]] = {}
        self.field_categories: Dict[FieldCategory, Set[str]] = {}
        self._initialize_unified_fields()
        self._initialize_state_mappings()
    
    def _initialize_unified_fields(self):
        """Initialize the core unified field definitions"""
        
        # Core transaction fields
        self.unified_fields.update({
            "transaction_id": FieldDefinition(
                name="transaction_id",
                category=FieldCategory.TRANSACTION_ID,
                field_type=FieldType.IDENTIFIER,
                description="Unique identifier for a financial transaction",
                examples=["contributionInfoId", "expendInfoId", "Receipt ID", "Expenditure ID"],
                validation_rules={"required": False, "unique": True}  # Not all records have IDs
            ),
            "amount": FieldDefinition(
                name="amount",
                category=FieldCategory.AMOUNT,
                field_type=FieldType.CURRENCY,
                description="Monetary amount of the transaction (negative values indicate refunds/corrections)",
                examples=["contributionAmount", "expendAmount", "Receipt Amount", "Expenditure Amount"],
                validation_rules={"required": False}  # Allow negative amounts for refunds
            ),
            "transaction_date": FieldDefinition(
                name="transaction_date",
                category=FieldCategory.DATE,
                field_type=FieldType.DATE,
                description="Date when the transaction occurred",
                examples=["contributionDt", "expendDt", "Receipt Date", "Expenditure Date"],
                validation_rules={"required": False}  # Not all records have dates
            ),
            "description": FieldDefinition(
                name="description",
                category=FieldCategory.DESCRIPTION,
                field_type=FieldType.STRING,
                description="Description or purpose of the transaction",
                examples=["contributionDescr", "expendDescr", "Description", "Purpose"],
                validation_rules={"max_length": 1000}
            ),
            "transaction_type": FieldDefinition(
                name="transaction_type",
                category=FieldCategory.TYPE,
                field_type=FieldType.CODE,
                description="Type of transaction (contribution, expenditure, loan, etc.)",
                examples=["Receipt Type", "Expenditure Type"],
                validation_rules={"enum_values": ["contribution", "expenditure", "loan", "pledge"]}
            ),
        })
        
        # Person/Entity fields
        self.unified_fields.update({
            "person_first_name": FieldDefinition(
                name="person_first_name",
                category=FieldCategory.PERSON_NAME,
                field_type=FieldType.STRING,
                description="First name of a person",
                examples=["contributorNameFirst", "payeeNameFirst", "First Name"],
                validation_rules={"max_length": 100}
            ),
            "person_last_name": FieldDefinition(
                name="person_last_name",
                category=FieldCategory.PERSON_NAME,
                field_type=FieldType.STRING,
                description="Last name of a person",
                examples=["contributorNameLast", "payeeNameLast", "Last Name"],
                validation_rules={"max_length": 100}
            ),
            "person_organization": FieldDefinition(
                name="person_organization",
                category=FieldCategory.PERSON_ORGANIZATION,
                field_type=FieldType.STRING,
                description="Organization name for entity contributors",
                examples=["contributorNameOrganization", "Committee Name"],
                validation_rules={"max_length": 200}
            ),
            "person_employer": FieldDefinition(
                name="person_employer",
                category=FieldCategory.PERSON_EMPLOYMENT,
                field_type=FieldType.STRING,
                description="Employer of the person",
                examples=["contributorEmployer", "Employer"],
                validation_rules={"max_length": 200}
            ),
            "person_occupation": FieldDefinition(
                name="person_occupation",
                category=FieldCategory.PERSON_EMPLOYMENT,
                field_type=FieldType.STRING,
                description="Occupation of the person",
                examples=["contributorOccupation", "Occupation"],
                validation_rules={"max_length": 200}
            ),
        })
        
        # Address fields
        self.unified_fields.update({
            "address_street_1": FieldDefinition(
                name="address_street_1",
                category=FieldCategory.PERSON_ADDRESS,
                field_type=FieldType.STRING,
                description="Primary street address",
                examples=["contributorStreetAddr1", "Address 1"],
                validation_rules={"max_length": 200}
            ),
            "address_street_2": FieldDefinition(
                name="address_street_2",
                category=FieldCategory.PERSON_ADDRESS,
                field_type=FieldType.STRING,
                description="Secondary street address",
                examples=["contributorStreetAddr2", "Address 2"],
                validation_rules={"max_length": 200}
            ),
            "address_city": FieldDefinition(
                name="address_city",
                category=FieldCategory.PERSON_ADDRESS,
                field_type=FieldType.STRING,
                description="City name",
                examples=["contributorStreetCity", "City"],
                validation_rules={"max_length": 100}
            ),
            "address_state": FieldDefinition(
                name="address_state",
                category=FieldCategory.PERSON_ADDRESS,
                field_type=FieldType.CODE,
                description="State or province code",
                examples=["contributorStreetStateCd", "State"],
                validation_rules={"max_length": 2}
            ),
            "address_zip": FieldDefinition(
                name="address_zip",
                category=FieldCategory.PERSON_ADDRESS,
                field_type=FieldType.STRING,
                description="Postal/ZIP code",
                examples=["contributorStreetPostalCode", "Zip"],
                validation_rules={"max_length": 10}
            ),
        })
        
        # Committee fields
        self.unified_fields.update({
            "committee_name": FieldDefinition(
                name="committee_name",
                category=FieldCategory.COMMITTEE_NAME,
                field_type=FieldType.STRING,
                description="Name of the political committee",
                examples=["filerName", "Committee Name"],
                validation_rules={"max_length": 200}
            ),
            "committee_type": FieldDefinition(
                name="committee_type",
                category=FieldCategory.COMMITTEE_TYPE,
                field_type=FieldType.CODE,
                description="Type of political committee",
                examples=["filerTypeCd", "Committee Type"],
                validation_rules={"enum_values": ["candidate", "pac", "party", "other"]}
            ),
        })
        
        # Filing/Administrative fields
        self.unified_fields.update({
            "filed_date": FieldDefinition(
                name="filed_date",
                category=FieldCategory.FILING_INFO,
                field_type=FieldType.DATE,
                description="Date when the report was filed",
                examples=["filedDt", "Filed Date"],
                validation_rules={"required": False}  # Not all records have this
            ),
            "amended": FieldDefinition(
                name="amended",
                category=FieldCategory.AMENDMENT_INFO,
                field_type=FieldType.BOOLEAN,
                description="Whether this is an amended filing",
                examples=["Amended"],
                validation_rules={"default": False}
            ),
        })
        
        # Debt fields
        self.unified_fields.update({
            "loan_guaranteed_flag": FieldDefinition(
                name="loan_guaranteed_flag",
                category=FieldCategory.FILING_INFO,
                field_type=FieldType.BOOLEAN,
                description="Whether the loan/debt has a guarantor",
                examples=["loanGuaranteedFlag"],
                validation_rules={"default": False}
            ),
            "loan_guarantee_amount": FieldDefinition(
                name="loan_guarantee_amount",
                category=FieldCategory.AMOUNT,
                field_type=FieldType.CURRENCY,
                description="Amount guaranteed by a third party",
                examples=["loanGuaranteeAmount"],
                validation_rules={"required": False}
            ),
        })
        
        # Travel fields
        self.unified_fields.update({
            "parent_type": FieldDefinition(
                name="parent_type",
                category=FieldCategory.TYPE,
                field_type=FieldType.CODE,
                description="Parent transaction type (RCPT, EXPN, etc.)",
                examples=["parentType"],
                validation_rules={"required": False}
            ),
            "parent_id": FieldDefinition(
                name="parent_id",
                category=FieldCategory.TRANSACTION_ID,
                field_type=FieldType.IDENTIFIER,
                description="Parent transaction ID",
                examples=["parentId"],
                validation_rules={"required": False}
            ),
            "parent_amount": FieldDefinition(
                name="parent_amount",
                category=FieldCategory.AMOUNT,
                field_type=FieldType.CURRENCY,
                description="Parent transaction amount",
                examples=["parentAmount"],
                validation_rules={"required": False}
            ),
            "parent_full_name": FieldDefinition(
                name="parent_full_name",
                category=FieldCategory.PERSON_NAME,
                field_type=FieldType.STRING,
                description="Full name associated with parent transaction",
                examples=["parentFullName"],
                validation_rules={"required": False}
            ),
            "transportation_type_cd": FieldDefinition(
                name="transportation_type_cd",
                category=FieldCategory.TYPE,
                field_type=FieldType.CODE,
                description="Transportation type code",
                examples=["transportationTypeCd"],
                validation_rules={"required": False}
            ),
            "transportation_type_descr": FieldDefinition(
                name="transportation_type_descr",
                category=FieldCategory.DESCRIPTION,
                field_type=FieldType.STRING,
                description="Transportation type description",
                examples=["transportationTypeDescr"],
                validation_rules={"required": False}
            ),
            "departure_city": FieldDefinition(
                name="departure_city",
                category=FieldCategory.PERSON_ADDRESS,
                field_type=FieldType.STRING,
                description="Departure city for travel",
                examples=["departureCity"],
                validation_rules={"required": False}
            ),
            "arrival_city": FieldDefinition(
                name="arrival_city",
                category=FieldCategory.PERSON_ADDRESS,
                field_type=FieldType.STRING,
                description="Arrival city for travel",
                examples=["arrivalCity"],
                validation_rules={"required": False}
            ),
            "departure_dt": FieldDefinition(
                name="departure_dt",
                category=FieldCategory.DATE,
                field_type=FieldType.DATE,
                description="Departure date for travel",
                examples=["departureDt"],
                validation_rules={"required": False}
            ),
            "arrival_dt": FieldDefinition(
                name="arrival_dt",
                category=FieldCategory.DATE,
                field_type=FieldType.DATE,
                description="Arrival date for travel",
                examples=["arrivalDt"],
                validation_rules={"required": False}
            ),
            "travel_purpose": FieldDefinition(
                name="travel_purpose",
                category=FieldCategory.DESCRIPTION,
                field_type=FieldType.STRING,
                description="Purpose of the travel",
                examples=["travelPurpose"],
                validation_rules={"required": False}
            ),
        })
        
        # Asset fields
        self.unified_fields.update({
            "asset_descr": FieldDefinition(
                name="asset_descr",
                category=FieldCategory.DESCRIPTION,
                field_type=FieldType.STRING,
                description="Description of the campaign asset",
                examples=["assetDescr"],
                validation_rules={"required": False}
            ),
        })
        
        # Build category index
        for field_name, field_def in self.unified_fields.items():
            if field_def.category not in self.field_categories:
                self.field_categories[field_def.category] = set()
            self.field_categories[field_def.category].add(field_name)
    
    def _initialize_state_mappings(self):
        """Initialize state-specific field mappings"""
        
        # Texas mappings
        self.state_mappings["texas"] = [
            # Transaction fields
            StateFieldMapping("texas", "contributionInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "expendInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "loanInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "contributionAmount", "amount", 1.0),
            StateFieldMapping("texas", "expendAmount", "amount", 1.0),
            StateFieldMapping("texas", "loanAmount", "amount", 1.0),
            StateFieldMapping("texas", "contributionDt", "transaction_date", 1.0),
            StateFieldMapping("texas", "expendDt", "transaction_date", 1.0),
            StateFieldMapping("texas", "loanDt", "transaction_date", 1.0),
            StateFieldMapping("texas", "contributionDescr", "description", 1.0),
            StateFieldMapping("texas", "expendDescr", "description", 1.0),
            StateFieldMapping("texas", "loanDescr", "description", 1.0),
            
            # Person fields
            StateFieldMapping("texas", "contributorNameFirst", "person_first_name", 1.0),
            StateFieldMapping("texas", "contributorNameLast", "person_last_name", 1.0),
            StateFieldMapping("texas", "contributorNameOrganization", "person_organization", 1.0),
            StateFieldMapping("texas", "contributorEmployer", "person_employer", 1.0),
            StateFieldMapping("texas", "contributorOccupation", "person_occupation", 1.0),
            
            # Address fields (contributor)
            StateFieldMapping("texas", "contributorStreetAddr1", "address_street_1", 1.0),
            StateFieldMapping("texas", "contributorStreetAddr2", "address_street_2", 1.0),
            StateFieldMapping("texas", "contributorStreetCity", "address_city", 1.0),
            StateFieldMapping("texas", "contributorStreetStateCd", "address_state", 1.0),
            StateFieldMapping("texas", "contributorStreetPostalCode", "address_zip", 1.0),
            
            # Address fields (filer) - for filers file
            StateFieldMapping("texas", "filerStreetAddr1", "address_street_1", 1.0),
            StateFieldMapping("texas", "filerStreetAddr2", "address_street_2", 1.0),
            StateFieldMapping("texas", "filerStreetCity", "address_city", 1.0),
            StateFieldMapping("texas", "filerStreetStateCd", "address_state", 1.0),
            StateFieldMapping("texas", "filerStreetPostalCode", "address_zip", 1.0),
            
            # Person fields (filer) - for filers file
            StateFieldMapping("texas", "filerNameFirst", "person_first_name", 1.0),
            StateFieldMapping("texas", "filerNameLast", "person_last_name", 1.0),
            
            # Committee fields
            StateFieldMapping("texas", "filerName", "committee_name", 1.0),
            StateFieldMapping("texas", "filerTypeCd", "committee_type", 1.0),
            StateFieldMapping("texas", "filerIdent", "committee_filer_id", 1.0),
            
            # Filing fields
            StateFieldMapping("texas", "filedDt", "filed_date", 1.0),
            StateFieldMapping("texas", "receivedDt", "filed_date", 0.9),  # Also used in contributions
            
            # Debt fields
            StateFieldMapping("texas", "debtInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "loanInfoId", "transaction_id", 0.9),  # Debt uses loanInfoId in Texas
            StateFieldMapping("texas", "loanGuaranteedFlag", "loan_guaranteed_flag", 1.0),
            StateFieldMapping("texas", "loanGuaranteeAmount", "loan_guarantee_amount", 1.0),
            
            # Credit fields
            StateFieldMapping("texas", "creditInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "creditDt", "transaction_date", 1.0),
            StateFieldMapping("texas", "creditAmount", "amount", 1.0),
            StateFieldMapping("texas", "creditDescr", "description", 1.0),
            StateFieldMapping("texas", "payorNameFirst", "person_first_name", 1.0),
            StateFieldMapping("texas", "payorNameLast", "person_last_name", 1.0),
            StateFieldMapping("texas", "payorNameOrganization", "person_organization", 1.0),
            
            # Travel fields
            StateFieldMapping("texas", "travelInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "parentType", "parent_type", 1.0),
            StateFieldMapping("texas", "parentId", "parent_id", 1.0),
            StateFieldMapping("texas", "parentDt", "transaction_date", 0.9),
            StateFieldMapping("texas", "parentAmount", "parent_amount", 1.0),
            StateFieldMapping("texas", "parentFullName", "parent_full_name", 1.0),
            StateFieldMapping("texas", "transportationTypeCd", "transportation_type_cd", 1.0),
            StateFieldMapping("texas", "transportationTypeDescr", "transportation_type_descr", 1.0),
            StateFieldMapping("texas", "departureCity", "departure_city", 1.0),
            StateFieldMapping("texas", "arrivalCity", "arrival_city", 1.0),
            StateFieldMapping("texas", "departureDt", "departure_dt", 1.0),
            StateFieldMapping("texas", "arrivalDt", "arrival_dt", 1.0),
            StateFieldMapping("texas", "travelPurpose", "travel_purpose", 1.0),
            StateFieldMapping("texas", "travellerNameFirst", "person_first_name", 0.9),
            StateFieldMapping("texas", "travellerNameLast", "person_last_name", 0.9),
            
            # Asset fields
            StateFieldMapping("texas", "assetInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "assetDescr", "asset_descr", 1.0),
            
            # Pledge fields
            StateFieldMapping("texas", "pledgeInfoId", "transaction_id", 1.0),
            StateFieldMapping("texas", "pledgeDt", "transaction_date", 1.0),
            StateFieldMapping("texas", "pledgeAmount", "amount", 1.0),
            StateFieldMapping("texas", "pledgeDescr", "description", 1.0),
            StateFieldMapping("texas", "pledgerNameFirst", "person_first_name", 0.9),
            StateFieldMapping("texas", "pledgerNameLast", "person_last_name", 0.9),
            StateFieldMapping("texas", "pledgerNameOrganization", "person_organization", 0.9),
        ]
        
        # Oklahoma mappings
        self.state_mappings["oklahoma"] = [
            # Transaction fields
            StateFieldMapping("oklahoma", "Receipt ID", "transaction_id", 1.0),
            StateFieldMapping("oklahoma", "Expenditure ID", "transaction_id", 1.0),
            StateFieldMapping("oklahoma", "Receipt Amount", "amount", 1.0),
            StateFieldMapping("oklahoma", "Expenditure Amount", "amount", 1.0),
            StateFieldMapping("oklahoma", "Receipt Date", "transaction_date", 1.0),
            StateFieldMapping("oklahoma", "Expenditure Date", "transaction_date", 1.0),
            StateFieldMapping("oklahoma", "Description", "description", 1.0),
            StateFieldMapping("oklahoma", "Purpose", "description", 0.8),
            StateFieldMapping("oklahoma", "Receipt Type", "transaction_type", 1.0),
            StateFieldMapping("oklahoma", "Expenditure Type", "transaction_type", 1.0),
            
            # Person fields
            StateFieldMapping("oklahoma", "First Name", "person_first_name", 1.0),
            StateFieldMapping("oklahoma", "Last Name", "person_last_name", 1.0),
            StateFieldMapping("oklahoma", "Employer", "person_employer", 1.0),
            StateFieldMapping("oklahoma", "Occupation", "person_occupation", 1.0),
            
            # Address fields
            StateFieldMapping("oklahoma", "Address 1", "address_street_1", 1.0),
            StateFieldMapping("oklahoma", "Address 2", "address_street_2", 1.0),
            StateFieldMapping("oklahoma", "City", "address_city", 1.0),
            StateFieldMapping("oklahoma", "State", "address_state", 1.0),
            StateFieldMapping("oklahoma", "Zip", "address_zip", 1.0),
            
            # Committee fields
            StateFieldMapping("oklahoma", "Committee Name", "committee_name", 1.0),
            StateFieldMapping("oklahoma", "Committee Type", "committee_type", 1.0),
            StateFieldMapping("oklahoma", "Org ID", "committee_filer_id", 1.0),
            
            # Filing fields
            StateFieldMapping("oklahoma", "Filed Date", "filed_date", 1.0),
            StateFieldMapping("oklahoma", "Amended", "amended", 1.0),
        ]
    
    def get_unified_field(self, field_name: str) -> Optional[FieldDefinition]:
        """Get a unified field definition by name"""
        return self.unified_fields.get(field_name)
    
    def get_state_mappings(self, state: str) -> List[StateFieldMapping]:
        """Get all field mappings for a specific state"""
        return self.state_mappings.get(state, [])
    
    def map_state_field_to_unified(self, state: str, state_field: str) -> Optional[str]:
        """Map a state-specific field to a unified field name"""
        mappings = self.state_mappings.get(state, [])
        for mapping in mappings:
            if mapping.state_field == state_field:
                return mapping.unified_field
        return None
    
    def get_fields_by_category(self, category: FieldCategory) -> List[str]:
        """Get all unified field names for a specific category"""
        return list(self.field_categories.get(category, set()))
    
    def get_all_state_fields(self, state: str) -> Set[str]:
        """Get all known field names for a specific state"""
        mappings = self.state_mappings.get(state, [])
        return {mapping.state_field for mapping in mappings}
    
    def get_unified_fields_for_state(self, state: str) -> Set[str]:
        """Get all unified fields that have mappings for a specific state"""
        mappings = self.state_mappings.get(state, [])
        return {mapping.unified_field for mapping in mappings}
    
    def add_state_mapping(self, state: str, state_field: str, unified_field: str, 
                         confidence: float = 1.0, notes: str = ""):
        """Add a new state field mapping"""
        if state not in self.state_mappings:
            self.state_mappings[state] = []
        
        mapping = StateFieldMapping(
            state=state,
            state_field=state_field,
            unified_field=unified_field,
            confidence=confidence,
            notes=notes
        )
        self.state_mappings[state].append(mapping)
    
    def get_officer_fields(self, state: str) -> dict[str, list[str]]:
        """Return officer field name mappings for the given state."""
        return _OFFICER_FIELD_REGISTRY.get(state.lower(), {})

    def add_unified_field(self, field_definition: FieldDefinition):
        """Add a new unified field definition"""
        self.unified_fields[field_definition.name] = field_definition
        
        # Update category index
        if field_definition.category not in self.field_categories:
            self.field_categories[field_definition.category] = set()
        self.field_categories[field_definition.category].add(field_definition.name)
    
    def export_mappings(self, file_path: Path):
        """Export all mappings to a JSON file"""
        export_data = {
            "unified_fields": {
                name: {
                    "name": field_def.name,
                    "category": field_def.category.value,
                    "field_type": field_def.field_type.value,
                    "description": field_def.description,
                    "examples": field_def.examples,
                    "validation_rules": field_def.validation_rules,
                    "normalization_rules": field_def.normalization_rules
                }
                for name, field_def in self.unified_fields.items()
            },
            "state_mappings": {
                state: [
                    {
                        "state": mapping.state,
                        "state_field": mapping.state_field,
                        "unified_field": mapping.unified_field,
                        "confidence": mapping.confidence,
                        "notes": mapping.notes
                    }
                    for mapping in mappings
                ]
                for state, mappings in self.state_mappings.items()
            }
        }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2)
    
    def import_mappings(self, file_path: Path):
        """Import mappings from a JSON file"""
        with open(file_path, 'r') as f:
            import_data = json.load(f)
        
        # Import unified fields
        for name, field_data in import_data["unified_fields"].items():
            field_def = FieldDefinition(
                name=field_data["name"],
                category=FieldCategory(field_data["category"]),
                field_type=FieldType(field_data["field_type"]),
                description=field_data["description"],
                examples=field_data.get("examples", []),
                validation_rules=field_data.get("validation_rules", {}),
                normalization_rules=field_data.get("normalization_rules", {})
            )
            self.unified_fields[name] = field_def
        
        # Import state mappings
        for state, mappings_data in import_data["state_mappings"].items():
            self.state_mappings[state] = [
                StateFieldMapping(
                    state=mapping["state"],
                    state_field=mapping["state_field"],
                    unified_field=mapping["unified_field"],
                    confidence=mapping.get("confidence", 1.0),
                    notes=mapping.get("notes", "")
                )
                for mapping in mappings_data
            ]
        
        # Rebuild category index
        self.field_categories.clear()
        for field_name, field_def in self.unified_fields.items():
            if field_def.category not in self.field_categories:
                self.field_categories[field_def.category] = set()
            self.field_categories[field_def.category].add(field_name)


# Global instance for easy access
field_library = UnifiedFieldLibrary() 