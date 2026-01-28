"""
Shared pytest fixtures and configuration for the campaign finance test suite.

This module provides reusable fixtures for:
- State configurations (Texas, Oklahoma)
- Sample record data for contributions, expenditures, etc.
- Mock database sessions
- Temporary file fixtures
- Field library instances
"""

import pytest
from pathlib import Path
from datetime import date
from typing import Dict, Any
from unittest.mock import MagicMock
import polars as pl


# =============================================================================
# State Configuration Fixtures
# =============================================================================

@pytest.fixture
def texas_config():
    """Texas state configuration for testing."""
    from app.states.texas import TEXAS_CONFIGURATION
    return TEXAS_CONFIGURATION


@pytest.fixture
def oklahoma_config():
    """Oklahoma state configuration for testing (if available)."""
    try:
        from app.states.oklahoma import OKLAHOMA_CONFIGURATION
        return OKLAHOMA_CONFIGURATION
    except ImportError:
        pytest.skip("Oklahoma configuration not available")


# =============================================================================
# Sample Record Fixtures - Texas
# =============================================================================

@pytest.fixture
def sample_texas_contribution_record() -> Dict[str, Any]:
    """
    Sample Texas contribution record matching TECContribution schema.

    This fixture provides a valid contribution record that can be used
    for testing validators, processors, and loaders.
    """
    return {
        'recordType': 'RCPT',
        'formTypeCd': 'A',
        'schedFormTypeCd': 'A1',
        'reportInfoIdent': 12345,
        'receivedDt': '20240115',
        'infoOnlyFlag': None,
        'filerIdent': 67890,
        'filerTypeCd': 'CAN',
        'filerName': 'SMITH FOR TEXAS',
        'contributionInfoId': 111222,
        'contributionDt': '20240110',
        'contributionAmount': '1000.00',
        'contributionDescr': 'Campaign contribution',
        'itemizeFlag': 'Y',
        'travelFlag': 'N',
        'contributorPersentTypeCd': 'INDIVIDUAL',
        'contributorNameOrganization': None,
        'contributorNameLast': 'DOE',
        'contributorNameSuffixCd': None,
        'contributorNameFirst': 'JOHN',
        'contributorNamePrefixCd': None,
        'contributorNameShort': None,
        'contributorNameFull': None,
        'contributorStreetCity': 'AUSTIN',
        'contributorStreetStateCd': 'TX',
        'contributorStreetCountyCd': None,
        'contributorStreetCountryCd': 'USA',
        'contributorStreetPostalCode': '78701',
        'contributorStreetRegion': None,
        'contributorEmployer': 'ACME INC',
        'contributorOccupation': 'ENGINEER',
        'contributorJobTitle': None,
        'contributorPacFein': None,
        'contributorOosPacFlag': None,
        'contributorLawFirmName': None,
        'contributorSpouseLawFirmName': None,
        'contributorParent1LawFirmName': None,
        'contributorParent2LawFirmName': None,
        'file_origin': 'contribs_01',
        'download_date': '20240120',
    }


@pytest.fixture
def sample_texas_entity_contribution_record(sample_texas_contribution_record) -> Dict[str, Any]:
    """Sample Texas contribution from an ENTITY (organization) contributor."""
    record = sample_texas_contribution_record.copy()
    record.update({
        'contributorPersentTypeCd': 'ENTITY',
        'contributorNameOrganization': 'ACME CORPORATION',
        'contributorNameFirst': None,
        'contributorNameLast': None,
        'contributionInfoId': 111223,
    })
    return record


@pytest.fixture
def sample_texas_expenditure_record() -> Dict[str, Any]:
    """
    Sample Texas expenditure record matching TECExpense schema.

    This fixture provides a valid expenditure record for testing.
    """
    return {
        'recordType': 'EXPN',
        'formTypeCd': 'F',
        'schedFormTypeCd': 'F1',
        'reportInfoIdent': 12345,
        'receivedDt': '20240115',
        'infoOnlyFlag': None,
        'filerIdent': 67890,
        'filerTypeCd': 'CAN',
        'filerName': 'SMITH FOR TEXAS',
        'expendInfoId': 333444,
        'expendDt': '20240112',
        'expendAmount': '500.00',
        'expendDescr': 'Campaign advertising',
        'expendCatCd': 'ADVERT',
        'expendCatDescr': 'Advertising',
        'itemizeFlag': 'Y',
        'travelFlag': 'N',
        'politicalExpendCd': 'Y',
        'reimburseIntendedFlag': 'N',
        'srcCorpContribFlag': 'N',
        'capitalLivingexpFlag': None,
        'payeePersentTypeCd': 'ENTITY',
        'payeeNameOrganization': 'AUSTIN SIGNS LLC',
        'payeeNameLast': None,
        'payeeNameFirst': None,
        'payeeNameSuffixCd': None,
        'payeeNamePrefixCd': None,
        'payeeNameShort': None,
        'payeeNameFull': None,
        'payeeStreetAddr1': '123 MAIN ST',
        'payeeStreetAddr2': None,
        'payeeStreetCity': 'AUSTIN',
        'payeeStreetStateCd': 'TX',
        'payeeStreetCountyCd': None,
        'payeeStreetCountryCd': 'USA',
        'payeeStreetPostalCode': '78702',
        'payeeStreetRegion': None,
        'creditCardIssuer': None,
        'repaymentDt': None,
        'file_origin': 'expend_01',
        'download_date': '20240120',
    }


@pytest.fixture
def sample_texas_filer_record() -> Dict[str, Any]:
    """Sample Texas filer record matching TECFilerName schema."""
    return {
        'recordType': 'FILER',
        'filerIdent': 67890,
        'filerTypeCd': 'CAN',
        'filerName': 'SMITH FOR TEXAS',
        'filerNameFirst': 'JOHN',
        'filerNameLast': 'SMITH',
        'filerNamePrefixCd': None,
        'filerNameSuffixCd': None,
        'filerNameShort': None,
        'filerStreetAddr1': '456 CAMPAIGN AVE',
        'filerStreetAddr2': 'SUITE 100',
        'filerStreetCity': 'DALLAS',
        'filerStreetStateCd': 'TX',
        'filerStreetCountryCd': 'USA',
        'filerStreetPostalCode': '75201',
        'filerStreetRegion': None,
        'filerHoldOfficeCd': 'REP',
        'filerHoldOfficeDistrict': '100',
        'filerHoldOfficePlace': None,
        'filerHoldOfficeDescr': 'State Representative',
        'filerSeekOfficeCd': None,
        'filerSeekOfficeDistrict': None,
        'filerSeekOfficePlace': None,
        'filerSeekOfficeDescr': None,
        'file_origin': 'filers_01',
        'download_date': '20240120',
    }


# =============================================================================
# Sample Record Fixtures - Oklahoma
# =============================================================================

@pytest.fixture
def sample_oklahoma_contribution_record() -> Dict[str, Any]:
    """Sample Oklahoma contribution record."""
    return {
        'Receipt ID': '1001',
        'Org ID': '5001',
        'Committee Name': 'JONES FOR OKLAHOMA',
        'Committee Type': 'Candidate',
        'First Name': 'JANE',
        'Last Name': 'DOE',
        'Receipt Amount': '500.00',
        'Receipt Date': '2024-01-10',
        'Receipt Type': 'Monetary Contribution',
        'Address 1': '789 OAK ST',
        'Address 2': None,
        'City': 'OKLAHOMA CITY',
        'State': 'OK',
        'Zip': '73102',
        'Employer': 'STATE UNIVERSITY',
        'Occupation': 'PROFESSOR',
        'Description': 'Campaign contribution',
        'Filed Date': '2024-01-15',
        'Amended': 'N',
        'file_origin': 'ok_receipts_2024',
        'state': 'oklahoma',
    }


@pytest.fixture
def sample_oklahoma_expenditure_record() -> Dict[str, Any]:
    """Sample Oklahoma expenditure record."""
    return {
        'Expenditure ID': '2001',
        'Org ID': '5001',
        'Committee Name': 'JONES FOR OKLAHOMA',
        'Committee Type': 'Candidate',
        'Expenditure Amount': '250.00',
        'Expenditure Date': '2024-01-12',
        'Expenditure Type': 'Advertising',
        'Purpose': 'Campaign flyers',
        'Payee Name': 'QUICK PRINT SHOP',
        'Address 1': '100 COMMERCE ST',
        'City': 'TULSA',
        'State': 'OK',
        'Zip': '74101',
        'Filed Date': '2024-01-15',
        'Amended': 'N',
        'file_origin': 'ok_expenditures_2024',
        'state': 'oklahoma',
    }


# =============================================================================
# Unified Model Fixtures
# =============================================================================

@pytest.fixture
def sample_unified_transaction_data() -> Dict[str, Any]:
    """Sample data for creating a UnifiedTransaction."""
    return {
        'transaction_id': 'TX-2024-001',
        'state_id': 1,
        'transaction_type': 'contribution',
        'amount': 1000.00,
        'transaction_date': date(2024, 1, 10),
        'description': 'Campaign contribution',
        'source_system': 'TEC',
        'original_record_id': '111222',
    }


@pytest.fixture
def sample_unified_person_data() -> Dict[str, Any]:
    """Sample data for creating a UnifiedPerson."""
    return {
        'first_name': 'JOHN',
        'last_name': 'DOE',
        'full_name': 'JOHN DOE',
        'person_type': 'individual',
        'employer': 'ACME INC',
        'occupation': 'ENGINEER',
    }


@pytest.fixture
def sample_unified_committee_data() -> Dict[str, Any]:
    """Sample data for creating a UnifiedCommittee."""
    return {
        'name': 'SMITH FOR TEXAS',
        'committee_type': 'candidate',
        'filer_id': '67890',
        'state_id': 1,
    }


# =============================================================================
# Field Library Fixtures
# =============================================================================

@pytest.fixture
def field_library():
    """UnifiedFieldLibrary instance for testing field mappings."""
    from app.states.unified_field_library import UnifiedFieldLibrary
    return UnifiedFieldLibrary()


@pytest.fixture
def texas_field_mappings(field_library):
    """Texas-specific field mappings."""
    return field_library.get_state_mappings('texas')


@pytest.fixture
def oklahoma_field_mappings(field_library):
    """Oklahoma-specific field mappings."""
    return field_library.get_state_mappings('oklahoma')


# =============================================================================
# Database Fixtures
# =============================================================================

@pytest.fixture
def mock_db_session():
    """
    Mock database session for testing without actual database connections.

    This fixture provides a MagicMock that can be used to test database
    operations without hitting a real database.
    """
    mock_session = MagicMock()
    mock_session.add = MagicMock()
    mock_session.commit = MagicMock()
    mock_session.rollback = MagicMock()
    mock_session.refresh = MagicMock()
    mock_session.execute = MagicMock()
    mock_session.query = MagicMock()
    return mock_session


@pytest.fixture
def in_memory_db_session():
    """
    In-memory SQLite session for integration testing.

    This fixture creates a real SQLAlchemy session with an in-memory
    SQLite database for testing database operations.
    """
    from sqlmodel import SQLModel, Session, create_engine

    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        yield session


@pytest.fixture
def mock_db_manager():
    """Mock database manager for testing loader functionality."""
    manager = MagicMock()
    manager.get_session = MagicMock(return_value=MagicMock())
    manager.engine = MagicMock()
    return manager


# =============================================================================
# File Operation Fixtures
# =============================================================================

@pytest.fixture
def temp_parquet_file(tmp_path) -> Path:
    """
    Create a temporary parquet file for testing file operations.

    Returns the path to a parquet file containing sample contribution-like data.
    """
    df = pl.DataFrame({
        'filerIdent': ['123', '456', '789'],
        'filerName': ['SMITH FOR TEXAS', 'JONES PAC', 'COMMITTEE FOR CHANGE'],
        'contributionAmount': ['100.00', '200.00', '500.00'],
        'contributionDt': ['20240110', '20240111', '20240112'],
        'contributorNameLast': ['DOE', 'SMITH', 'JOHNSON'],
        'contributorNameFirst': ['JOHN', 'JANE', 'BOB'],
        'contributorPersentTypeCd': ['INDIVIDUAL', 'INDIVIDUAL', 'INDIVIDUAL'],
        'file_origin': ['test_01', 'test_01', 'test_01'],
    })
    path = tmp_path / 'test_contributions.parquet'
    df.write_parquet(path)
    return path


@pytest.fixture
def temp_csv_file(tmp_path) -> Path:
    """Create a temporary CSV file for testing file readers."""
    csv_content = """recordType,filerIdent,filerName,contributionAmount,contributionDt
RCPT,123,SMITH FOR TEXAS,100.00,20240110
RCPT,456,JONES PAC,200.00,20240111
RCPT,789,COMMITTEE FOR CHANGE,500.00,20240112"""
    path = tmp_path / 'test_contributions.csv'
    path.write_text(csv_content)
    return path


@pytest.fixture
def temp_data_directory(tmp_path) -> Path:
    """
    Create a temporary directory structure mimicking the data layout.

    Creates:
    - tmp/texas/contributions/
    - tmp/texas/expenses/
    - tmp/oklahoma/
    """
    texas_contrib = tmp_path / 'texas' / 'contributions'
    texas_contrib.mkdir(parents=True)

    texas_expend = tmp_path / 'texas' / 'expenses'
    texas_expend.mkdir(parents=True)

    oklahoma = tmp_path / 'oklahoma'
    oklahoma.mkdir(parents=True)

    return tmp_path


@pytest.fixture
def sample_parquet_files(temp_data_directory) -> Dict[str, Path]:
    """Create multiple parquet files for testing batch operations."""
    files = {}

    # Texas contributions
    df_contrib = pl.DataFrame({
        'contributionInfoId': [1, 2, 3],
        'contributionAmount': ['100', '200', '300'],
        'file_origin': ['contrib_01', 'contrib_01', 'contrib_01'],
    })
    contrib_path = temp_data_directory / 'texas' / 'contributions' / 'contrib_01.parquet'
    df_contrib.write_parquet(contrib_path)
    files['texas_contributions'] = contrib_path

    # Texas expenses
    df_expend = pl.DataFrame({
        'expendInfoId': [1, 2],
        'expendAmount': ['50', '75'],
        'file_origin': ['expend_01', 'expend_01'],
    })
    expend_path = temp_data_directory / 'texas' / 'expenses' / 'expend_01.parquet'
    df_expend.write_parquet(expend_path)
    files['texas_expenses'] = expend_path

    return files


# =============================================================================
# Validation Test Fixtures
# =============================================================================

@pytest.fixture
def invalid_contribution_missing_name(sample_texas_contribution_record) -> Dict[str, Any]:
    """Sample contribution record with missing required name field."""
    record = sample_texas_contribution_record.copy()
    record['contributorNameLast'] = None
    record['contributorNameFirst'] = None
    return record


@pytest.fixture
def invalid_contribution_missing_date(sample_texas_contribution_record) -> Dict[str, Any]:
    """Sample contribution record with missing date."""
    record = sample_texas_contribution_record.copy()
    record['contributionDt'] = None
    return record


@pytest.fixture
def contribution_with_edge_case_amount(sample_texas_contribution_record) -> Dict[str, Any]:
    """Sample contribution with edge case amount (negative/refund)."""
    record = sample_texas_contribution_record.copy()
    record['contributionAmount'] = '-500.00'
    record['contributionDescr'] = 'Refund'
    record['contributionInfoId'] = 111224
    return record


# =============================================================================
# Loader and Processor Fixtures
# =============================================================================

@pytest.fixture
def mock_unified_processor():
    """Mock unified processor for testing loader workflows."""
    processor = MagicMock()
    processor.process_record = MagicMock(return_value=MagicMock())
    return processor


@pytest.fixture
def loader_config():
    """Default configuration for production loader testing."""
    return {
        'batch_size': 10,
        'commit_frequency': 5,
        'max_retries': 3,
        'skip_errors': False,
    }


# =============================================================================
# Hypothesis Strategy Registration (for property-based testing)
# =============================================================================

def pytest_configure(config):
    """Register custom markers for the test suite."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "database: marks tests requiring database connection"
    )


# =============================================================================
# Cleanup Fixtures
# =============================================================================

@pytest.fixture(autouse=True)
def reset_global_caches():
    """
    Reset any global caches between tests.

    This ensures test isolation by clearing cached data.
    """
    yield
    # Cleanup happens after test completes
    # Add any cache clearing logic here if needed


@pytest.fixture
def isolated_environment(monkeypatch, tmp_path):
    """
    Create an isolated environment for testing.

    Sets up environment variables and paths for isolated testing.
    """
    monkeypatch.setenv('CF_DATA_DIR', str(tmp_path))
    monkeypatch.setenv('CF_ENV', 'test')
    return tmp_path
