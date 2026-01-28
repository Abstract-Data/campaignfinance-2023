#!/usr/bin/env python3
"""
Integration tests for the ProductionLoader.

These tests verify the ProductionLoader's core functionality without
requiring a real PostgreSQL database connection. They test the data
classes, helper methods, and state metadata independently.
"""

from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/loaders to path so we can import from it
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts" / "loaders"))


class TestLoaderConfig:
    """Tests for LoaderConfig dataclass."""

    def test_default_values(self):
        """Test LoaderConfig has sensible defaults."""
        from production_loader import LoaderConfig

        config = LoaderConfig()
        assert config.batch_size == 100
        assert config.max_records is None
        assert config.commit_frequency == 50
        assert config.enable_progress is True
        assert config.enable_logging is True
        assert config.retry_failed is True
        assert config.max_retries == 3

    def test_custom_values(self):
        """Test LoaderConfig accepts custom values."""
        from production_loader import LoaderConfig

        config = LoaderConfig(
            batch_size=50,
            max_records=1000,
            commit_frequency=10,
            enable_progress=False,
            enable_logging=False,
        )
        assert config.batch_size == 50
        assert config.max_records == 1000
        assert config.commit_frequency == 10
        assert config.enable_progress is False
        assert config.enable_logging is False


class TestLoaderStats:
    """Tests for LoaderStats dataclass."""

    def test_success_rate_zero_records(self):
        """Test success rate with zero records."""
        from production_loader import LoaderStats

        stats = LoaderStats()
        assert stats.success_rate == 0.0

    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        from production_loader import LoaderStats

        stats = LoaderStats(
            total_records=100,
            successful_records=85,
            failed_records=15,
        )
        assert stats.success_rate == 85.0

    def test_duration_calculation(self):
        """Test duration calculation."""
        from production_loader import LoaderStats

        stats = LoaderStats(start_time=100.0, end_time=150.0)
        assert stats.duration == 50.0

    def test_records_per_second_zero_duration(self):
        """Test records per second with zero duration."""
        from production_loader import LoaderStats

        stats = LoaderStats(start_time=100.0, end_time=100.0)
        assert stats.records_per_second == 0.0

    def test_records_per_second_calculation(self):
        """Test records per second calculation."""
        from production_loader import LoaderStats

        stats = LoaderStats(
            successful_records=100,
            start_time=0.0,
            end_time=10.0,
        )
        assert stats.records_per_second == 10.0


class TestProductionLoaderHelpers:
    """Tests for ProductionLoader helper methods."""

    def test_normalize_name_none(self):
        """Test name normalization with None."""
        from production_loader import ProductionLoader

        result = ProductionLoader._normalize_name(None)
        assert result == ""

    def test_normalize_name_empty(self):
        """Test name normalization with empty string."""
        from production_loader import ProductionLoader

        result = ProductionLoader._normalize_name("")
        assert result == ""

    def test_normalize_name_whitespace(self):
        """Test name normalization strips whitespace."""
        from production_loader import ProductionLoader

        result = ProductionLoader._normalize_name("  John Doe  ")
        assert result == "john doe"

    def test_normalize_name_special_chars(self):
        """Test name normalization removes special characters."""
        from production_loader import ProductionLoader

        result = ProductionLoader._normalize_name("O'Brien, Mary-Jane")
        assert result == "o brien mary jane"

    def test_normalize_name_multiple_spaces(self):
        """Test name normalization collapses multiple spaces."""
        from production_loader import ProductionLoader

        result = ProductionLoader._normalize_name("John    Doe")
        assert result == "john doe"

    def test_address_key_none(self):
        """Test address key with None."""
        from production_loader import ProductionLoader

        result = ProductionLoader._address_key(None)
        assert result == ("", "", "", "")

    def test_address_key_full_address(self):
        """Test address key with full address."""
        from production_loader import ProductionLoader

        @dataclass
        class MockAddress:
            street_1: str = "123 Main St"
            city: str = "Austin"
            state: str = "TX"
            zip_code: str = "78701"

        result = ProductionLoader._address_key(MockAddress())
        assert result == ("123 MAIN ST", "AUSTIN", "TX", "78701")

    def test_address_key_partial_address(self):
        """Test address key with partial address."""
        from production_loader import ProductionLoader

        @dataclass
        class MockAddress:
            street_1: Optional[str] = None
            city: str = "Austin"
            state: str = "TX"
            zip_code: Optional[str] = None

        result = ProductionLoader._address_key(MockAddress())
        assert result == ("", "AUSTIN", "TX", "")


class TestStateMetadata:
    """Tests for state metadata resolution."""

    def test_us_state_metadata_coverage(self):
        """Test all 50 states are in US_STATE_METADATA."""
        from production_loader import US_STATE_METADATA

        assert len(US_STATE_METADATA) == 50

    def test_state_metadata_structure(self):
        """Test state metadata has correct structure."""
        from production_loader import US_STATE_METADATA

        for state_slug, (code, name) in US_STATE_METADATA.items():
            assert isinstance(state_slug, str)
            assert state_slug == state_slug.lower()
            assert len(code) == 2
            assert code == code.upper()
            assert isinstance(name, str)
            assert len(name) > 0

    def test_texas_metadata(self):
        """Test Texas state metadata."""
        from production_loader import US_STATE_METADATA

        assert "texas" in US_STATE_METADATA
        assert US_STATE_METADATA["texas"] == ("TX", "Texas")

    def test_oklahoma_metadata(self):
        """Test Oklahoma state metadata."""
        from production_loader import US_STATE_METADATA

        assert "oklahoma" in US_STATE_METADATA
        assert US_STATE_METADATA["oklahoma"] == ("OK", "Oklahoma")


class TestProductionLoaderInitialization:
    """Tests for ProductionLoader initialization with mocked database."""

    @pytest.fixture
    def mock_db_manager(self):
        """Create a mock database manager."""
        mock = MagicMock()
        mock.get_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock.get_session.return_value.__exit__ = MagicMock(return_value=None)
        return mock

    def test_loader_initializes_caches(self, mock_db_manager):
        """Test loader initializes all required caches."""
        with patch('production_loader.create_postgres_database_manager', return_value=mock_db_manager):
            from production_loader import LoaderConfig, ProductionLoader

            config = LoaderConfig()
            loader = ProductionLoader(config)

            assert hasattr(loader, 'address_cache')
            assert hasattr(loader, 'committee_cache')
            assert hasattr(loader, 'entity_cache')
            assert hasattr(loader, 'campaign_cache')
            assert hasattr(loader, 'person_cache')
            assert hasattr(loader, 'state_cache')
            assert hasattr(loader, 'file_origin_cache')

            # All caches should be empty at initialization
            assert len(loader.address_cache) == 0
            assert len(loader.committee_cache) == 0
            assert len(loader.entity_cache) == 0

    def test_loader_initializes_error_tracking(self, mock_db_manager):
        """Test loader initializes error tracking."""
        with patch('production_loader.create_postgres_database_manager', return_value=mock_db_manager):
            from production_loader import LoaderConfig, ProductionLoader

            config = LoaderConfig()
            loader = ProductionLoader(config)

            assert hasattr(loader, 'errors')
            assert hasattr(loader, 'failed_records')
            assert len(loader.errors) == 0
            assert len(loader.failed_records) == 0


class TestResolveStateMetadata:
    """Tests for _resolve_state_metadata method."""

    @pytest.fixture
    def mock_db_manager(self):
        """Create a mock database manager."""
        mock = MagicMock()
        mock.get_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock.get_session.return_value.__exit__ = MagicMock(return_value=None)
        return mock

    def test_resolve_full_name(self, mock_db_manager):
        """Test resolving state by full name."""
        with patch('production_loader.create_postgres_database_manager', return_value=mock_db_manager):
            from production_loader import LoaderConfig, ProductionLoader

            loader = ProductionLoader(LoaderConfig())
            code, name = loader._resolve_state_metadata("texas")

            assert code == "TX"
            assert name == "Texas"

    def test_resolve_code(self, mock_db_manager):
        """Test resolving state by code."""
        with patch('production_loader.create_postgres_database_manager', return_value=mock_db_manager):
            from production_loader import LoaderConfig, ProductionLoader

            loader = ProductionLoader(LoaderConfig())
            code, name = loader._resolve_state_metadata("TX")

            assert code == "TX"
            assert name == "Texas"

    def test_resolve_case_insensitive(self, mock_db_manager):
        """Test state resolution is case insensitive."""
        with patch('production_loader.create_postgres_database_manager', return_value=mock_db_manager):
            from production_loader import LoaderConfig, ProductionLoader

            loader = ProductionLoader(LoaderConfig())

            # All should resolve to the same result
            assert loader._resolve_state_metadata("TEXAS") == ("TX", "Texas")
            assert loader._resolve_state_metadata("Texas") == ("TX", "Texas")
            assert loader._resolve_state_metadata("texas") == ("TX", "Texas")

    def test_resolve_invalid_state(self, mock_db_manager):
        """Test resolving invalid state raises ValueError."""
        with patch('production_loader.create_postgres_database_manager', return_value=mock_db_manager):
            from production_loader import LoaderConfig, ProductionLoader

            loader = ProductionLoader(LoaderConfig())

            with pytest.raises(ValueError, match="Unsupported state identifier"):
                loader._resolve_state_metadata("invalid_state")


class TestFileNotFound:
    """Tests for file not found handling."""

    @pytest.fixture
    def mock_db_manager(self):
        """Create a mock database manager."""
        mock = MagicMock()
        mock.get_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock.get_session.return_value.__exit__ = MagicMock(return_value=None)
        return mock

    def test_load_file_not_found(self, mock_db_manager):
        """Test loading a file that doesn't exist raises FileNotFoundError."""
        with patch('production_loader.create_postgres_database_manager', return_value=mock_db_manager):
            from production_loader import LoaderConfig, ProductionLoader

            loader = ProductionLoader(LoaderConfig())

            with pytest.raises(FileNotFoundError):
                loader.load_file(Path("/nonexistent/file.csv"), state="texas")


class TestCSVCreation:
    """Helper tests for creating test CSV files."""

    def test_create_sample_csv(self, tmp_path):
        """Test creating a sample CSV file."""
        file_path = tmp_path / "sample.csv"
        headers = ["filerIdent", "filerName", "contributorNameFirst", "contributorNameLast", "contributionAmount"]
        records = [
            {"filerIdent": "12345", "filerName": "Test Committee", "contributorNameFirst": "John",
             "contributorNameLast": "Doe", "contributionAmount": "100.00"},
        ]

        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(records)

        assert file_path.exists()

        # Verify content
        with file_path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["filerIdent"] == "12345"
