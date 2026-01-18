#!/usr/bin/env python3
"""
Configuration presets for the production loader.
"""

from dataclasses import dataclass
from typing import Optional
from pathlib import Path

@dataclass
class LoaderConfig:
    """Configuration for the production loader"""
    batch_size: int = 100
    max_records: Optional[int] = None
    commit_frequency: int = 50  # Commit every N batches
    enable_progress: bool = True
    enable_logging: bool = True
    retry_failed: bool = True
    max_retries: int = 3

class LoaderPresets:
    """Predefined configuration presets"""
    
    @staticmethod
    def development() -> LoaderConfig:
        """Development preset - small batches, limited records"""
        return LoaderConfig(
            batch_size=50,
            max_records=100,
            commit_frequency=5,
            enable_progress=True,
            enable_logging=True,
            retry_failed=True,
            max_retries=3
        )
    
    @staticmethod
    def testing() -> LoaderConfig:
        """Testing preset - medium batches, moderate records"""
        return LoaderConfig(
            batch_size=100,
            max_records=1000,
            commit_frequency=10,
            enable_progress=True,
            enable_logging=True,
            retry_failed=True,
            max_retries=3
        )
    
    @staticmethod
    def production() -> LoaderConfig:
        """Production preset - large batches, all records"""
        return LoaderConfig(
            batch_size=500,
            max_records=None,  # Process all records
            commit_frequency=20,
            enable_progress=True,
            enable_logging=True,
            retry_failed=True,
            max_retries=3
        )
    
    @staticmethod
    def high_performance() -> LoaderConfig:
        """High performance preset - very large batches"""
        return LoaderConfig(
            batch_size=1000,
            max_records=None,  # Process all records
            commit_frequency=50,
            enable_progress=True,
            enable_logging=False,  # Disable logging for performance
            retry_failed=False,  # Disable retries for performance
            max_retries=1
        )
    
    @staticmethod
    def safe() -> LoaderConfig:
        """Safe preset - small batches, frequent commits"""
        return LoaderConfig(
            batch_size=25,
            max_records=None,
            commit_frequency=2,
            enable_progress=True,
            enable_logging=True,
            retry_failed=True,
            max_retries=5
        )

# File configurations
FILE_CONFIGS = {
    "oklahoma_2020": {
        "file_path": Path("tmp/oklahoma/2020_ContributionLoanExtract.csv"),
        "state": "oklahoma",
        "description": "Oklahoma 2020 Contribution/Loan Extract"
    },
    "oklahoma_2021": {
        "file_path": Path("tmp/oklahoma/2021_ContributionLoanExtract.csv"),
        "state": "oklahoma", 
        "description": "Oklahoma 2021 Contribution/Loan Extract"
    },
    "texas_sample": {
        "file_path": Path("tmp/texas/sample_data.csv"),
        "state": "texas",
        "description": "Texas Sample Data"
    },
    "texas_contributions": {
        "file_path": Path("tmp/texas/contribs_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Contributions (Parquet) - 648MB"
    },
    "texas_expenditures": {
        "file_path": Path("tmp/texas/expend_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Expenditures (Parquet) - 152MB"
    },
    "texas_filers": {
        "file_path": Path("tmp/texas/filers_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Filers (Parquet) - 2.8MB"
    },
    "texas_loans": {
        "file_path": Path("tmp/texas/loans_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Loans (Parquet)"
    },
    "texas_cover": {
        "file_path": Path("tmp/texas/cover_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Cover Sheets (Parquet)"
    },
    "texas_pledges": {
        "file_path": Path("tmp/texas/pledges_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Pledges (Parquet)"
    },
    "texas_credits": {
        "file_path": Path("tmp/texas/credits_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Credits (Parquet)"
    },
    "texas_debts": {
        "file_path": Path("tmp/texas/debts_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Debts (Parquet)"
    },
    "texas_travel": {
        "file_path": Path("tmp/texas/travel_20250805w.parquet"),
        "state": "texas",
        "description": "Texas Travel (Parquet)"
    }
}

def get_config(preset_name: str = "testing") -> LoaderConfig:
    """Get configuration by preset name"""
    preset_methods = {
        "development": LoaderPresets.development,
        "testing": LoaderPresets.testing,
        "production": LoaderPresets.production,
        "high_performance": LoaderPresets.high_performance,
        "safe": LoaderPresets.safe
    }
    
    if preset_name not in preset_methods:
        raise ValueError(f"Unknown preset: {preset_name}. Available: {list(preset_methods.keys())}")
    
    return preset_methods[preset_name]()

def get_file_config(file_key: str) -> dict:
    """Get file configuration by key"""
    if file_key not in FILE_CONFIGS:
        raise ValueError(f"Unknown file key: {file_key}. Available: {list(FILE_CONFIGS.keys())}")
    
    return FILE_CONFIGS[file_key] 