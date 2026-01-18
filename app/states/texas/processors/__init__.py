"""
Texas Parquet Processors

Processors for loading and transforming Texas Ethics Commission
parquet files into unified data models with relationships.
"""

from .parquet_loader import TexasParquetProcessor

__all__ = ['TexasParquetProcessor']