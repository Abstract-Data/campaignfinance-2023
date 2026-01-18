"""Ingestion utilities for unified campaign finance pipeline."""

from .file_reader import (
    FieldSpec,
    SchemaDefinition,
    GenericFileReader,
    SchemaValidationError,
    HeaderValidationError,
    RecordValidationError,
    build_schema_for_states,
    build_unified_schema,
)

__all__ = [
    "FieldSpec",
    "SchemaDefinition",
    "GenericFileReader",
    "SchemaValidationError",
    "HeaderValidationError",
    "RecordValidationError",
    "build_schema_for_states",
    "build_unified_schema",
]
