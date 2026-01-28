"""Generic schema-driven file reader for campaign finance data.

This module provides reusable facilities to read flat files (CSV, TXT, Parquet)
from multiple states or agencies, normalise header names, validate records
against a schema derived from the unified field library, and emit dictionaries
that downstream processors can consume without having to worry about the
original column naming conventions.

The reader is designed to be state-agnostic. It can stitch together schemas for
multiple states (e.g. Texas, Oklahoma, FEC) and will happily ingest files that
mix different header variants as long as those variants are registered in the
field library.
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Sequence, Set, Tuple

import polars as pl

from app.core.unified_field_library import FieldDefinition, FieldType, StateFieldMapping, field_library

logger = logging.getLogger(__name__)


class SchemaValidationError(RuntimeError):
    """Base class for schema/validation errors raised by the reader."""


class HeaderValidationError(SchemaValidationError):
    """Raised when required headers are missing or ambiguous."""


class RecordValidationError(SchemaValidationError):
    """Raised when a record violates field-level validation rules."""


_normalise_key_pattern = re.compile(r"[^a-z0-9]")


def _normalise_key(value: Optional[str]) -> str:
    """Normalise a header/field name to snake_case for comparison."""
    if value is None:
        return ""
    lowered = value.strip().lower()
    lowered = _normalise_key_pattern.sub("_", lowered)
    lowered = re.sub(r"_+", "_", lowered)
    return lowered.strip("_")


def _parse_date(value: Any) -> Optional[dt.date]:
    if value in (None, ""):
        return None
    if isinstance(value, dt.date):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, (int, float)):
        # Interpret as Excel-style ordinal or timestamp? We leave untouched.
        return None
    text = str(value).strip()
    if not text:
        return None
    # Try common date formats
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d", "%d-%b-%Y", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return None
    text = re.sub(r"[^0-9+\-\.]+", "", text)
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"true", "t", "yes", "y", "1"}:
        return True
    if text in {"false", "f", "no", "n", "0"}:
        return False
    return None


@dataclass(slots=True)
class FieldSpec:
    """Specification for a field in a schema."""

    name: str
    aliases: Tuple[str, ...] = ()
    required: bool = False
    field_type: Optional[FieldType] = None
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    converter: Optional[Callable[[Any], Any]] = None
    normalizer: Optional[Callable[[Any], Any]] = None

    _alias_lookup: Set[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        alias_values = {self.name, *self.aliases}
        self._alias_lookup = {_normalise_key(alias) for alias in alias_values if alias}

    def matches(self, header: str) -> bool:
        return _normalise_key(header) in self._alias_lookup

    def convert(self, value: Any) -> Any:
        if value == "":
            value = None
        if self.converter:
            value = self.converter(value)
        else:
            value = self._auto_convert(value)
        if self.normalizer:
            value = self.normalizer(value)
        self._validate(value)
        return value

    def _auto_convert(self, value: Any) -> Any:
        if self.field_type is None or value is None:
            return value
        if self.field_type in {FieldType.STRING, FieldType.CODE, FieldType.IDENTIFIER}:
            if value is None:
                return None
            return str(value).strip()
        if self.field_type in {FieldType.DECIMAL, FieldType.CURRENCY, FieldType.PERCENTAGE}:
            return _parse_decimal(value)
        if self.field_type in {FieldType.INTEGER}:
            if value in (None, ""):
                return None
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        if self.field_type in {FieldType.DATE, FieldType.DATETIME}:
            return _parse_date(value)
        if self.field_type is FieldType.BOOLEAN:
            return _parse_bool(value)
        return value

    def _validate(self, value: Any) -> None:
        if value in (None, ""):
            if self.required:
                raise RecordValidationError(f"Required field '{self.name}' is missing")
            return

        rules = self.validation_rules or {}
        max_length = rules.get("max_length")
        if max_length and isinstance(value, str) and len(value) > max_length:
            raise RecordValidationError(
                f"Field '{self.name}' exceeds max length {max_length} (got {len(value)})"
            )

        enum_values = rules.get("enum_values")
        if enum_values and value not in enum_values:
            raise RecordValidationError(
                f"Field '{self.name}' expected one of {enum_values}, got '{value}'"
            )

        min_value = rules.get("min_value")
        if min_value is not None and isinstance(value, (int, float, Decimal)):
            if value < min_value:
                raise RecordValidationError(
                    f"Field '{self.name}' must be >= {min_value}, got {value}"
                )


@dataclass(slots=True)
class SchemaDefinition:
    """Schema definition describing expected fields and validation rules."""

    name: str
    fields: Dict[str, FieldSpec]
    allow_extra_fields: bool = True
    record_validators: Tuple[Callable[[Dict[str, Any]], None], ...] = ()

    _alias_map: Dict[str, str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        alias_map: Dict[str, str] = {}
        for canonical, spec in self.fields.items():
            for alias in spec._alias_lookup:
                alias_map.setdefault(alias, canonical)
        self._alias_map = alias_map

    @property
    def required_fields(self) -> Set[str]:
        return {name for name, spec in self.fields.items() if spec.required}

    def resolve_header(self, header: str) -> Optional[str]:
        return self._alias_map.get(_normalise_key(header))

    def map_headers(self, headers: Sequence[str]) -> Dict[str, str]:
        if headers is None:
            headers = []
        mapping: Dict[str, str] = {}
        matched: Set[str] = set()
        for header in headers:
            canonical = self.resolve_header(header)
            if canonical:
                mapping[header] = canonical
                matched.add(canonical)
        missing = self.required_fields - matched
        if missing:
            raise HeaderValidationError(
                f"Missing required fields: {sorted(missing)}"
            )
        return mapping

    def validate_record(self, record: Dict[str, Any]) -> None:
        for validator in self.record_validators:
            validator(record)


def build_schema_for_states(states: Sequence[str]) -> SchemaDefinition:
    """Build a schema that merges unified field definitions for given states."""
    fields: Dict[str, FieldSpec] = {}
    for state in states:
        mappings: List[StateFieldMapping] = field_library.get_state_mappings(state)
        for mapping in mappings:
            unified_name = mapping.unified_field
            definition = field_library.get_unified_field(unified_name)
            spec = fields.get(unified_name)
            aliases = set(spec.aliases) if spec else set()
            aliases.add(mapping.state_field)
            if not definition:
                if spec is None:
                    spec = FieldSpec(name=unified_name, aliases=tuple(aliases))
                else:
                    spec = FieldSpec(
                        name=spec.name,
                        aliases=tuple(set(spec.aliases) | aliases),
                        required=spec.required,
                        field_type=spec.field_type,
                        validation_rules=spec.validation_rules,
                        converter=spec.converter,
                        normalizer=spec.normalizer,
                    )
                fields[unified_name] = spec
                continue
            base_rules = dict(definition.validation_rules or {})
            base_rules.pop("enum_values", None)
            if spec is None:
                spec = FieldSpec(
                    name=definition.name,
                    aliases=tuple(aliases),
                    required=bool(base_rules.get("required")),
                    field_type=definition.field_type,
                    validation_rules=base_rules,
                )
            else:
                updated_rules = spec.validation_rules or base_rules
                updated_rules = dict(updated_rules)
                updated_rules.pop("enum_values", None)
                spec = FieldSpec(
                    name=spec.name,
                    aliases=tuple(set(spec.aliases) | aliases),
                    required=spec.required or base_rules.get("required", False),
                    field_type=spec.field_type or definition.field_type,
                    validation_rules=updated_rules,
                    converter=spec.converter,
                    normalizer=spec.normalizer,
                )
            fields[unified_name] = spec
    # Always allow metadata fields even if not in mapping
    for meta_field in ("file_origin", "download_date"):
        if meta_field not in fields:
            fields[meta_field] = FieldSpec(name=meta_field, aliases=(meta_field,), required=False)
    return SchemaDefinition(
        name="_".join([_normalise_key(state) or "generic" for state in states]) or "generic",
        fields=fields,
    )


def build_unified_schema() -> SchemaDefinition:
    """Build a schema covering all known states in the field library."""
    return build_schema_for_states(list(field_library.state_mappings.keys()))


class GenericFileReader:
    """Schema-aware file reader capable of handling multiple input formats."""

    def __init__(
        self,
        schema: Optional[SchemaDefinition] = None,
        *,
        add_metadata: bool = True,
        strict: bool = True,
    ) -> None:
        self.schema = schema or build_unified_schema()
        self.add_metadata = add_metadata
        self.strict = strict

    def read_records(
        self,
        file_path: Path | str,
        *,
        file_type: Optional[str] = None,
        encoding: str = "utf-8",
    ) -> Generator[Dict[str, Any], None, None]:
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        ext = (file_type or path.suffix).lower()
        if ext in {".csv", ".txt"}:
            yield from self._read_csv(path, encoding=encoding)
        elif ext == ".parquet":
            yield from self._read_parquet(path)
        else:
            raise ValueError(f"Unsupported file type: {ext}")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_csv(self, path: Path, *, encoding: str) -> Generator[Dict[str, Any], None, None]:
        try:
            yield from self._read_csv_with_encoding(path, encoding)
        except UnicodeDecodeError:
            fallback = "ISO-8859-1"
            logger.warning("Falling back to %s encoding for %s", fallback, path.name)
            yield from self._read_csv_with_encoding(path, fallback)

    def _read_csv_with_encoding(self, path: Path, encoding: str) -> Generator[Dict[str, Any], None, None]:
        with path.open("r", encoding=encoding, newline="") as fh:
            reader = csv.DictReader(fh)
            mapping = self.schema.map_headers(reader.fieldnames or [])
            for index, raw in enumerate(reader, start=1):
                try:
                    yield self._normalize_record(raw, mapping, path)
                except RecordValidationError as exc:
                    message = f"Record {index} failed validation in {path.name}: {exc}"
                    if self.strict:
                        raise RecordValidationError(message) from exc
                    logger.warning(message)
                    continue

    def _read_parquet(self, path: Path) -> Generator[Dict[str, Any], None, None]:
        try:
            lazy = pl.scan_parquet(path)
            df = lazy.collect()
        except Exception as exc:
            logger.warning("Falling back to eager parquet read for %s: %s", path, exc)
            df = pl.read_parquet(path)
        mapping = self.schema.map_headers(df.columns)
        for index, row in enumerate(df.iter_rows(named=True), start=1):
            try:
                yield self._normalize_record(row, mapping, path)
            except RecordValidationError as exc:
                message = f"Record {index} failed validation in {path.name}: {exc}"
                if self.strict:
                    raise RecordValidationError(message) from exc
                logger.warning(message)
                continue

    def _normalize_record(
        self,
        raw_record: Dict[str, Any],
        header_mapping: Dict[str, str],
        path: Path,
    ) -> Dict[str, Any]:
        record: Dict[str, Any] = {}
        for key, value in raw_record.items():
            canonical = header_mapping.get(key)
            if canonical:
                field_spec = self.schema.fields.get(canonical)
                if field_spec:
                    record[canonical] = field_spec.convert(value)
                else:
                    record[canonical] = value
            elif self.schema.allow_extra_fields:
                record[_normalise_key(key)] = value
        # ensure required fields present even if missing in row
        missing_required = [name for name, spec in self.schema.fields.items() if spec.required and name not in record]
        if missing_required:
            raise RecordValidationError(
                f"Missing required fields {missing_required} in record for file {path.name}"
            )
        if self.add_metadata:
            record.setdefault("file_origin", path.stem)
            record.setdefault("download_date", dt.datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d"))
        self.schema.validate_record(record)
        return record


__all__ = [
    "GenericFileReader",
    "FieldSpec",
    "SchemaDefinition",
    "SchemaValidationError",
    "HeaderValidationError",
    "RecordValidationError",
    "build_schema_for_states",
    "build_unified_schema",
]
