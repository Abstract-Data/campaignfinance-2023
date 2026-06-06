"""Stage-2 blocking for candidate pair generation."""

from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import combinations

from sqlalchemy import Column, String, UniqueConstraint
from sqlmodel import Field, SQLModel, delete, select

from app.resolve.models.resolution import SOURCE_ID_MAX_LENGTH
from app.resolve.standardize.staging import ResolutionInput

LOGGER = logging.getLogger(__name__)

ADDRESS_FIELDS = frozenset({"line_1", "line_2", "city", "state", "zip5", "zip4", "zip3"})
VIRTUAL_FIELDS = frozenset({"zip3", "first_initial"})


@dataclass(frozen=True)
class BlockingRule:
    """Named data-driven rule for deriving one blocking key."""

    name: str
    fields: tuple[str, ...]

    def key_for(self, row: ResolutionInput) -> str | None:
        parts: list[str] = []
        for field_name in self.fields:
            if field_name == "zip3":
                raw_zip = (row.zip5 or "").strip()
                if len(raw_zip) < 3:
                    return None
                parts.append(raw_zip[:3].lower())
                continue

            if field_name == "first_initial":
                first_name = (row.first_name or "").strip()
                if not first_name:
                    return None
                parts.append(first_name[0].lower())
                continue

            value = getattr(row, field_name, None)
            if value is None:
                return None
            normalized = str(value).strip()
            if not normalized:
                return None
            parts.append(normalized.lower())
        return "|".join(parts)

    @property
    def is_address_only(self) -> bool:
        return bool(self.fields) and set(self.fields).issubset(ADDRESS_FIELDS)


class CandidatePair(SQLModel, table=True):
    """Per-run blocked pairs consumed by downstream matching stages."""

    __tablename__ = "candidate_pairs"
    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "source_a_type",
            "source_a_id",
            "source_b_type",
            "source_b_id",
            name="uq_candidate_pairs_run_sources",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    source_a_type: str
    source_a_id: str = Field(sa_column=Column(String(SOURCE_ID_MAX_LENGTH), nullable=False))
    source_b_type: str
    source_b_id: str = Field(sa_column=Column(String(SOURCE_ID_MAX_LENGTH), nullable=False))
    rule_name: str


def default_blocking_rules() -> list[BlockingRule]:
    """Return conservative default blocking rules for release-scale runs.

    Person rules require either ZIP3 or a *full* phonetic first name alongside
    the phonetic last name — never phonetic last name alone, and never a single
    first initial (both explode pair counts on common surnames at 1M+ rows: a
    first-initial+last-phonetic key collapses every "J Smith" in the state into
    one block, summing to >100M candidate pairs). Using the full first-name
    phonetic keeps the cross-ZIP same-person signal while staying selective.
    Organization/committee rules require ZIP3 alongside normalized name.
    """
    return [
        BlockingRule(
            name="person_last_phonetic_zip3",
            fields=("last_name_phonetic", "zip3"),
        ),
        BlockingRule(
            name="person_first_last_phonetic",
            fields=("first_name_phonetic", "last_name_phonetic"),
        ),
        BlockingRule(
            name="org_normalized_zip3",
            fields=("normalized_org", "zip3"),
        ),
    ]


def _rules_from_config(config: dict) -> list[BlockingRule]:
    configured = config.get("blocking_rules")
    if not configured:
        rules = default_blocking_rules()
    else:
        rules = [
            BlockingRule(
                name=str(rule["name"]),
                fields=tuple(str(field) for field in rule["fields"]),
            )
            for rule in configured
        ]

    for rule in rules:
        if rule.is_address_only:
            raise ValueError(f"Blocking rule '{rule.name}' cannot use address alone")
    return rules


def _ordered_pair(
    left: ResolutionInput,
    right: ResolutionInput,
    rule_name: str,
) -> CandidatePair:
    left_key = (left.source_type, left.source_id)
    right_key = (right.source_type, right.source_id)
    if right_key < left_key:
        left, right = right, left
    return CandidatePair(
        run_id=left.run_id,
        source_a_type=left.source_type,
        source_a_id=left.source_id,
        source_b_type=right.source_type,
        source_b_id=right.source_id,
        rule_name=rule_name,
    )


def generate_candidate_pairs(
    session,
    run_id: int,
    rules: list[BlockingRule],
    *,
    max_block_size: int,
) -> Iterable[CandidatePair]:
    """Emit unique candidate pairs for one run across all blocking rules."""
    rows = session.exec(select(ResolutionInput).where(ResolutionInput.run_id == run_id)).all()
    emitted: set[tuple[str, str, str, str]] = set()

    for rule in rules:
        blocks: dict[str, list[ResolutionInput]] = defaultdict(list)
        for row in rows:
            key = rule.key_for(row)
            if key is None:
                continue
            blocks[key].append(row)

        for key, block_rows in blocks.items():
            if len(block_rows) > max_block_size:
                LOGGER.warning(
                    "Skipping oversized block rule=%s key=%s size=%s max=%s",
                    rule.name,
                    key,
                    len(block_rows),
                    max_block_size,
                )
                continue

            for left, right in combinations(block_rows, 2):
                pair = _ordered_pair(left, right, rule.name)
                pair_key = (
                    pair.source_a_type,
                    pair.source_a_id,
                    pair.source_b_type,
                    pair.source_b_id,
                )
                if pair_key in emitted:
                    continue
                emitted.add(pair_key)
                yield pair


def _cap_pairs(pairs: list[CandidatePair], max_pairs_per_run: int | None) -> list[CandidatePair]:
    """Return at most *max_pairs_per_run* pairs in deterministic source-id order."""
    if max_pairs_per_run is None or len(pairs) <= max_pairs_per_run:
        return pairs

    LOGGER.warning(
        "Capping candidate pairs from %s to max_pairs_per_run=%s (golden-set tuning guardrail)",
        len(pairs),
        max_pairs_per_run,
    )
    ordered = sorted(
        pairs,
        key=lambda pair: (
            pair.source_a_type,
            pair.source_a_id,
            pair.source_b_type,
            pair.source_b_id,
            pair.rule_name,
        ),
    )
    return ordered[:max_pairs_per_run]


def resolve_blocking_backend(session, config: dict) -> str:
    """Return ``python`` or ``sql`` for stage-2 blocking."""
    configured = config.get("blocking_backend")
    if configured is not None:
        backend = str(configured).strip().lower()
        if backend not in {"python", "sql"}:
            raise ValueError(
                f"blocking_backend must be 'python' or 'sql', got {configured!r}"
            )
        return backend

    bind = session.get_bind()
    dialect = bind.dialect.name if bind is not None else "sqlite"
    return "sql" if dialect == "postgresql" else "python"


def _run_blocking_stage_python(session, run_id: int, config: dict) -> dict:
    max_block_size = int(config.get("max_block_size", 100))
    max_pairs_raw = config.get("max_pairs_per_run")
    max_pairs_per_run = int(max_pairs_raw) if max_pairs_raw is not None else None
    rules = _rules_from_config(config)

    session.exec(delete(CandidatePair).where(CandidatePair.run_id == run_id))
    pairs = list(
        generate_candidate_pairs(
            session,
            run_id=run_id,
            rules=rules,
            max_block_size=max_block_size,
        )
    )
    pairs = _cap_pairs(pairs, max_pairs_per_run)
    session.add_all(pairs)
    session.commit()
    return {"pairs_compared": len(pairs)}


def run_blocking_stage(session, run_id: int, config: dict) -> dict:
    """Run stage 2 and persist candidate pairs for downstream stages."""
    backend = resolve_blocking_backend(session, config)
    if backend == "sql":
        from app.resolve.blocking_sql import run_blocking_stage_sql

        return run_blocking_stage_sql(session, run_id, config)
    return _run_blocking_stage_python(session, run_id, config)
