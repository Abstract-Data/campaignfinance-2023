"""Stage-2 blocking for candidate pair generation."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from itertools import combinations
import logging

from sqlmodel import Field, SQLModel, delete, select

from app.resolve.standardize.staging import ResolutionInput

LOGGER = logging.getLogger(__name__)

ADDRESS_FIELDS = frozenset({"line_1", "line_2", "city", "state", "zip5", "zip4", "zip3"})


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
                parts.append(raw_zip[:3])
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

    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    source_a_type: str
    source_a_id: str
    source_b_type: str
    source_b_id: str
    rule_name: str


def default_blocking_rules() -> list[BlockingRule]:
    """Return conservative default blocking rules."""
    return [
        BlockingRule(name="person_last_phonetic", fields=("last_name_phonetic",)),
        BlockingRule(name="org_normalized", fields=("normalized_org",)),
        BlockingRule(
            name="person_last_phonetic_zip3",
            fields=("last_name_phonetic", "zip3"),
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
    rows = session.exec(
        select(ResolutionInput).where(ResolutionInput.run_id == run_id)
    ).all()
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


def run_blocking_stage(session, run_id: int, config: dict) -> dict:
    """Run stage 2 and persist candidate pairs for downstream stages."""
    max_block_size = int(config.get("max_block_size", 500))
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
    session.add_all(pairs)
    session.commit()
    return {"pairs_compared": len(pairs)}
