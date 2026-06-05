"""Stage 3: deterministic fast-path matching.

Reads ``resolution_input`` for a run, emits certain merge edges and
``match_decision`` audit rows, and writes edges to the per-run ``merge_edges``
staging table for downstream clustering (task-1g).

Task: 1f | Branch: resolve/phase-1/task-1f-fastpath
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Column, String
from sqlmodel import Field, Session, SQLModel, delete, select

from app.resolve.models.resolution import (
    DecisionBand,
    DecisionOutcome,
    MatchDecision,
    MatchMethod,
    SourceType,
)
from app.resolve.standardize.staging import ResolutionInput


class MergeEdge(SQLModel, table=True):
    """Staging table of merge edges consumed by survivorship/clustering."""

    __tablename__ = "merge_edges"

    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    source_a_type: str = Field(sa_column=Column(String(64), nullable=False))
    source_a_id: str = Field(sa_column=Column(String(128), nullable=False))
    source_b_type: str = Field(sa_column=Column(String(64), nullable=False))
    source_b_id: str = Field(sa_column=Column(String(128), nullable=False))
    edge_source: str = Field(
        default="deterministic",
        sa_column=Column(String(32), nullable=False),
    )


@dataclass(frozen=True)
class _RecordRef:
    source_type: str
    source_id: str


@dataclass(frozen=True)
class _MergeCandidate:
    left: _RecordRef
    right: _RecordRef
    method: MatchMethod
    rule: str


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().lower().split())


def _address_key(row: ResolutionInput) -> tuple[str, ...] | None:
    if row.parse_status != "parsed":
        return None
    parts = (
        _normalize_text(row.line_1),
        _normalize_text(row.line_2),
        _normalize_text(row.city),
        _normalize_text(row.state),
        _normalize_text(row.zip5),
        _normalize_text(row.zip4),
    )
    if not parts[0] and not parts[2]:
        return None
    return parts


def _person_name_key(row: ResolutionInput) -> tuple[str, ...] | None:
    if row.entity_type != "person" or row.is_organization:
        return None
    parts = (
        _normalize_text(row.first_name),
        _normalize_text(row.middle_name),
        _normalize_text(row.last_name),
        _normalize_text(row.suffix),
    )
    if not parts[0] and not parts[2]:
        return None
    return parts


def _canonical_pair(
    left: _RecordRef,
    right: _RecordRef,
) -> tuple[_RecordRef, _RecordRef]:
    left_key = (left.source_type, left.source_id)
    right_key = (right.source_type, right.source_id)
    if left_key <= right_key:
        return left, right
    return right, left


def _record_ref(row: ResolutionInput) -> _RecordRef:
    return _RecordRef(source_type=row.source_type, source_id=row.source_id)


def _group_records(
    rows: list[ResolutionInput],
    key_fn,
) -> dict[Any, list[_RecordRef]]:
    groups: dict[Any, list[_RecordRef]] = defaultdict(list)
    for row in rows:
        key = key_fn(row)
        if key is None:
            continue
        groups[key].append(_record_ref(row))
    return groups


def _star_edges(
    refs: list[_RecordRef],
    *,
    method: MatchMethod,
    rule: str,
) -> list[_MergeCandidate]:
    if len(refs) < 2:
        return []
    ordered = sorted(refs, key=lambda ref: (ref.source_type, ref.source_id))
    anchor = ordered[0]
    return [
        _MergeCandidate(left=anchor, right=ref, method=method, rule=rule) for ref in ordered[1:]
    ]


def _collect_filer_id_candidates(rows: list[ResolutionInput]) -> list[_MergeCandidate]:
    committees = [
        row
        for row in rows
        if row.source_type == "unified_committee" and row.entity_type == "committee"
    ]
    groups = _group_records(committees, lambda row: row.source_id)
    candidates: list[_MergeCandidate] = []
    for refs in groups.values():
        candidates.extend(
            _star_edges(
                refs,
                method=MatchMethod.exact,
                rule="identical_filer_id",
            )
        )
    return candidates


def _collect_name_address_candidates(
    rows: list[ResolutionInput],
) -> list[_MergeCandidate]:
    persons = [row for row in rows if row.entity_type == "person"]
    groups: dict[tuple[Any, ...], list[_RecordRef]] = defaultdict(list)
    for row in persons:
        name_key = _person_name_key(row)
        address_key = _address_key(row)
        if name_key is None or address_key is None:
            continue
        groups[(name_key, address_key)].append(_record_ref(row))

    candidates: list[_MergeCandidate] = []
    for refs in groups.values():
        candidates.extend(
            _star_edges(
                refs,
                method=MatchMethod.exact,
                rule="identical_name_and_address",
            )
        )
    return candidates


def _collect_entity_to_source_candidates(
    rows: list[ResolutionInput],
) -> list[_MergeCandidate]:
    """Merge a ``unified_entity`` with the source record it 1:1-links to.

    ``unified_entities.person_id`` / ``committee_id`` are unique FKs: an entity of
    type 'person' IS the same real-world entity as the ``unified_person`` it points
    to (same for committees).  Blocking cannot reliably pair them (the entity's name
    is the full normalized name while the person's is first/last, so they often land
    in different blocks), so this deterministic edge guarantees co-clustering — the
    fix for near-zero cross-source dedup.
    """
    persons_by_id = {
        row.source_id: row for row in rows if row.source_type == "unified_person"
    }
    committees_by_filer = {
        row.source_id: row for row in rows if row.source_type == "unified_committee"
    }
    candidates: list[_MergeCandidate] = []
    for row in rows:
        if row.source_type != "unified_entity":
            continue
        target: ResolutionInput | None = None
        rule = ""
        if row.linked_person_id is not None:
            target = persons_by_id.get(str(row.linked_person_id))
            rule = "unified_entity_to_person_link"
        elif row.linked_committee_id is not None:
            target = committees_by_filer.get(str(row.linked_committee_id))
            rule = "unified_entity_to_committee_link"
        if target is not None:
            candidates.append(
                _MergeCandidate(
                    left=_record_ref(row),
                    right=_record_ref(target),
                    method=MatchMethod.exact,
                    rule=rule,
                )
            )
    return candidates


def _dedupe_candidates(candidates: list[_MergeCandidate]) -> list[_MergeCandidate]:
    seen_pairs: set[tuple[str, str, str, str]] = set()
    deduped: list[_MergeCandidate] = []
    for candidate in candidates:
        left, right = _canonical_pair(candidate.left, candidate.right)
        pair = (left.source_type, left.source_id, right.source_type, right.source_id)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        deduped.append(candidate)
    return deduped


def _collect_merge_candidates(rows: list[ResolutionInput]) -> list[_MergeCandidate]:
    ordered_rules = (
        _collect_entity_to_source_candidates,  # deterministic 1:1 FK link — runs first
        _collect_filer_id_candidates,
        _collect_name_address_candidates,
    )
    candidates: list[_MergeCandidate] = []
    for rule_fn in ordered_rules:
        candidates.extend(rule_fn(rows))
    return _dedupe_candidates(candidates)


def _to_source_type(source_type: str) -> SourceType:
    return SourceType(source_type)


def _write_outputs(
    session: Session,
    run_id: int,
    candidates: list[_MergeCandidate],
) -> int:
    session.exec(delete(MergeEdge).where(MergeEdge.run_id == run_id))
    session.exec(delete(MatchDecision).where(MatchDecision.run_id == run_id))

    edges: list[MergeEdge] = []
    decisions: list[MatchDecision] = []
    for candidate in candidates:
        left, right = _canonical_pair(candidate.left, candidate.right)
        edges.append(
            MergeEdge(
                run_id=run_id,
                source_a_type=left.source_type,
                source_a_id=left.source_id,
                source_b_type=right.source_type,
                source_b_id=right.source_id,
                edge_source="deterministic",
            )
        )
        decisions.append(
            MatchDecision(
                run_id=run_id,
                source_a_type=_to_source_type(left.source_type),
                source_a_id=left.source_id,
                source_b_type=_to_source_type(right.source_type),
                source_b_id=right.source_id,
                score=None,
                method=candidate.method,
                band=DecisionBand.auto,
                outcome=DecisionOutcome.merged,
                explanation_json=json.dumps({"rule": candidate.rule}, sort_keys=True),
            )
        )

    session.add_all(edges)
    session.add_all(decisions)
    session.commit()
    return len(candidates)


def run_fastpath_stage(session: Session, run_id: int, config: dict) -> dict:
    """Run stage 3 deterministic fast-path for one match run."""
    _ = config
    rows = session.exec(
        select(ResolutionInput)
        .where(ResolutionInput.run_id == run_id)
        .order_by(
            ResolutionInput.source_type,
            ResolutionInput.source_id,
            ResolutionInput.id,
        )
    ).all()
    candidates = _collect_merge_candidates(rows)
    auto_merges = _write_outputs(session, run_id, candidates)
    return {"auto_merges": auto_merges}
