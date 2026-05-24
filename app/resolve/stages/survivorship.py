"""Stage 7: trivial clustering + survivorship + canonical publish.

Phase 1 uses a trivial connected-components path over deterministic merge
edges from Stage 3 (task-1f).  Phase 2 will replace ``cluster_edges`` with
full Splink-powered connected-components; keep the function boundary clean so
Phase 2 can swap it in without touching survivorship or crosswalk logic.

Exports
-------
- ``Edge``                   — pairwise merge edge (source-type, source-id pairs)
- ``Cluster``                — connected group of source records
- ``cluster_edges``          — groups edges into clusters (Phase 1: union-find)
- ``build_golden_record``    — applies survivorship rules to produce CanonicalEntity
- ``run_survivorship_stage`` — Stage 7 entry point (Stage protocol)
"""

from __future__ import annotations

import uuid as _uuid_mod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import delete
from sqlmodel import Session, select

from app.resolve.models.canonical import (
    CanonicalEntity,
    CanonicalNameHistory,
    EntityType,
    NameHistorySubjectType,
    map_unified_to_canonical_entity_type,
)
from app.resolve.models.resolution import (
    ConfidenceBand,
    EntityCrosswalk,
    MatchMethod,
    MergeReview,
    ReviewStatus,
    SourceType,
)
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.standardize.orgs import normalize_org_name
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class Edge:
    """A deterministic merge edge between two source records."""

    source_type_a: str
    source_id_a: str
    source_type_b: str
    source_id_b: str


@dataclass
class Cluster:
    """A connected group of source records that resolve to one canonical entity."""

    cluster_id: str = field(default_factory=lambda: str(_uuid_mod.uuid4()))
    members: list[tuple[str, str]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Union-Find (private)
# ---------------------------------------------------------------------------


class _UnionFind:
    """Path-compressed union-find for connected-components over (type, id) keys."""

    def __init__(self) -> None:
        self._parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(self, x: tuple[str, str]) -> tuple[str, str]:
        if x not in self._parent:
            self._parent[x] = x
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: tuple[str, str], y: tuple[str, str]) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self._parent[ry] = rx

    def add(self, x: tuple[str, str]) -> None:
        if x not in self._parent:
            self._parent[x] = x

    def groups(self) -> list[list[tuple[str, str]]]:
        """Return all connected components as lists of members."""
        roots: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        for node in self._parent:
            roots[self.find(node)].append(node)
        return list(roots.values())


# ---------------------------------------------------------------------------
# Public API — clustering
# ---------------------------------------------------------------------------


def cluster_edges(
    edges: list[Edge],
    all_source_keys: list[tuple[str, str]] | None = None,
) -> list[Cluster]:
    """Group merge edges into clusters using connected-components (union-find).

    Each connected component of merge edges becomes one ``Cluster``.  When
    ``all_source_keys`` is provided, source records not reachable from any
    edge become singleton clusters — ensuring every record ends up in exactly
    one cluster.

    Phase 2 replaces this function with full Splink-edge connected-components;
    the ``Cluster`` / ``Edge`` contract remains stable.

    Parameters
    ----------
    edges:
        Pairwise merge edges produced by Stage 3 (task-1f).
    all_source_keys:
        Full ``(source_type, source_id)`` key list for this run.  When given,
        keys absent from every edge are added as singletons.

    Returns
    -------
    list[Cluster]
        One cluster per connected component; singletons included.
    """
    uf = _UnionFind()

    for edge in edges:
        a = (edge.source_type_a, edge.source_id_a)
        b = (edge.source_type_b, edge.source_id_b)
        uf.add(a)
        uf.add(b)
        uf.union(a, b)

    if all_source_keys:
        for key in all_source_keys:
            uf.add(key)

    return [Cluster(members=sorted(group)) for group in uf.groups()]


# ---------------------------------------------------------------------------
# Survivorship helpers (private)
# ---------------------------------------------------------------------------


def _name_completeness(row: ResolutionInput) -> int:
    """Count populated name parts — higher value means more complete."""
    if row.is_organization:
        return 1 if row.normalized_org else 0
    return sum(
        1
        for part in (row.first_name, row.middle_name, row.last_name, row.suffix)
        if part is not None
    )


def _pick_best_name_row(rows: list[ResolutionInput]) -> ResolutionInput:
    """Return the row with the most complete name; break ties by most-recent created_at."""
    return max(rows, key=lambda r: (_name_completeness(r), r.created_at))


def _pick_best_address_row(rows: list[ResolutionInput]) -> ResolutionInput | None:
    """Return the most recent fully-parsed address row, or None if none exist."""
    parsed = [r for r in rows if r.parse_status == "parsed"]
    if not parsed:
        return None
    return max(parsed, key=lambda r: r.created_at)


def _canonical_name_for(row: ResolutionInput) -> str:
    """Reconstruct a human-readable display name from standardized parts."""
    if row.is_organization:
        return row.normalized_org or row.raw_name or ""
    parts = [row.first_name, row.middle_name, row.last_name, row.suffix]
    name = " ".join(p for p in parts if p)
    return name or row.raw_name or ""


def _normalized_name_for(row: ResolutionInput) -> str:
    return normalize_org_name(_canonical_name_for(row))


# ---------------------------------------------------------------------------
# Public API — survivorship
# ---------------------------------------------------------------------------


def build_golden_record(
    cluster: Cluster,
    resolution_input_rows: list[ResolutionInput],
    state_code: str = "TX",
) -> CanonicalEntity:
    """Apply survivorship rules to a cluster and return an unsaved CanonicalEntity.

    Rules (per spec § "Survivorship rules"):

    - **Name:** most complete (most non-empty parts); ties → most recent by
      ``created_at``.
    - **Address:** most recent fully-parsed address (``parse_status == "parsed"``).
    - **Dates:** ``first_seen_date`` = min, ``last_seen_date`` = max across
      linked rows' ``created_at`` dates.
    - **source_record_count:** cluster size (number of members).

    Parameters
    ----------
    cluster:
        Cluster of source records to resolve.
    resolution_input_rows:
        All ``ResolutionInput`` rows relevant to this cluster.  Rows whose
        ``(source_type, source_id)`` are not in ``cluster.members`` are
        ignored.
    state_code:
        Two-letter state code written onto the canonical row.

    Returns
    -------
    CanonicalEntity
        Unsaved; the caller must ``session.add()`` and ``session.flush()``
        to obtain a database id.
    """
    if not resolution_input_rows:
        raise ValueError("cluster has no resolution_input rows")

    cluster_keys = set(cluster.members)
    cluster_rows = [
        r for r in resolution_input_rows if (r.source_type, r.source_id) in cluster_keys
    ]
    if not cluster_rows:
        cluster_rows = list(resolution_input_rows)

    best_name_row = _pick_best_name_row(cluster_rows)
    canonical_name = _canonical_name_for(best_name_row)
    normalized = _normalized_name_for(best_name_row)

    entity_type_str = best_name_row.entity_type
    try:
        entity_type = map_unified_to_canonical_entity_type(entity_type_str)
    except Exception:
        entity_type = EntityType.organization

    dates = [r.created_at.date() for r in cluster_rows]
    first_seen = min(dates)
    last_seen = max(dates)

    return CanonicalEntity(
        entity_type=entity_type,
        canonical_name=canonical_name,
        normalized_name=normalized,
        state_code=state_code,
        first_seen_date=first_seen,
        last_seen_date=last_seen,
        source_record_count=len(cluster.members),
    )


# ---------------------------------------------------------------------------
# Private: name-history builder
# ---------------------------------------------------------------------------


def _build_name_history_rows(
    canonical_entity_id: int,
    cluster_rows: list[ResolutionInput],
) -> list[CanonicalNameHistory]:
    """Build one ``CanonicalNameHistory`` row per distinct normalized name."""
    name_to_rows: dict[str, list[ResolutionInput]] = defaultdict(list)
    for row in cluster_rows:
        norm = normalize_org_name(_canonical_name_for(row))
        name_to_rows[norm].append(row)

    history_rows: list[CanonicalNameHistory] = []
    for norm_name, rows in name_to_rows.items():
        dates = [r.created_at.date() for r in rows]
        display_name = _canonical_name_for(rows[0])
        history_rows.append(
            CanonicalNameHistory(
                subject_type=NameHistorySubjectType.entity,
                subject_id=canonical_entity_id,
                name=display_name,
                normalized_name=norm_name,
                first_seen_date=min(dates),
                last_seen_date=max(dates),
                occurrence_count=len(rows),
            )
        )
    return history_rows


# ---------------------------------------------------------------------------
# Private: merge-edge loading + staging helpers
# ---------------------------------------------------------------------------


def _edge_key(edge: Edge) -> frozenset[tuple[str, str]]:
    """Undirected key for deduplicating pairwise merge edges."""
    return frozenset(
        {
            (edge.source_type_a, edge.source_id_a),
            (edge.source_type_b, edge.source_id_b),
        }
    )


def _source_type_str(value: object) -> str:
    return value.value if hasattr(value, "value") else str(value)


def load_cluster_edges(session: Session, run_id: int) -> list[Edge]:
    """Load merge edges for clustering from ``merge_edges`` and approved reviews.

    Phase 1 fast-path writes deterministic edges to ``merge_edges``; Phase 2
    appends probabilistic and approved-review edges there as well.  Approved
    ``merge_review`` rows are included when present so human decisions merge
    even before a classify stage re-emits them as ``merge_edges``.
    """
    seen: set[frozenset[tuple[str, str]]] = set()
    edges: list[Edge] = []

    def add(edge: Edge) -> None:
        key = _edge_key(edge)
        if key in seen:
            return
        seen.add(key)
        edges.append(edge)

    for row in session.exec(select(MergeEdge).where(MergeEdge.run_id == run_id)).all():
        add(
            Edge(
                source_type_a=row.source_a_type,
                source_id_a=row.source_a_id,
                source_type_b=row.source_b_type,
                source_id_b=row.source_b_id,
            )
        )

    for review in session.exec(
        select(MergeReview).where(MergeReview.status == ReviewStatus.approved)
    ).all():
        add(
            Edge(
                source_type_a=_source_type_str(review.source_a_type),
                source_id_a=review.source_a_id,
                source_type_b=_source_type_str(review.source_b_type),
                source_id_b=review.source_b_id,
            )
        )

    return edges


def _clear_live_canonical_snapshot(session: Session) -> None:
    """Remove the prior live canonical snapshot before publishing this run.

    Each successful survivorship stage replaces the full live canonical layer
    so reruns do not accumulate duplicate golden records.  Historical per-run
    mappings remain in ``entity_crosswalk`` keyed by ``run_id``.
    """
    session.exec(delete(CanonicalNameHistory))
    session.exec(delete(CanonicalEntity))
    session.flush()


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------


def run_survivorship_stage(
    session: Session,
    run_id: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Stage 7 entry point: cluster → survivorship → canonical publish.

    Reads this run's merge edges from ``merge_edges`` (plus approved
    ``merge_review`` rows when present), builds clusters (including singletons
    for all unlinked source records), applies survivorship rules, clears the
    prior live canonical snapshot, and writes:

    - ``canonical_entity`` rows (one per cluster),
    - ``canonical_name_history`` rows (one per distinct normalized name per
      cluster),
    - ``entity_crosswalk`` rows (one per source record in each cluster).

    Parameters
    ----------
    session:
        Active SQLModel ``Session``.
    run_id:
        The ``MatchRun.id`` for this pipeline execution.
    config:
        Run configuration dict.  Expected keys:

        - ``state_code`` — two-letter state code (default ``"TX"``).

    Returns
    -------
    dict
        ``{"canonical_out": <n>}`` where *n* is the number of canonical
        entity rows written for this run (live table row count after publish).
    """
    state_code: str = config.get("state_code", "TX")

    input_rows: list[ResolutionInput] = list(
        session.exec(select(ResolutionInput).where(ResolutionInput.run_id == run_id)).all()
    )

    all_source_keys: list[tuple[str, str]] = [(r.source_type, r.source_id) for r in input_rows]
    edges = load_cluster_edges(session, run_id)
    clusters = cluster_edges(edges, all_source_keys=all_source_keys)

    row_by_key: dict[tuple[str, str], ResolutionInput] = {
        (r.source_type, r.source_id): r for r in input_rows
    }

    _clear_live_canonical_snapshot(session)
    session.exec(delete(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id))

    canonical_count = 0
    for cluster in clusters:
        cluster_rows = [row_by_key[key] for key in cluster.members if key in row_by_key]
        if not cluster_rows:
            continue

        entity = build_golden_record(cluster, cluster_rows, state_code=state_code)
        entity.last_run_id = run_id
        session.add(entity)
        session.flush()

        assert entity.id is not None, "flush must populate canonical_entity.id"

        for h in _build_name_history_rows(entity.id, cluster_rows):
            session.add(h)

        for member_type, member_id in cluster.members:
            try:
                source_type = SourceType(member_type)
            except ValueError:
                source_type = SourceType.unified_entity

            session.add(
                EntityCrosswalk(
                    source_type=source_type,
                    source_id=member_id,
                    canonical_entity_id=entity.id,
                    match_method=MatchMethod.deterministic_rule,
                    match_score=None,
                    confidence_band=ConfidenceBand.auto,
                    run_id=run_id,
                )
            )

        canonical_count += 1

    session.commit()
    return {"canonical_out": canonical_count}
