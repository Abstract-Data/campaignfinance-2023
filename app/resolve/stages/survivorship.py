"""Stage 7: clustering + survivorship + canonical publish.

Phase 1 uses a trivial connected-components path over deterministic merge
edges from Stage 3 (task-1f).  Phase 2 (task-2d) reads the ``clusters``
staging table produced by Stage 6 (task-2c) and adds field-level provenance.

When ``ClusterAssignment`` rows exist for the current run, Stage 7 uses them
directly; otherwise it falls back to Phase 1 trivial clustering so that Phase 1
integration tests continue to pass unchanged.

Held clusters (``held_for_review=True``) are not auto-published as merged
canonical entities — each member is treated as an individual singleton instead.

Exports
-------
- ``Edge``                   — pairwise merge edge (source-type, source-id pairs)
- ``Cluster``                — connected group of source records
- ``cluster_edges``          — groups edges into clusters (Phase 1: union-find)
- ``build_golden_record``    — applies survivorship rules to produce CanonicalEntity
- ``run_survivorship_stage`` — Stage 7 entry point (Stage protocol)
"""

from __future__ import annotations

import json
import uuid as _uuid_mod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sqlalchemy import delete
from sqlmodel import Session, select

from app.core.constants import DEFAULT_STATE
from app.resolve.models.canonical import (
    CanonicalCampaign,
    CanonicalEntity,
    CanonicalNameHistory,
    EntityType,
    NameHistorySubjectType,
    map_unified_to_canonical_entity_type,
)
from app.resolve.models.resolution import (
    ConfidenceBand,
    EntityCrosswalk,
    MatchDecision,
    MatchMethod,
    MergeReview,
    ReviewStatus,
    SourceType,
)
from app.resolve.stages.cluster import ClusterAssignment
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


def _row_activity_date(row: ResolutionInput) -> date:
    """Return the best single date for a row: last_activity_date, first_activity_date, or created_at.date()."""
    if row.last_activity_date is not None:
        return row.last_activity_date
    if row.first_activity_date is not None:
        return row.first_activity_date
    return row.created_at.date()


def _pick_employer(rows: list[ResolutionInput]) -> str | None:
    """Return the employer from the most-recent row (by activity/created date) that has a non-null, non-blank employer.

    Returns None when no row in the cluster carries an employer.
    """
    employer_rows = [r for r in rows if r.employer and r.employer.strip()]
    if not employer_rows:
        return None
    # Pick the most-recent by last_activity_date → first_activity_date → created_at.
    best = max(employer_rows, key=_row_activity_date)
    return best.employer


def _build_employer_history(rows: list[ResolutionInput]) -> list[dict[str, str]]:
    """Aggregate per-cluster employer history as a list of dicts.

    Returns one entry per distinct employer value (after stripping whitespace),
    each with:
        value      — the employer string
        first_seen — ISO date string of the earliest date any row with that employer has
        last_seen  — ISO date string of the latest date any row with that employer has

    Dates use last_activity_date → first_activity_date → created_at fallback.
    The list is ordered by (first_seen, value) for determinism.
    Returns an empty list when no row carries an employer.
    """
    employer_rows = [r for r in rows if r.employer and r.employer.strip()]
    if not employer_rows:
        return []

    by_employer: dict[str, list[date]] = defaultdict(list)
    for row in employer_rows:
        by_employer[row.employer.strip()].append(_row_activity_date(row))

    history: list[dict[str, str]] = []
    for value, dates in by_employer.items():
        history.append(
            {
                "value": value,
                "first_seen": min(dates).isoformat(),
                "last_seen": max(dates).isoformat(),
            }
        )

    history.sort(key=lambda e: (e["first_seen"], e["value"]))
    return history


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
    state_code: str,
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

    # Prefer real filing/activity dates (min/max across the cluster's source
    # records); fall back to ETL ``created_at`` only when no activity dates exist.
    firsts = [r.first_activity_date for r in cluster_rows if r.first_activity_date is not None]
    lasts = [r.last_activity_date for r in cluster_rows if r.last_activity_date is not None]
    if firsts or lasts:
        first_seen = min(firsts) if firsts else min(lasts)
        last_seen = max(lasts) if lasts else max(firsts)
    else:
        created_dates = [r.created_at.date() for r in cluster_rows]
        first_seen = min(created_dates)
        last_seen = max(created_dates)

    first_seen_row = min(cluster_rows, key=lambda r: r.created_at)
    last_seen_row = max(cluster_rows, key=lambda r: r.created_at)

    provenance: dict[str, dict[str, str]] = {
        "canonical_name": {
            "source_type": best_name_row.source_type,
            "source_id": best_name_row.source_id,
        },
        "first_seen_date": {
            "source_type": first_seen_row.source_type,
            "source_id": first_seen_row.source_id,
        },
        "last_seen_date": {
            "source_type": last_seen_row.source_type,
            "source_id": last_seen_row.source_id,
        },
    }

    best_address_row = _pick_best_address_row(cluster_rows)
    if best_address_row is not None:
        provenance["address"] = {
            "source_type": best_address_row.source_type,
            "source_id": best_address_row.source_id,
        }

    # Employer survivorship — scalar (most-recent) + history in provenance.
    employer = _pick_employer(cluster_rows)
    employer_history = _build_employer_history(cluster_rows)
    if employer_history:
        provenance["employer_history"] = employer_history

    return CanonicalEntity(
        entity_type=entity_type,
        canonical_name=canonical_name,
        normalized_name=normalized,
        state_code=state_code,
        first_seen_date=first_seen,
        last_seen_date=last_seen,
        source_record_count=len(cluster.members),
        employer=employer,
        provenance_json=json.dumps(provenance),
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
        # Real activity window for *this name* — the period the entity actually
        # filed under it — so name-as-of-date lookups are accurate.  Fall back to
        # ETL created_at only when a name's rows carry no activity dates.
        firsts = [r.first_activity_date for r in rows if r.first_activity_date is not None]
        lasts = [r.last_activity_date for r in rows if r.last_activity_date is not None]
        if firsts or lasts:
            first_seen = min(firsts) if firsts else min(lasts)
            last_seen = max(lasts) if lasts else max(firsts)
        else:
            created_dates = [r.created_at.date() for r in rows]
            first_seen = min(created_dates)
            last_seen = max(created_dates)
        display_name = _canonical_name_for(rows[0])
        history_rows.append(
            CanonicalNameHistory(
                subject_type=NameHistorySubjectType.entity,
                subject_id=canonical_entity_id,
                name=display_name,
                normalized_name=norm_name,
                first_seen_date=first_seen,
                last_seen_date=last_seen,
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


def _load_phase2_clusters(
    session: Session,
    run_id: int,
) -> list[tuple[Cluster, bool]] | None:
    """Read clusters from the ``ClusterAssignment`` staging table for *run_id*.

    Returns a list of ``(Cluster, held_for_review)`` tuples when Phase 2
    cluster rows exist, or ``None`` when the table is empty for this run
    (which triggers Phase 1 trivial-clustering fallback).
    """
    rows = list(
        session.exec(select(ClusterAssignment).where(ClusterAssignment.run_id == run_id)).all()
    )
    if not rows:
        return None

    members_by_id: dict[str, list[tuple[str, str]]] = defaultdict(list)
    held_by_id: dict[str, bool] = {}
    for row in rows:
        members_by_id[row.cluster_id].append((row.source_type, row.source_id))
        held_by_id[row.cluster_id] = row.held_for_review

    result: list[tuple[Cluster, bool]] = []
    for cluster_id, members in members_by_id.items():
        result.append(
            (
                Cluster(cluster_id=cluster_id, members=sorted(members)),
                held_by_id[cluster_id],
            )
        )
    return result


# ---------------------------------------------------------------------------
# Private: per-node crosswalk attribute derivation
# ---------------------------------------------------------------------------

# Map MergeEdge.edge_source → MatchMethod for crosswalk rows.
_EDGE_SOURCE_TO_METHOD: dict[str, MatchMethod] = {
    "deterministic": MatchMethod.exact,
    "probabilistic": MatchMethod.probabilistic,
    "approved_review": MatchMethod.approved_review,
}

# Priority order for choosing a method when a node has multiple merge edges.
# Higher value wins; singletons default to exact (priority 1).
_METHOD_PRIORITY: dict[MatchMethod, int] = {
    MatchMethod.approved_review: 3,
    MatchMethod.probabilistic: 2,
    MatchMethod.exact: 1,
    MatchMethod.deterministic_rule: 0,
    MatchMethod.manual: 0,
}


def _build_node_crosswalk_attrs(
    session: Session,
    run_id: int,
) -> dict[tuple[str, str], tuple[MatchMethod, float | None]]:
    """Return a map from (source_type, source_id) → (match_method, match_score).

    Used by ``run_survivorship_stage`` to set accurate per-member crosswalk
    attributes instead of a single hard-coded ``deterministic_rule``.

    Rules
    -----
    - ``deterministic`` edges → ``MatchMethod.exact``, score ``None``
    - ``probabilistic`` edges → ``MatchMethod.probabilistic``, score from
      ``MatchDecision`` for that pair
    - ``approved_review`` edges → ``MatchMethod.approved_review``, score ``None``
    - Approved ``MergeReview`` rows (any run) → ``MatchMethod.approved_review``
      (covers reviews from previous runs not yet re-emitted as MergeEdge rows)
    - Singletons absent from the map → ``(MatchMethod.exact, None)``

    When a node has multiple edges of different types, the highest-priority
    method wins (approved_review > probabilistic > exact > deterministic_rule).
    """
    node_method: dict[tuple[str, str], MatchMethod] = {}
    node_score: dict[tuple[str, str], float | None] = {}

    def _update(node: tuple[str, str], method: MatchMethod, score: float | None) -> None:
        existing = node_method.get(node)
        if existing is None or _METHOD_PRIORITY.get(method, -1) > _METHOD_PRIORITY.get(
            existing, -1
        ):
            node_method[node] = method
            node_score[node] = score

    # Probabilistic scores keyed by unordered pair — used when edge is probabilistic.
    pair_score: dict[frozenset[tuple[str, str]], float] = {}
    for decision in session.exec(select(MatchDecision).where(MatchDecision.run_id == run_id)).all():
        if decision.score is not None and decision.method == MatchMethod.probabilistic:
            key = frozenset(
                {
                    (_source_type_str(decision.source_a_type), decision.source_a_id),
                    (_source_type_str(decision.source_b_type), decision.source_b_id),
                }
            )
            pair_score[key] = decision.score

    # Process MergeEdge rows for this run.
    for edge in session.exec(select(MergeEdge).where(MergeEdge.run_id == run_id)).all():
        node_a = (edge.source_a_type, edge.source_a_id)
        node_b = (edge.source_b_type, edge.source_b_id)
        method = _EDGE_SOURCE_TO_METHOD.get(edge.edge_source, MatchMethod.exact)
        score: float | None = None
        if method == MatchMethod.probabilistic:
            score = pair_score.get(frozenset({node_a, node_b}))
        _update(node_a, method, score)
        _update(node_b, method, score)

    # Approved MergeReview rows from any run — covers human approvals that
    # survivorship loads via load_cluster_edges (no run_id filter there).
    for review in session.exec(
        select(MergeReview).where(MergeReview.status == ReviewStatus.approved)
    ).all():
        node_a = (_source_type_str(review.source_a_type), review.source_a_id)
        node_b = (_source_type_str(review.source_b_type), review.source_b_id)
        _update(node_a, MatchMethod.approved_review, None)
        _update(node_b, MatchMethod.approved_review, None)

    return {node: (node_method[node], node_score.get(node)) for node in node_method}


def _clear_live_canonical_snapshot(session: Session) -> None:
    """Remove the prior live canonical snapshot before publishing this run.

    Each successful survivorship stage replaces the full live canonical layer
    so reruns do not accumulate duplicate golden records.  Historical per-run
    mappings remain in ``entity_crosswalk`` keyed by ``run_id``.

    CanonicalCampaign is cleared first: its committee_entity_id/candidate_entity_id
    FKs reference canonical_entity, so a stale campaign row from a prior campaign
    pass would otherwise block ``DELETE FROM canonical_entity`` on a rerun.  The
    campaign pass (run after entity) repopulates it.
    """
    session.exec(delete(CanonicalNameHistory))
    session.exec(delete(CanonicalCampaign))
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
    state_code: str | None = config.get("state_code", DEFAULT_STATE)
    if not state_code:
        raise ValueError(
            "state_code is required in run config; no default state is assumed (RF-MAGIC-003)"
        )

    input_rows: list[ResolutionInput] = list(
        session.exec(select(ResolutionInput).where(ResolutionInput.run_id == run_id)).all()
    )

    row_by_key: dict[tuple[str, str], ResolutionInput] = {
        (r.source_type, r.source_id): r for r in input_rows
    }

    # ------------------------------------------------------------------
    # Cluster source: Phase 2 staging table, or Phase 1 trivial fallback
    # ------------------------------------------------------------------
    phase2_clusters = _load_phase2_clusters(session, run_id)

    if phase2_clusters is not None:
        # Phase 2 path: use connected-components clusters from Stage 6.
        # Held clusters are not published as merged entities; each member
        # is treated as a singleton so that it still receives a crosswalk
        # row without forcing a mega-cluster into the live canonical layer.
        cluster_with_held: list[tuple[Cluster, bool]] = phase2_clusters

        # Expand held clusters into per-member singletons
        expanded: list[tuple[Cluster, bool]] = []
        for cluster, held in cluster_with_held:
            if held:
                for member in cluster.members:
                    singleton = Cluster(members=[member])
                    expanded.append((singleton, False))
            else:
                expanded.append((cluster, False))

        clusters_tagged = expanded
    else:
        # Phase 1 fallback: trivial union-find over merge edges.
        all_source_keys: list[tuple[str, str]] = [(r.source_type, r.source_id) for r in input_rows]
        edges = load_cluster_edges(session, run_id)
        trivial_clusters = cluster_edges(edges, all_source_keys=all_source_keys)
        clusters_tagged = [(c, False) for c in trivial_clusters]

    _clear_live_canonical_snapshot(session)
    session.exec(delete(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id))

    # Build per-node crosswalk attributes once before iterating clusters so
    # every EntityCrosswalk row carries the correct match_method and score.
    node_xwalk_attrs = _build_node_crosswalk_attrs(session, run_id)

    canonical_count = 0
    for cluster, _held in clusters_tagged:
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

            member_method, member_score = node_xwalk_attrs.get(
                (member_type, member_id),
                (MatchMethod.exact, None),
            )

            session.add(
                EntityCrosswalk(
                    source_type=source_type,
                    source_id=member_id,
                    canonical_entity_id=entity.id,
                    match_method=member_method,
                    match_score=member_score,
                    confidence_band=ConfidenceBand.auto,
                    run_id=run_id,
                )
            )

        canonical_count += 1

    session.commit()
    return {"canonical_out": canonical_count}
