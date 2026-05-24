"""Stage 6: connected-components clustering with mega-cluster guard."""

from __future__ import annotations

import logging
from itertools import combinations
from typing import Any

from sqlalchemy import Column, String, delete
from sqlmodel import Field, SQLModel, Session, select

from app.resolve.models.resolution import MergeReview, SourceType
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.standardize.staging import ResolutionInput

logger = logging.getLogger(__name__)


class ClusterAssignment(SQLModel, table=True):
    """Staging table that maps each source record to a cluster."""

    __tablename__ = "clusters"

    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    cluster_id: str = Field(sa_column=Column(String(64), nullable=False, index=True))
    source_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))
    source_id: str = Field(sa_column=Column(String(128), nullable=False, index=True))
    entity_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))
    held_for_review: bool = Field(default=False, index=True)


NodeKey = tuple[str, str]


def _to_source_type(source_type: str) -> SourceType:
    try:
        return SourceType(source_type)
    except ValueError:
        return SourceType.unified_entity


def _build_components(
    edges: list[MergeEdge],
    all_nodes: set[NodeKey],
) -> list[list[NodeKey]]:
    adjacency: dict[NodeKey, set[NodeKey]] = {node: set() for node in all_nodes}
    for edge in sorted(
        edges,
        key=lambda e: (
            e.source_a_type,
            e.source_a_id,
            e.source_b_type,
            e.source_b_id,
            e.id or 0,
        ),
    ):
        left = (edge.source_a_type, edge.source_a_id)
        right = (edge.source_b_type, edge.source_b_id)
        adjacency.setdefault(left, set())
        adjacency.setdefault(right, set())
        adjacency[left].add(right)
        adjacency[right].add(left)

    seen: set[NodeKey] = set()
    components: list[list[NodeKey]] = []
    for start in sorted(adjacency):
        if start in seen:
            continue

        stack = [start]
        seen.add(start)
        component: list[NodeKey] = []
        while stack:
            node = stack.pop()
            component.append(node)
            for neighbor in sorted(adjacency[node], reverse=True):
                if neighbor in seen:
                    continue
                seen.add(neighbor)
                stack.append(neighbor)

        components.append(sorted(component))

    components.sort(key=lambda members: members[0])
    return components


def run_cluster_stage(
    session: Session,
    run_id: int,
    config: dict[str, Any],
) -> dict[str, int]:
    """Run stage 6 clustering and write the `clusters` staging table."""
    max_cluster_size = int(config.get("max_cluster_size", 0))

    rows = session.exec(
        select(ResolutionInput).where(ResolutionInput.run_id == run_id)
    ).all()
    edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == run_id)).all()

    node_entity_type: dict[NodeKey, str] = {
        (row.source_type, row.source_id): row.entity_type for row in rows
    }
    all_nodes: set[NodeKey] = set(node_entity_type)
    for edge in edges:
        all_nodes.add((edge.source_a_type, edge.source_a_id))
        all_nodes.add((edge.source_b_type, edge.source_b_id))

    components = _build_components(edges, all_nodes)

    existing_reviews = session.exec(
        select(MergeReview).where(MergeReview.run_id == run_id)
    ).all()
    existing_pairs: set[tuple[str, str, str, str]] = set()
    for review in existing_reviews:
        pair = sorted(
            [
                (str(review.source_a_type.value), review.source_a_id),
                (str(review.source_b_type.value), review.source_b_id),
            ]
        )
        existing_pairs.add((pair[0][0], pair[0][1], pair[1][0], pair[1][1]))

    session.exec(delete(ClusterAssignment).where(ClusterAssignment.run_id == run_id))

    held_cluster_count = 0
    for index, members in enumerate(components, start=1):
        held_for_review = max_cluster_size > 0 and len(members) > max_cluster_size
        if held_for_review:
            held_cluster_count += 1
            logger.warning(
                "Mega-cluster guard held cluster run_id=%s cluster_id=%s size=%s",
                run_id,
                index,
                len(members),
            )
            for (left_type, left_id), (right_type, right_id) in combinations(members, 2):
                pair = sorted([(left_type, left_id), (right_type, right_id)])
                pair_key = (pair[0][0], pair[0][1], pair[1][0], pair[1][1])
                if pair_key in existing_pairs:
                    continue
                existing_pairs.add(pair_key)
                session.add(
                    MergeReview(
                        run_id=run_id,
                        source_a_type=_to_source_type(pair[0][0]),
                        source_a_id=pair[0][1],
                        source_b_type=_to_source_type(pair[1][0]),
                        source_b_id=pair[1][1],
                    )
                )

        cluster_id = f"cluster_{index:06d}"
        for source_type, source_id in members:
            session.add(
                ClusterAssignment(
                    run_id=run_id,
                    cluster_id=cluster_id,
                    source_type=source_type,
                    source_id=source_id,
                    entity_type=node_entity_type.get((source_type, source_id), "unknown"),
                    held_for_review=held_for_review,
                )
            )

    session.commit()
    return {"clusters": len(components), "held_for_review": held_cluster_count}
