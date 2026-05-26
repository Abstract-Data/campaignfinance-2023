"""Publish layer for the resolution pipeline.

Exports the view builders and co-location / cross-state helpers that are
wired into the ``publish`` CLI subcommand.

Public API
----------
build_resolved_views
    Create (or replace) the three resolved views:
    ``resolved_transactions``, ``resolved_contributions``, and
    ``resolved_expenditures``.

build_address_occupancy_view
    Create (or replace) the ``address_occupancy`` analytics view.

find_colocated
    Return canonical entities sharing a given address.

assert_colocation
    Record a ``co_located_with`` association between two entities.

suggest_colocations
    Return advisory co-location pairs for a low-frequency address.

get_master_entity
    Follow the ``master_entity_id`` chain to its root.

entities_for_master
    Return all canonical entities in a master group.

link_to_master
    Set ``master_entity_id`` with cycle-guard.
"""

from __future__ import annotations

from app.resolve.publish.colocation import (
    SelfColocationError,
    assert_colocation,
    find_colocated,
    suggest_colocations,
)
from app.resolve.publish.crossstate import (
    entities_for_master,
    get_master_entity,
    link_to_master,
)
from app.resolve.publish.occupancy import build_address_occupancy_view
from app.resolve.publish.views import build_resolved_views

__all__ = [
    "build_resolved_views",
    "build_address_occupancy_view",
    "find_colocated",
    "assert_colocation",
    "suggest_colocations",
    "SelfColocationError",
    "get_master_entity",
    "entities_for_master",
    "link_to_master",
]
