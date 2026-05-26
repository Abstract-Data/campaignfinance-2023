"""Phase 3 review package — human-review queue, CLI, and explanation rendering.

Exposes three sub-modules so callers can import from ``app.resolve.review``
rather than reaching into each file individually:

- ``queue``   — lifecycle helpers: ``list_pending``, ``approve``, ``reject``, …
- ``cli``     — command-line interface for reviewers (``list``, ``show``, …)
- ``explain`` — explanation rendering and run-level reporting

Task: 3z | Branch: resolve/phase-3/task-3z-integration
"""

from app.resolve.review import cli, explain, queue

__all__ = ["cli", "explain", "queue"]
