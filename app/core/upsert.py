"""
Dialect-aware bulk upsert helper (DO UPDATE).

Supports SQLite and PostgreSQL via their respective SQLAlchemy dialect
``insert`` constructs, both of which expose ``.on_conflict_do_update()``.

Usage example::

    from app.core.upsert import bulk_upsert

    total = bulk_upsert(
        session,
        MyModel,
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        conflict_cols=["id"],
    )

The function executes one ``INSERT … ON CONFLICT DO UPDATE`` statement per
chunk and commits once after all chunks are processed.  Callers that wrap
this in an outer transaction should manage the commit themselves — see the
``commit_per_chunk`` parameter if per-chunk durability is required.
"""

from __future__ import annotations

import itertools
from typing import Any, Dict, Iterable, List, Optional, Sequence, Type

from sqlmodel import Session, SQLModel

# Columns whose presence on the model should be excluded from the SET clause
# because they represent immutable creation timestamps.
_IMMUTABLE_COLS: frozenset[str] = frozenset({"created_at", "createdAt"})


def _iter_chunks(
    iterable: Iterable[Dict[str, Any]], chunk_size: int
) -> Iterable[List[Dict[str, Any]]]:
    """Yield successive *chunk_size*-sized lists from *iterable*."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break
        yield chunk


def bulk_upsert(
    session: Session,
    model: Type[SQLModel],
    rows: Iterable[Dict[str, Any]],
    *,
    conflict_cols: Sequence[str],
    update_cols: Optional[Sequence[str]] = None,
    chunk_size: int = 5_000,
    commit_per_chunk: bool = False,
) -> int:
    """Insert *rows* into *model*'s table, updating on conflict.

    Parameters
    ----------
    session:
        An active SQLModel/SQLAlchemy ``Session``.
    model:
        The SQLModel table class (must have ``__table__``).
    rows:
        An iterable of plain ``dict`` objects whose keys match model columns.
        The iterable is consumed lazily in *chunk_size* batches.
    conflict_cols:
        Column name(s) that identify a conflict (typically the primary key).
    update_cols:
        Column name(s) to update on conflict.  ``None`` (default) updates
        *all* columns except those listed in *conflict_cols* and any column
        whose name appears in the immutable-column set
        ``{"created_at", "createdAt"}``.  An explicit empty list ``[]`` means
        ``ON CONFLICT DO NOTHING`` (first-occurrence-wins) — distinct from
        ``None``, which updates everything.
    chunk_size:
        Number of rows per ``INSERT`` statement.  Defaults to 5 000.
    commit_per_chunk:
        When ``True``, commits after every chunk instead of once at the end.
        Use when you need partial-load durability on very large datasets.

    Returns
    -------
    int
        Total number of rows processed (sum of all chunk sizes).

    Raises
    ------
    ValueError
        If the database dialect is not ``sqlite`` or ``postgresql``.
    """
    dialect_name: str = session.get_bind().dialect.name

    if dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as _insert
    elif dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as _insert
    else:
        raise ValueError(
            f"bulk_upsert: unsupported dialect '{dialect_name}'. "
            "Only 'sqlite' and 'postgresql' are supported."
        )

    table = model.__table__

    # Build the set_ mapping once (reused for every chunk).
    if update_cols is not None:
        _update_col_names: Sequence[str] = update_cols
    else:
        conflict_set = set(conflict_cols)
        model_col_names = {c.name for c in table.columns}
        _update_col_names = [
            name
            for name in model_col_names
            if name not in conflict_set and name not in _IMMUTABLE_COLS
        ]

    total_rows = 0

    for chunk in _iter_chunks(rows, chunk_size):
        if not chunk:
            continue

        insert_stmt = _insert(table).values(chunk)

        # An empty SET clause is invalid SQL: when the caller asked for no update
        # columns (explicit update_cols=[]) the intent is ON CONFLICT DO NOTHING.
        if not _update_col_names:
            upsert_stmt = insert_stmt.on_conflict_do_nothing(index_elements=list(conflict_cols))
        else:
            # Build set_ from excluded pseudo-table so values reference the
            # incoming row, not a static literal.
            set_mapping: Dict[str, Any] = {
                col_name: getattr(insert_stmt.excluded, col_name) for col_name in _update_col_names
            }
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=list(conflict_cols),
                set_=set_mapping,
            )

        session.exec(upsert_stmt)  # type: ignore[arg-type]
        total_rows += len(chunk)

        if commit_per_chunk:
            session.commit()

    if not commit_per_chunk:
        session.commit()

    return total_rows
