# Model: claude-sonnet-4-6

# Task 3a: Add cf resolve prune Command

## Phase

Wave 3 — parallel with 3b (disjoint file ownership). Only after wave-2-complete.

## Branch

`db-bloat/wave-3/task-3a-resolve-prune`

## Objective

Add a `cf resolve prune` Typer subcommand that deletes stale resolve run rows in
FK-safe order, keeping the latest N run_ids (default 1).

## File Ownership

This task owns:
- `app/cli/resolve_prune.py` (new file)
- `app/cli/main.py` — wire in the new command

Do NOT touch `scripts/`, `docs/`, or any resolve stage files.

## Mandatory Pre-work

Read `app/resolve/reverse.py` to understand the FK-safe delete order already
implemented for rollback. Mirror that order for pruning.

Read `app/resolve/stages/*.py` to identify which tables are per-run.

## Implementation

### Step 1: Create app/cli/resolve_prune.py

```python
"""cf resolve prune — delete stale resolve run rows, keeping latest N."""
import typer
from sqlmodel import Session, select
from app.core.unified_database import db_manager
from app.logger import Logger

logger = Logger(__name__)
app = typer.Typer()


@app.command()
def prune(
    keep: int = typer.Option(1, "--keep", help="Number of most recent run_ids to keep"),
    purge_headers: bool = typer.Option(False, "--purge-headers", help="Also delete match_run header rows"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting"),
) -> None:
    """Delete stale resolve run rows, keeping the latest --keep run_ids."""
    ...
```

FK-safe delete order (mirror `app/resolve/reverse.py`):
1. `match_decision`
2. `merge_review`
3. `scored_pairs`
4. `candidate_pairs`
5. `merge_edges`
6. `cluster_assignment`
7. `resolution_input`
8. Per-run crosswalk rows: `entity_crosswalk`, `address_crosswalk`, `campaign_crosswalk`
9. `match_run` headers (only if `--purge-headers`)

Logic:
1. Query all distinct `run_id` values from `match_run` (or `resolution_input`)
2. Sort by created_at descending
3. Keep the top `--keep` run_ids
4. For each stale run_id, delete from each table in FK-safe order
5. Print rows deleted per table (use `Logger`, not `print()`)
6. Use a single transaction — rollback on any error (idempotent)

### Step 2: Wire into app/cli/main.py

Add the prune command to the existing Typer app under the `resolve` group or as
`cf resolve prune`. Follow the pattern of existing CLI commands in `app/cli/`.

## Testing

Write a simple unit test that mocks the DB session and verifies:
- Only stale run_ids are deleted
- Latest run_id is preserved
- Deletion happens in FK-safe order

## Commit

```
feat: add cf resolve prune retention command (3a)
```

## Checklist

- [ ] Wave 2 `db-bloat/wave-2-complete` tag confirmed
- [ ] `app/resolve/reverse.py` read to understand FK-safe delete order
- [ ] `app/cli/resolve_prune.py` created with idempotent, transactional prune logic
- [ ] FK-safe order: match_decision → merge_review → scored_pairs → candidate_pairs → merge_edges → cluster_assignment → resolution_input → crosswalks → headers
- [ ] `app/cli/main.py` wired in
- [ ] `--keep N`, `--purge-headers`, `--dry-run` flags implemented
- [ ] Uses `Logger`, not `print()`
- [ ] `gitnexus_detect_changes()` run before commit
