#!/usr/bin/env python3
"""Verify AGENTS.md contains the required sections.

Run in CI (.github/workflows/tool-config-verify.yml) and locally. Pure stdlib —
no project dependencies. Exits non-zero if any required section is missing.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
AGENTS_MD = REPO_ROOT / "AGENTS.md"

# Section headings (or header-block fields) that must appear in AGENTS.md.
REQUIRED_MARKERS: list[str] = [
    "# Version:",
    "## Agent Scope",
    "## Model Configuration",
    "## Documentation Priority",
    "## Tool Permissions by Mode",
    "## Goal Proposal Protocol",
    "## Session Management",
    "## Definition of Done",
    "## Anti-Pattern Warnings",
    "## Notion References",
    "## Boundaries & Guardrails",
]

# Notion References block must carry resolved values, not placeholders.
FORBIDDEN_SUBSTRINGS: list[str] = [
    "<!-- RESOLVE",
    "{project-page-id}",
    "{client-page-id}",
]


def main() -> int:
    if not AGENTS_MD.is_file():
        print(f"FAIL: {AGENTS_MD} not found")
        return 1

    text = AGENTS_MD.read_text(encoding="utf-8")
    missing = [m for m in REQUIRED_MARKERS if m not in text]
    placeholders = [s for s in FORBIDDEN_SUBSTRINGS if s in text]

    if missing:
        print("FAIL: AGENTS.md is missing required section(s):")
        for m in missing:
            print(f"  - {m}")
    if placeholders:
        print("FAIL: AGENTS.md still contains unresolved placeholder(s):")
        for s in placeholders:
            print(f"  - {s}")

    if missing or placeholders:
        return 1

    print(f"OK: AGENTS.md has all {len(REQUIRED_MARKERS)} required sections.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
