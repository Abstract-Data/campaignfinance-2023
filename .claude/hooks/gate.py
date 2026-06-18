#!/usr/bin/env python3
"""
gate.py — Abstract Data Dev-Env enforcement gate (v1.0.0)

One file, three responsibilities, no third-party dependencies (stdlib only):

  1. Stop / SubagentStop loop-closer   -> `gate.py stop-check`
     Refuses to let a Claude Code turn end while there are unresolved
     verification failures or an outstanding task-critic verdict.

  2. Dangerous-ops PreToolUse gate     -> `gate.py pretool`
     DENY for operations that are never appropriate autonomously; ASK
     (escalate to the human) for meaning-changing operations.

  3. Disposition ledger CLI            -> record-failure | dispose | task-critic | status
     A failed check is cleared only by a written disposition that names it.

Philosophy: deterministic, evidence-based, fail-OPEN on internal bugs (a gate
bug must never brick every session) but fail-CLOSED on its conditions. The Stop
gate has a loop guard so a genuinely stuck session is released with a warning.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

VERSION = "1.0.0"
MAX_STOP_BLOCKS = 3  # loop guard: release after this many consecutive blocks


def read_payload() -> dict:
    try:
        raw = sys.stdin.read()
        return json.loads(raw) if raw.strip() else {}
    except Exception:
        return {}


def project_dir(payload: dict | None = None) -> Path:
    env = os.environ.get("CLAUDE_PROJECT_DIR")
    if env:
        return Path(env)
    if payload and payload.get("cwd"):
        return Path(payload["cwd"])
    return Path.cwd()


def state_dir(proj: Path) -> Path:
    d = proj / ".claude" / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def ledger_path(proj: Path, session_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", session_id or "no-session")
    return state_dir(proj) / f"gate-{safe}.json"


def load_ledger(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {"session_id": path.stem, "task_critic": None, "checks": {}, "stop_blocks": 0}


def save_ledger(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2))


def emit_stop_block(reason: str) -> None:
    print(json.dumps({"decision": "block", "reason": reason}))
    sys.exit(0)


def emit_allow() -> None:
    sys.exit(0)


def emit_pretool(decision: str, reason: str) -> None:
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "permissionDecision": decision,
                    "permissionDecisionReason": reason,
                }
            }
        )
    )
    sys.exit(0)


def open_items(ledger: dict, require_task_critic: bool) -> list[str]:
    items: list[str] = []
    if require_task_critic:
        tc = ledger.get("task_critic")
        if not tc or tc.get("verdict") != "PASS":
            items.append(
                "task-critic has not recorded a PASS for this session. Run task-critic against TASK.md, then: python .claude/hooks/gate.py task-critic --verdict PASS|BLOCK"
            )
    for name, rec in ledger.get("checks", {}).items():
        if rec.get("status") in ("failed", "skipped") and not rec.get("disposition"):
            detail = rec.get("detail") or ""
            items.append(
                f"check '{name}' is {rec['status']} with no disposition"
                + (f" ({detail})" if detail else "")
                + ". Fix and re-run, or: python .claude/hooks/gate.py dispose --check '"
                + name
                + "' --status fixed|deferred|ticket|ignore --note '...'"
            )
    return items


def cmd_stop_check() -> None:
    payload = read_payload()
    proj = project_dir(payload)
    session_id = payload.get("session_id", "no-session")
    try:
        lpath = ledger_path(proj, session_id)
        ledger = load_ledger(lpath)
        require_tc = (proj / "TASK.md").exists()
        items = open_items(ledger, require_task_critic=require_tc)
        if not items:
            if ledger.get("stop_blocks"):
                ledger["stop_blocks"] = 0
                save_ledger(lpath, ledger)
            emit_allow()
        ledger["stop_blocks"] = int(ledger.get("stop_blocks", 0)) + 1
        save_ledger(lpath, ledger)
        if ledger["stop_blocks"] > MAX_STOP_BLOCKS:
            sys.stderr.write(
                "[gate] WARNING: released after "
                + str(ledger["stop_blocks"])
                + " blocks with unresolved items:\n  - "
                + "\n  - ".join(items)
                + "\n"
            )
            emit_allow()
        reason = (
            "Do not end the turn yet. Unresolved items ("
            + str(len(items))
            + "):\n\n  - "
            + "\n  - ".join(items)
            + "\n\nResolve each, then stop. No session ends on a failed or skipped check without a written disposition."
        )
        emit_stop_block(reason)
    except SystemExit:
        raise
    except Exception as exc:
        sys.stderr.write(f"[gate] internal error in stop-check, allowing: {exc}\n")
        emit_allow()


# Hard DENY: never appropriate for an agent to do autonomously.
BASH_DENY = [
    (
        re.compile(r"\bgit\s+config\s+--global\b"),
        "Global git config changes are blocked. Make this change yourself.",
    ),
    (re.compile(r"\bchmod\b.*\.claude/hooks/"), "Modifying enforcement hook files is blocked."),
]
# ASK (escalate to human): meaning-changing operations from the review.
BASH_ASK = [
    (re.compile(r"\bgit\s+push\b.*(--force|-f)\b"), "Force push — confirm target branch."),
    (
        re.compile(r"\bgit\s+push\b.*\b(main|master|preview|prod|production|release)\b"),
        "Push to a protected branch — confirm.",
    ),
    (re.compile(r"\bgit\s+reset\s+--hard\b"), "Hard reset discards work — confirm."),
    (re.compile(r"\bgit\s+clean\s+-[a-z]*f"), "git clean -f deletes untracked files — confirm."),
    (
        re.compile(r"\balembic\s+(upgrade|downgrade)\b"),
        "Alembic run — confirm target DB is NOT cloud/production (house rule: no cloud alembic upgrades).",
    ),
    (
        re.compile(r"\bsupabase\s+db\s+(push|reset)\b"),
        "Supabase schema push/reset against remote — confirm.",
    ),
    (
        re.compile(r"\bpsql\b.*-c\b.*\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER)\b", re.I),
        "Direct SQL write/DDL — confirm (prefer migrations / queued discovery over direct backfill).",
    ),
    (
        re.compile(r"\bbut\b.*\b(config|reset|undo)\b"),
        "GitButler state-changing operation — confirm.",
    ),
    (re.compile(r"\brm\s+-rf\b"), "Recursive force delete — confirm path."),
]
PATH_DENY = [
    (
        re.compile(r"\.claude/hooks/"),
        "Editing enforcement hook files is blocked. Change them via a reviewed PR.",
    ),
    (
        re.compile(r"\.claude/settings(\.local)?\.json$"),
        "Editing hook settings is blocked. Change them via a reviewed PR.",
    ),
]
PATH_ASK = [
    (
        re.compile(r"\.github/workflows/"),
        "CI workflow edit — confirm. CI changes alter what 'passing' means.",
    ),
    (
        re.compile(r"production.*loader|loader.*production", re.I),
        "Production loader edit — confirm.",
    ),
    (
        re.compile(
            r"(^|/)(pyproject\.toml|requirements[^/]*\.txt|uv\.lock|package\.json|package-lock\.json|bun\.lock(b)?|pnpm-lock\.yaml)$"
        ),
        "Dependency / lockfile change — confirm.",
    ),
    (re.compile(r"(alembic|migrations)/versions/"), "Database migration file — confirm."),
    (
        re.compile(r"(^|/)(Dockerfile|railway\.(json|toml)|vercel\.json|.*\.tf)$"),
        "Infrastructure / deploy config edit — confirm.",
    ),
]


def cmd_pretool() -> None:
    payload = read_payload()
    try:
        tool = payload.get("tool_name", "")
        ti = payload.get("tool_input", {}) or {}
        if tool == "Bash":
            command = ti.get("command", "") or ""
            for pat, msg in BASH_DENY:
                if pat.search(command):
                    emit_pretool("deny", msg)
            for pat, msg in BASH_ASK:
                if pat.search(command):
                    emit_pretool("ask", msg)
            emit_allow()
        if tool in ("Edit", "Write", "MultiEdit"):
            fp = ti.get("file_path", "") or ""
            for pat, msg in PATH_DENY:
                if pat.search(fp):
                    emit_pretool("deny", msg)
            for pat, msg in PATH_ASK:
                if pat.search(fp):
                    emit_pretool("ask", msg)
            emit_allow()
        emit_allow()
    except SystemExit:
        raise
    except Exception as exc:
        sys.stderr.write(f"[gate] internal error in pretool, allowing: {exc}\n")
        emit_allow()


def _arg(flag: str, default: str | None = None) -> str | None:
    a = sys.argv
    return a[a.index(flag) + 1] if flag in a and a.index(flag) + 1 < len(a) else default


def _session_ledger() -> tuple[Path, dict]:
    proj = project_dir()
    session = os.environ.get("CLAUDE_SESSION_ID") or _arg("--session") or "no-session"
    lpath = ledger_path(proj, session)
    return lpath, load_ledger(lpath)


def cmd_record_failure() -> None:
    name = _arg("--check")
    if not name:
        sys.exit("record-failure: --check NAME is required")
    lpath, ledger = _session_ledger()
    ledger.setdefault("checks", {})[name] = {
        "status": _arg("--status", "failed"),
        "detail": _arg("--detail", ""),
        "disposition": None,
        "at": int(time.time()),
    }
    save_ledger(lpath, ledger)
    print(f"recorded {ledger['checks'][name]['status']} check: {name}")


def cmd_dispose() -> None:
    name = _arg("--check")
    status = _arg("--status")
    if not name or status not in ("fixed", "deferred", "ticket", "ignore"):
        sys.exit("dispose: --check NAME and --status fixed|deferred|ticket|ignore required")
    lpath, ledger = _session_ledger()
    rec = ledger.setdefault("checks", {}).get(name)
    if not rec:
        rec = {"status": "failed", "detail": "(no prior record)", "at": int(time.time())}
        ledger["checks"][name] = rec
    rec["disposition"] = {"status": status, "note": _arg("--note", ""), "at": int(time.time())}
    save_ledger(lpath, ledger)
    print(f"disposition recorded for '{name}': {status}")


def cmd_task_critic() -> None:
    verdict = _arg("--verdict")
    if verdict not in ("PASS", "BLOCK"):
        sys.exit("task-critic: --verdict PASS|BLOCK required")
    lpath, ledger = _session_ledger()
    ledger["task_critic"] = {"verdict": verdict, "note": _arg("--note", ""), "at": int(time.time())}
    save_ledger(lpath, ledger)
    print(f"task-critic verdict recorded: {verdict}")


def cmd_status() -> None:
    lpath, ledger = _session_ledger()
    require_tc = (project_dir() / "TASK.md").exists()
    items = open_items(ledger, require_task_critic=require_tc)
    print(f"gate v{VERSION} — ledger: {lpath}")
    print(f"task_critic: {ledger.get('task_critic')}")
    print(f"checks: {json.dumps(ledger.get('checks', {}), indent=2)}")
    print(f"open items: {len(items)}")
    for it in items:
        print(f"  - {it}")


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    dispatch = {
        "stop-check": cmd_stop_check,
        "pretool": cmd_pretool,
        "record-failure": cmd_record_failure,
        "dispose": cmd_dispose,
        "task-critic": cmd_task_critic,
        "status": cmd_status,
        "version": lambda: print(VERSION),
    }
    fn = dispatch.get(cmd)
    if not fn:
        sys.exit(
            f"gate.py v{VERSION}\nusage: gate.py {{stop-check|pretool|record-failure|dispose|task-critic|status|version}}"
        )
    fn()


if __name__ == "__main__":
    main()
