# GitButler — Virtual Branch Workflow

This project uses [GitButler](https://gitbutler.com) for branch management. This
document explains the workflow and the one hard rule agents must follow.

## Why this exists

GitButler replaces the normal "one branch checked out at a time" model with
**virtual branches**: several independent lines of work applied to the working
tree at once, each tracking its own set of file changes. The real Git branch
checked out on disk is `gitbutler/workspace` — an integration branch GitButler
manages for you.

Because GitButler owns the working tree and the index, **raw Git branch commands
desync it**. Running `git checkout`, `git switch`, `git branch <name>`, or
`git merge` directly moves Git's `HEAD` out from under GitButler, and the next
GitButler operation can lose or scramble virtual-branch assignments.

## The one rule

**Never run raw `git checkout` / `git switch` / `git branch <name>` / `git merge`
for feature work.** Use GitButler — the desktop app or the `but` CLI.

This is enforced by `.claude/hooks/block-raw-git.sh` (a PreToolUse hook): those
commands are blocked before they run. Read-only inspection is still allowed —
`git status`, `git log`, `git diff`, `git branch --list`, `git branch
--show-current`, and `git checkout -- <file>` (file restore) all pass through.

## Workflow

1. **Create a virtual branch** for the unit of work
   `but branch create <name>`
2. **Work normally.** Edit files, run tests. GitButler tracks which changes
   belong to which virtual branch — you do not switch branches to move between
   them.
3. **Assign changes** to the right virtual branch if you have more than one
   active (done in the GitButler app, or via `but`).
4. **Commit** within the virtual branch.
5. **Push** the branch to the remote
   `but branch push <name>`
6. **Open a PR** from the pushed branch — standard GitHub flow from here.

## Command reference

GitButler's CLI is `but`. The exact subcommand surface evolves between
releases — run `but --help` (and `but branch --help`) for the authoritative
list. Common operations:

| Task | Command |
|------|---------|
| List virtual branches | `but branch list` |
| Create a virtual branch | `but branch create <name>` |
| Switch active virtual branch | `but branch switch <name>` |
| Push a virtual branch | `but branch push <name>` |
| Show status | `but status` |

The GitButler desktop app covers everything the CLI does, plus drag-and-drop
assignment of changes (hunks) between virtual branches.

## Common patterns

- **Parallel features** — create one virtual branch per feature; all are live
  in the working tree at once. No stashing, no context-switch checkouts.
- **Stacking** — build a virtual branch on top of another for dependent work;
  GitButler keeps the stack ordered when you push.
- **Conflict resolution** — GitButler surfaces conflicts per virtual branch when
  branches touch the same lines; resolve in the app and re-apply.

## If a raw git command is genuinely needed

The hook blocks branch-context commands, not all of Git. If you hit a real case
the hook blocks and GitButler can't express, do it yourself in a terminal
outside the agent session — and expect to reconcile GitButler afterward
(re-apply virtual branches from the app).

## References

- GitButler documentation: https://docs.gitbutler.com
- Enforcement hook: `.claude/hooks/block-raw-git.sh`
- Project rule: `AGENTS.md` → `## GitButler`
