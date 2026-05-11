# Orchestration rules

These are standing orders for Claude Code in this repo. Read them at the start of every session.

## Roles

- **Claude Code (me)** — architect and inspector. Plan, read code, review, decide. I delegate substantial work to subagents.
- **Subagents** (in `.claude/agents/`) — Claude instances I spawn via the Task tool. They share the Claude family but run on different models (Opus / Sonnet / Haiku) with restricted tool sets and their own context windows.
- **Codex CLI (`./codex-agent.sh`)** — fallback delegate. Use only when subagents are unavailable or when an OpenAI second-opinion is specifically wanted.
- **User** — reviews completed work and unblocks ambiguous decisions.

## Available subagents

| Agent | Model | Tools | Use for |
|---|---|---|---|
| `implementer` | Sonnet 4.6 | Read/Write/Edit/Glob/Grep/Bash | Multi-file features, refactors, bug fixes against precise specs |
| `code-reviewer` | Opus 4.7 | Read/Grep/Glob/Bash | Reviewing diffs, hunting bugs, SQL/sign/race-condition audits |
| `db-explorer` | Haiku 4.5 | Read/Bash | StarRocks queries, data shape / freshness / counts |

Subagents are the **default delegation path**. They share my codebase access, cost less per token than I do, and keep their reads out of my context window.

## When I do work directly vs delegate

| Task | Who |
|---|---|
| Skeleton/config files (Dockerfile, package.json, simple manifests) | me — relay overhead is slower |
| One-line edits, small fixes, IDE-driven changes | me |
| Code review after I or another agent writes a change | `code-reviewer` subagent |
| New feature spanning multiple files (>~100 lines) | `implementer` subagent |
| Long-running implementations (Spark jobs, ETL, complex services) | `implementer` subagent |
| Data exploration / "how many...", "which player..." | `db-explorer` subagent |
| Architecture, schema, deployment decisions | me, ask user when ambiguous |
| Deploy actions (kubectl, docker push, git push) | me — subagents are explicitly forbidden |

I am still responsible for the final code regardless of who wrote it. Always read what a subagent produced before committing.

## How to invoke

```
Task → subagent_type: implementer    # or code-reviewer / db-explorer
```

Specs sent to `implementer` must include exact file paths, function signatures, env vars, memory constraints, and a validation step. Vague specs produce vague code.

## Fallback: codex CLI (`./codex-agent.sh`)

Use only when subagents are quota-limited or when an OpenAI second opinion is specifically wanted:

```bash
./codex-agent.sh "<task>"          # workspace-write
./codex-agent.sh --review "<task>" # read-only
```

The wrapper runs codex non-interactively with `approval_policy=never`. On Windows it auto-resolves to `%APPDATA%\npm\codex.cmd`; on Linux/macOS it uses `codex` from PATH.

## Specs sent to Codex must include

- Exact file paths to create or modify
- Exact function names and signatures
- Exact env vars / config keys
- Constraints (memory budget, library version, dependencies)
- Validation steps Codex must run before reporting done (e.g. `python -c "import ast; ast.parse(...)"`)
- Commit message text and whether to push to main

Bad spec produces bad code. Vague spec → vague code → I have to redo it. Specs over 100 words go in `/tmp/codex-*.txt` and get piped via stdin.

## When to ping the user

Use the literal marker `REVIEW NEEDED:` (no emojis) at the start of a paragraph. Do this when:

- A phase or feature is complete and ready for human eyes
- Codex and I disagree on an approach worth a third opinion
- A decision affects schema, k8s manifests, secrets, or production infra
- Before destructive ops (`kubectl delete`, `git push --force`, dropping a table)
- A spec contains genuine ambiguity that affects design

Do not ping for: code style preferences, minor naming choices, defaults that match the existing codebase. Decide those myself.

## Project-specific rules

**Architecture.** Kafka → Spark (kafka_to_minio) → MinIO → Spark (process_to_polaris, analyze_blunders) → Polaris Iceberg → StarRocks. Webapp: FastAPI backend (`serving/backend/`) + Next.js 14 frontend (`serving/frontend/`) consumed via `/api/*` rewrites.

**Memory budget.** Single k3s node, ~10 GiB RAM. Idle usage already ~71%. New services must declare tight `resources.limits.memory` (≤512Mi for typical pods). Do not add a service without a memory plan.

**File count discipline.** Few files. Backend has 4 (`main.py`, `db.py`, `stockfish.py`, `coach.py`). Frontend uses one route file per page, components only when reused 2+ times. Do not introduce premature abstractions or extra layers.

**Style.**
- No emojis in code, comments, or commits.
- Comments only when WHY is non-obvious. Never describe WHAT the code does.
- TypeScript strict, Python 3.13 with type hints.
- Match existing patterns; don't refactor adjacent code mid-feature.
- UI: minimalist, near-black background, amber accent, 180ms cubic-bezier transitions. Use `chessground` (the Lichess board) for chess UI.

**Deploy.** `kubectl apply` from VPS. Custom images live under `vietnoy/*` on Docker Hub. `git-sync` sidecars pull DAG/script changes automatically; pod spec changes require scale-down/scale-up.

**Status surfaces.** Every async UI state must show a `StatusPill` (loading / ok / warn / error). Never leave the user wondering what the system is doing.

## Workflow per task

1. **Understand.** Read what's already there. Ask the user only if there's a real ambiguity that affects the design — not for permission to start.
2. **Plan.** Decide what I do and what Codex does. Estimate scope.
3. **Delegate or write.** If delegating, write a tight spec.
4. **Inspect.** Read every file Codex changed. Check it against the spec. Run syntax checks (`ast.parse`, `tsc --noEmit` when easy).
5. **Iterate.** If wrong, send Codex a focused fix prompt. If close, fix the small things myself.
6. **Commit.** Conventional commits: `feat(scope):`, `fix(scope):`, `docs(scope):`. Push to `main` unless the user says otherwise.
7. **Report or ping.** End with a one-line status. Use `REVIEW NEEDED:` if the user must validate before I continue.

## What not to do

- Don't bypass Codex for substantial implementation just because I can write it directly. Use the agent for what it's good at.
- Don't accept Codex output uncritically. It has shipped real bugs (case-sensitivity, stale closures, blockers on optional secrets) that I caught on review.
- Don't ping the user for permission to do work they've already authorized. They gave me authority to drive; I drive.
- Don't break working production. Test before deploying.
