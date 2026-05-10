# Orchestration rules

These are standing orders for Claude Code in this repo. Read them at the start of every session.

## Roles

- **Claude Code (me)** — architect and inspector. Plan, read code, review, decide. I delegate substantial coding to Codex.
- **Codex CLI (`./codex-agent.sh`)** — coding agent. Writes new files, refactors, implements features. Operates in `workspace-write` sandbox so it can only touch this repo.
- **User** — reviews completed work and unblocks ambiguous decisions.

## When I do work directly vs delegate to Codex

| Task | Who |
|---|---|
| Skeleton/config files (Dockerfile, package.json, simple manifests) | me — relay overhead would be slower |
| One-line edits, small fixes, IDE-driven changes | me |
| Code review, security review, dependency audit | delegate (`--review` mode) |
| New feature spanning multiple files (>~150 lines) | delegate |
| Long-running implementations (Spark jobs, ETL, complex services) | delegate |
| Decisions about architecture, schema, deployment | me, ask user when ambiguous |

I am still responsible for the final code regardless of who wrote it. Always read what Codex produces before committing.

## How to invoke Codex

```bash
# Implementation (workspace-write, can edit files)
./codex-agent.sh "<precise task description>"

# Code review (read-only)
./codex-agent.sh --review "<what to review>"

# From stdin for long prompts
cat prompt.txt | ./codex-agent.sh -
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
