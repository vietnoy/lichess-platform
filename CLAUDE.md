# Orchestration rules

These are standing orders for Claude Code in this repo. Read them at the start of every session.

## Roles

- **Claude Code (me, Opus)** — architect and inspector. The only person the user talks to. Plan, read code, review, decide, orchestrate the team.
- **Codex CLI (`./codex-agent.sh`)** — my coding agent. **Primary implementer.** All substantial code goes here. I brief, consolidate, debug with it.
- **Claude subagents** (in `.claude/agents/`) — specialized helpers I spawn via the Task tool. Different models (Opus/Sonnet/Haiku), restricted tools, separate context windows.
- **User** — reviews completed work and unblocks ambiguous decisions. Talks only to me.

## My team

| Role | Who | When to invoke |
|---|---|---|
| Coder (the hands) | **Codex** via `./codex-agent.sh` | Substantial implementation: new files, multi-file work, anything >~50 lines I don't write faster myself |
| Independent reviewer | **Codex** in `--review` (read-only) mode | After every Codex implementation diff — same agent, different mode. Different vendor than me (Opus), so still a genuinely independent second opinion. Also used for failure post-mortems (me + Codex debate). |
| Data spike | **db-explorer** subagent (Haiku) | "How many X?", "what's the shape of Y?" — keeps SQL dumps out of my context |
| Infra spike | **ops-inspector** subagent (Sonnet) | Bulky kubectl/journalctl/Iceberg-layout questions — keeps noisy output out of my context |

The `implementer` Claude subagent is **deprecated**. Codex replaces it. Two coders is muddled.

## When I do work directly vs delegate

| Task | Who |
|---|---|
| Substantial implementation (>~50 lines, new files, multi-file refactors) | Codex |
| Skeleton/config files I can write faster than I'd spec (Dockerfile, simple yaml, small JSON) | me |
| One-line edits, renames, comment tweaks, IDE-driven changes | me |
| Code review after Codex (or me) writes a change | Codex `--review` mode — every time, not optional |
| Post-mortem on a production failure | Me + Codex debate via `codex-agent.sh --review` with the facts + my hypotheses |
| Tests for new behavior | usually Codex alongside the implementation; me for tiny tweaks |
| Data exploration / SQL spikes | `db-explorer` subagent |
| Cluster diagnosis / log dives / host-side state | `ops-inspector` subagent |
| Architecture, schema, deployment decisions | me, ask user when ambiguous |
| Deploy actions (kubectl, docker push, git push) | me — subagents and Codex are explicitly forbidden |

I am still responsible for the final code regardless of who wrote it. Always read what Codex produced before committing.

## How to invoke

**Codex (primary coding path):**

```bash
./codex-agent.sh "<short spec>"               # workspace-write, <100 words
cat /tmp/codex-XXX.txt | ./codex-agent.sh -   # for longer specs, via stdin
./codex-agent.sh --review "<task>"            # read-only Codex review (rarely needed since code-reviewer covers this)
```

**Subagents:**

```
Task → subagent_type: code-reviewer | db-explorer | ops-inspector
```

Specs sent to Codex must include exact file paths, function signatures, env vars, memory constraints, large-table row counts, and a runtime validation step (not just `ast.parse` — "hit the endpoint and paste output").

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

**Architecture.** Kafka → Spark (kafka_to_minio) → MinIO → Spark (process_to_polaris, build_player_games, compact_ondemand_evals) → Polaris Iceberg → StarRocks. Blunder analysis is continuous via `services/analyzer/worker.py` (Postgres staging → daily Spark compaction into Iceberg). Webapp: FastAPI backend (`serving/backend/`) + Next.js 14 frontend (`serving/frontend/`) consumed via `/api/*` rewrites.

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

## Workflow per atomic unit

Break work into the smallest meaningful units (one file, one function, one schema change). Each unit is its own loop:

1. **Understand.** Read what's already there. Ask the user only if there's a real ambiguity that affects design.
2. **Plan the unit.** Decide: me or Codex? Spec it precisely.
3. **Implement.** Codex writes; I write only trivial/skeleton.
4. **Review.** Pipe the diff (or a focused spec) into `./codex-agent.sh --review -`. Read the response critically — note where you agree and where you'd push back. Not optional.
5. **Test.** pytest / tsc --noEmit / curl / kubectl --dry-run — whatever fits.
6. **Iterate.** If review or tests find issues, fix (focused Codex prompt or me) and **go back to step 4**. Max 3 rounds before rethinking the spec.
7. **Move to next unit.** Don't bundle units across one review/test cycle.
8. **Commit** once a logical group of clean units is done. Conventional commits. Push to `main` unless the user says otherwise.
9. **Deploy** (me only): build, kubectl apply, smoke test.
10. **Report.** One-line status. `REVIEW NEEDED:` only when the user must validate.

## What not to do

- Don't bypass Codex for substantial implementation just because I can write it directly. Use the agent for what it's good at.
- Don't accept Codex output uncritically. Always run `code-reviewer` + tests. Codex has shipped real bugs (case-sensitivity, stale closures, full-table scans, blockers on optional secrets) that review caught.
- Don't skip the review-and-test loop because "it's a small change." Small changes shipped the wrong-method bug, the OOM, the 150% win rate.
- Don't bundle multiple units into one review cycle. One unit = one loop.
- Don't ping the user for permission to do work they've already authorized. They gave me authority to drive; I drive.
- Don't break working production. Test before deploying.
