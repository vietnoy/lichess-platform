# Session resume state — last updated 2026-05-13

Working notes so a fresh Claude Code session (any machine) can pick up cleanly.

## Current shipped state (live on VPS)

All work is **deployed and committed to `main`** as `vietnoy`.

- Frontend: http://160.187.0.108:30900 — home, /player/[name], /patterns/[name], /game/[id], /whatif/[id]/[ply], /coach all live
- Backend: http://160.187.0.108:30900/api — `/metrics`, `/api/freshness`, `/api/whatif`, `/api/games/{id}/evaluations`, `/api/players/{name}/patterns`, `/api/games/{id}/analyze`, `/api/exercise/<name>`
- Analyzer: `services/analyzer/worker.py` runs continuously in-cluster as `chess-analyzer-worker` Deployment
- StarRocks MySQL: `mysql -h 160.187.0.108 -P 30930 -u root` (no password — `chess-secrets` value is stale; CN startup tries passwordless first)
- Airflow: http://160.187.0.108:30808 (admin / `kubectl get secret chess-secrets -n chess -o jsonpath='{.data.AIRFLOW_ADMIN_PASSWORD}' | base64 -d`)
- Streamlit (legacy): http://160.187.0.108:30051

## On-demand Stockfish analyzer (NEW — shipped 2026-05-12 → 2026-05-13)

The legacy `analyze_blunders` Airflow DAG was a top-5-players-per-day path and **is not running anymore**. The continuous on-demand worker has replaced it for fresh evaluations.

**Flow:**

```
chess-analyzer-worker pod (1 replica)
   └─ every 5s sleep + ~30-60s of work per cycle:
       1. fetch_eligible_players(5) — Postgres, ORDER BY throttle_until ASC NULLS FIRST
       2. ThreadPoolExecutor(5) — 5 players run concurrently, each gets its own
          (pg, sr) connection pair from psycopg2/mysql connection pools
       3. process_player:
            a. fetch_player_games (≤20 newest unprocessed)
            b. fetch_plies_batch — ONE SR query for all 20 games' plies
            c. eval_plies_batch — bulk Postgres cache lookup, ThreadPoolExecutor(8)
               for Stockfish HTTP misses, bulk INSERT into position_evals
            d. classify each ply, batch insert into move_evaluations_ondemand
            e. update_cursor, throttle_until = now + 24h

Postgres chess_analyzer_db (staging — fresh, ~1 sec latency)
   └─ daily Spark compaction (processing/compact_ondemand_evals.py @ 01:15 UTC)

Iceberg polaris.prod.move_evaluations_ondemand (permanent, via MinIO parquet)
   └─ StarRocks reads via polaris_catalog REST → backend UNION with legacy table
```

**Current throughput:** ~4 players/min sustained. Full first pass of 366K eligible players takes ~64 days. After first pass, cursor revisits each player every ~24h (or however long the queue takes when fully populated).

**Why not faster:** the bottleneck is Stockfish total CPU on the box. 8 Stockfish replicas at ~20 evals/sec = 160 evals/sec aggregate. Each cycle does 5 players × ~20 games × ~30 plies × ~50% cache miss = ~1500 Stockfish calls. Stockfish scaling beyond 8 needs a bigger node.

**24h throttle is the FLOOR not the cadence.** A player's `throttle_until = last_processed_at + 24h` says "don't come back sooner than 24h," but the queue is huge so realistic revisit interval ≈ time-for-one-full-pass.

**Resource layout (post-tuning 2026-05-13):**

| Service | Replicas | CPU req | Mem req | Mem limit |
|---|---|---|---|---|
| stockfish | 8 | 100m | 256Mi | 512Mi |
| chess-analyzer-worker | 1 | 100m | 192Mi | 256Mi |
| starrocks-fe | 1 | 500m | 1Gi | 2Gi (JVM -Xmx1536m) |
| starrocks-cn | 2 | 250m | 768Mi | 2Gi |
| spark-worker | 4 | 100m | 512Mi | 3Gi |
| airflow-scheduler | 1 | 150m | 512Mi | 5Gi |

Node total: 6 cores / ~20 GiB. ~86% CPU requested, ~50% memory requested. Plenty of headroom.

## Test data known good

- `diamonddoll` — legacy table data, /patterns shows 25 games, 33 blunders. Confirms legacy half of UNION.
- `tabriz55` — on-demand only (never in legacy). /patterns shows 8 games, 24 blunders. Confirms on-demand half.
- `temporalmente` — 64 games, canonical example
- Game ID: `0jBgxOKP` — eval timeline + AI analyze verified
- Game ID: `aqXZphC1` — older known-good
- Latest analyzed date in legacy `move_evaluations`: `2026-04-18` (frozen; daily DAG is dead)
- Latest analyzed date in on-demand `move_evaluations_ondemand`: yesterday's compaction (manual run on 2026-05-12)

## Load-bearing gotchas (do not re-discover)

1. **Stockfish service is GET-only.** `POST /eval` returns 405. Use `session.get(STOCKFISH_URL, params={"fen":..., "depth":12})`.
2. **StarRocks CN needs ≥2Gi memory limit** for concurrent bulk fetch_plies_batch queries from the analyzer. 1Gi caused per-query OOMs (`Memory of process exceed limit`). Don't downsize.
3. **StarRocks FE JVM heap must be capped to fit container.** Default `-Xmx8192m` inside a 768Mi container = guaranteed OOMKill. Manifest now sets `-Xmx1536m` via sed at startup; container limit 2Gi.
4. **CN auto-registration is passwordless-first.** After an FE state-wipe the root account is passwordless until `starrocks-init` runs again. CN startup tries passwordless `mysql -uroot` first, falls back to `STARROCKS_PASSWORD` — fixes the chicken-and-egg where stale secret value blocked re-registration.
5. **Polaris catalog evaporates on FE restart.** Re-create via `airflow tasks test init_catalog_starrocks setup_polaris_catalog <date>` then the refresh task. Both run in <1 min.
6. **chess_move_events has upstream duplicates** per `(game_id, move_number)`. Always `GROUP BY` or `SELECT DISTINCT` before aggregating.
7. **No index on `white_id`/`black_id`** on chess_move_events. Broad scans OOM the CN. Always include `date >= ...` predicate.
8. **Patterns query must scope by game_ids first.** Two-pass: find user's games (small, partition-pruned by date), then `WHERE game_id IN (...)` on both sides of the join. See `serving/backend/db.py::query_player_patterns`.
9. **/patterns and /exercise use `UNION` (not UNION ALL)** across legacy + on-demand eval tables. Same Stockfish depth produces identical rows when a game is in both — UNION dedupes, UNION ALL double-counts.
10. **Win-rate SQL pattern.** Never nested `CASE WHEN winner = CASE WHEN white_id=...` — produced 150% win rates in the old app. Use explicit `(white_id=%s AND winner='white') OR (black_id=%s AND winner='black')` with `SELECT DISTINCT game_id` subquery to dedupe.
11. **Off-by-one on game board.** `moves[ply].fen` is the position **before** ply N. Use `chess.js`-derived `fenAfter(moves, ply)`.
12. **Cross-arch Docker builds on M-Mac via QEMU take 30+ min.** Build on the VPS (native amd64, ~2 min): `rsync ... root@160.187.0.108:/tmp/<svc>-build/ && ssh root@... docker build && docker push`.
13. **Disk pressure on k3s.** Evicted spark-worker pods accumulate. Periodic cleanup: `kubectl delete pod -n chess --field-selector=status.phase=Failed && k3s crictl rmi --prune`.
14. **Analyzer worker DOES NOT auto-recover from a broken StarRocks connection mid-cycle.** Per-player try/except catches the error so main()'s reconnect logic never sees it. Workaround: `kubectl delete pod -l app=chess-analyzer-worker` to restart. Proper fix is in the punchlist.

## Cross-account git push (this repo specifically)

User has two GitHub accounts: `khangdv-sonat` (default) and `vietnoy` (target for this repo). After `gh auth login` for both, push as vietnoy via:

```bash
~/git-push-as vietnoy main
```

Or `gh auth switch -u vietnoy && git push origin main`.

## How to resume on a new machine

1. Install: Claude Code, Codex CLI, Docker, kubectl, gh, Node 20, Python 3.13.
2. Clone this repo. Read `CLAUDE.md` (workflow + team), then this file (current state).
3. `gh auth login` for both accounts. `gh auth setup-git`.
4. Copy VPS kubeconfig to `~/.kube/config` (or merge as a context).
5. Recreate `.env` from the secrets in k3s: `kubectl get secret chess-secrets -n chess -o yaml`.
6. `chmod +x ./codex-agent.sh`.
7. Local Python testing of analyzer/backend: `pip install fastapi openai mysql-connector-python python-dotenv pytest psycopg2-binary` (use `/opt/homebrew/opt/python@3.10/bin/python3.10` on Mac, since Python 3.13 hasn't been installed locally and analyzer tests use `int | None` union syntax that's 3.10+).
8. To resume frontend dev: `BACKEND_URL=http://160.187.0.108:30900 npm --prefix serving/frontend run dev`.

## Punchlist (real follow-ups, prioritized)

| # | Item | Why | Effort |
|---|---|---|---|
| 1 | Fix `cycle()` to propagate `OperationalError` to `main()` reconnect loop | Worker silently wedges on StarRocks restart; needs manual pod kill | Codex unit, ~60 lines |
| 2 | Hourly compaction instead of daily | New on-demand players currently invisible to /patterns and /exercise for up to 24h after worker processes them | DAG schedule edit + verify resource usage |
| 3 | Delete `analyze_blunders.py` and the daily-only DAG entry | Dead code; legacy table is frozen anyway | After on-demand has 30+ days of coverage |
| 4 | Optimize concurrent CN query — `fetch_plies_batch` joins against full `player_games` per call | Mild perf gain at scale, not a blocker today | Codex unit |
| 5 | Drop the per-cycle 30s → 5s sleep further once we trust the queue rate | Throughput tuning | 1-line edit |

## Deferred / known-but-not-doing

- Backfill `analyze_blunders` for old dates — the on-demand worker is doing this work continuously, no need to revive the dead path.
- Airflow scheduler doesn't auto-promote queued runs to running; manual `airflow tasks test` works. Root cause not investigated.

## Notes on the workflow

- See `CLAUDE.md` (project root) and `~/.claude/CLAUDE.md` (user-level) for the Claude-as-architect / Codex-as-coder / specialist-subagents team setup.
- Team this session: Codex did the substantial implementation (compaction Spark job, UNION updates, ply parallelization, bulk fetch, cycle parallelization, patterns UNION). code-reviewer subagent caught one blocker (lazy DataFrame re-eval in compaction) and several majors (incorrect cursor pagination direction, OOM-prone player_games join, missing test assertions). db-explorer used for distribution queries. ops-inspector used twice for cluster diagnosis (StarRocks OOMKill triage, resource budgeting before scale).
- Codex track record across the session: ~9 substantive deliveries, all clean after at most one focused fix round.

## Live URLs reference

- Frontend (live): http://160.187.0.108:30900
- Streamlit (legacy coach): http://160.187.0.108:30051
- Airflow webserver: http://160.187.0.108:30808
- StarRocks SQL: `mysql -h 160.187.0.108 -P 30930 -u root` (no password)
- VPS host (root): `ssh root@160.187.0.108`
