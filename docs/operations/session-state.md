# Session resume state — last updated 2026-05-10

Working notes so a fresh Claude Code session (any machine) can pick up cleanly.

## Current shipped state (live on VPS)

All UI/backend renovation work is **deployed and committed to `main`** as `vietnoy`.

- Frontend: http://160.187.0.108:30900 — home, /player/[name], /patterns/[name], /game/[id], /whatif/[id]/[ply], /coach all live
- Backend: http://160.187.0.108:30900/api — adds `/metrics`, `/api/freshness`, `/api/whatif`, `/api/games/{id}/evaluations`, `/api/players/{name}/patterns`, `/api/games/{id}/analyze` (24h cache, Groq-backed)
- StarRocks MySQL: `mysql -h 160.187.0.108 -P 30930 -u root` (no password after chess-secrets reset)
- Airflow: http://160.187.0.108:30808 (admin / `kubectl get secret chess-secrets -n chess -o jsonpath='{.data.AIRFLOW_ADMIN_PASSWORD}' | base64 -d`)
- Streamlit (legacy): http://160.187.0.108:30051

## Test data known good

- Player: `diamonddoll` — has analyzed games, patterns dashboard populated (25 games, 38 blunders, 66 mistakes, 94 inaccuracies)
- Player: `temporalmente` — 64 games, canonical example
- Game ID: `0jBgxOKP` — eval timeline + AI analyze verified
- Game ID: `aqXZphC1` — older known-good ID
- Analyzed date partition: `2026-04-18` (only one currently backfilled)

## Load-bearing gotchas (do not re-discover)

1. **Stockfish service is GET-only.** `POST /eval` returns 405. Use `session.get(STOCKFISH_URL, params={"fen":..., "depth":12})`. See `processing/analyze_blunders.py:140`.
2. **Idempotency in analyze_blunders.** Treat NULL-eval rows as not-yet-evaluated so a failed run doesn't poison the partition. Output unions `successful` rows + freshly-evaluated rows; this drops the poison. See `processing/analyze_blunders.py:259`.
3. **Patterns query must scope by game_ids first.** `chess_move_events` is ~17M rows; joining evals against the full table OOMs the backend pod. Two-pass: find user's games (small, indexed by date), then `WHERE game_id IN (...)` on both sides of the join. See `serving/backend/db.py` `query_player_patterns`.
4. **Win-rate SQL pattern.** Never use nested `CASE WHEN winner = CASE WHEN white_id=...` — produced 150% win rates in the old app. Use explicit `(white_id=%s AND winner='white') OR (black_id=%s AND winner='black')` with `SELECT DISTINCT game_id` subquery to dedupe. See `serving/backend/coach.py`.
5. **Off-by-one on game board.** `moves[ply].fen` is the position **before** ply N. Use `chess.js`-derived `fenAfter(moves, ply)` to get the position **after** ply N. See `serving/frontend/app/game/[id]/page.tsx`.
6. **Cross-arch Docker builds on M-Mac via QEMU take 30+ min.** Build on the VPS (native amd64, ~2 min) via SSH instead.
7. **Disk pressure on k3s.** Evicted spark-worker pods accumulate. Periodic cleanup: `kubectl delete pod -n chess --field-selector=status.phase=Failed && k3s crictl rmi --prune`.

## Cross-account git push (this repo specifically)

User has two GitHub accounts: `khangdv-sonat` (default) and `vietnoy` (target for this repo). After `gh auth login` for both, push as vietnoy via:

```bash
~/git-push-as vietnoy main
```

Or `gh auth switch -u vietnoy && git push origin main`.

## How to resume on a new machine

1. Install: Claude Code, Codex CLI, Docker, kubectl, gh, Node 20, Python 3.13.
2. Clone this repo. Read `CLAUDE.md`, then this file.
3. `gh auth login` for both accounts. `gh auth setup-git`.
4. Copy VPS kubeconfig to `~/.kube/config` (or merge as a context).
5. Recreate `.env` from the secrets in k3s: `kubectl get secret chess-secrets -n chess -o yaml`.
6. `chmod +x ./codex-agent.sh`.
7. To resume frontend dev: `BACKEND_URL=http://160.187.0.108:30900 npm --prefix serving/frontend run dev`.

## Deferred / known-but-not-doing

- Backfill `analyze_blunders` for dates other than 2026-04-18. User said "we gonna backfill more data later" — do not initiate.
- Airflow scheduler doesn't auto-promote queued runs to running; manual `airflow tasks test` works. Root cause unknown.
- Optional polish: date filter on patterns, deep links to blunder ply, i18n toggle, auth.

## Notes on the workflow

- See `CLAUDE.md` (project root) and `~/.claude/CLAUDE.md` (user-level) for the Claude-as-architect / Codex-as-sidekick rules.
- Codex track record this session: 2/3 tasks clean (coach.py SQL fix, tests+CI). 1 had a perf bug (patterns endpoint full-table scan) — caught in inspection. Specs that touch large tables must state row counts.
