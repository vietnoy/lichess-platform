# Session resume state — 2026-05-10

Working notes so a fresh Claude Code session can pick up cleanly.

## What's shipped (uncommitted) in the working tree

### Frontend (`serving/frontend/`)
- `app/page.tsx` — home redesign: bigger hero, helper text under inputs, both buttons primary, header rendered, favicon
- `app/icon.svg` — knight glyph favicon (kills 404 on `/favicon.ico`)
- `app/player/[name]/page.tsx` — 7 fixes: top-openings label fix, clickable rows, white/black contrast, draws KPI, vs-rating games count, Player subtitle, opening name truncation
- `app/game/[id]/page.tsx` — board off-by-one fix using chess.js, last-move highlight, eval/arrow polish
- `app/whatif/[id]/[ply]/page.tsx` — actual-move off-by-one fix, "Awaiting input" idle state, helper text moved up, **wired to new `/api/whatif`** batched endpoint
- `app/coach/page.tsx` — react-markdown rendering for AI responses, example player updated to `temporalmente`
- `components/Header.tsx` — fetches `/api/freshness` and shows "data through YYYY-MM-DD" subtle text on lg+ screens
- `components/EvalBar.tsx` — bigger signed eval text (+0.3 not 0.3)
- `tailwind.config.ts` — added `@tailwindcss/typography` plugin
- `package.json` — added `react-markdown`, `remark-gfm`, `@tailwindcss/typography`

### Backend (`serving/backend/`)
- `main.py` — added `/metrics` (prometheus text), `/api/freshness` (cached 5m), `/api/whatif` (batched twin-line), coach rate limit (10 req / 60s per session_id), middleware that records request latency
- `coach.py` — Codex fixed all 6 SQL functions: replaced nested `CASE WHEN winner = CASE WHEN white_id=...` with explicit `(white_id=%s AND winner='white') OR (black_id=%s AND winner='black')`, wrapped each in `SELECT DISTINCT game_id` subquery to dedupe leftover duplicate rows
- `requirements.txt` — added `pytest==8.3.3`, `chess==1.11.1`
- `tests/` — Codex created: `__init__.py`, `conftest.py`, `test_metrics.py`, `test_rate_limit.py`, `test_whatif.py`, `test_stockfish.py`

### CI
- `.github/workflows/ci.yml` — backend pytest + frontend `npm run build` on push & PR

### Docs
- `docs/operations/backups.md` — backup story (3 tiers: secrets/postgres, MinIO bulk, k3s state) with cron snippets
- `docs/operations/session-state.md` — this file

## What's done but not deployed

The dev server (npm run dev) ran on localhost:3000 with `BACKEND_URL=http://160.187.0.108:30900`. Everything was visually verified locally. **The VPS still runs the old images.**

## Todo to actually ship

1. **Build + push backend image:** `docker build --platform linux/amd64 -t vietnoy/chess-webapp-backend:latest serving/backend/` then push, then `kubectl scale deployment webapp-backend -n chess --replicas=0 && sleep 3 && --replicas=1`
2. **Build + push frontend image:** same dance for `vietnoy/chess-webapp-frontend:latest`. Note: cross-compile via QEMU on Mac is 10–30 min; consider building on the VPS instead.
3. **Verify on VPS:** snapshot http://160.187.0.108:30900 with Playwright MCP (browser tools listed below).
4. **Commit:** several conventional commits per change-area (feat(webapp): ..., feat(backend): ..., test(backend): ..., chore(ci): ..., docs(ops): ...).

## Still on the punchlist (NOT done)

| # | Item | Why deferred |
|---|------|--------------|
| 1 | Pattern Analysis dashboard `/patterns/[name]` | Depends on `move_evaluations` table; DAG hasn't run successfully |
| 2 | Game features 2/3/4 (eval chart, AI Analyze button, move quality coloring) | Same dependency |
| 3 | `analyze_blunders` DAG runs are stuck in `queued` state | Separate Airflow scheduler issue — needs investigation |
| 4 | Mobile responsive sweep across all routes | Home + player verified OK; remaining 4 routes unchecked |
| 5 | (none) | — |

## How to resume locally

```bash
# Restart dev server with backend proxy
cd serving/frontend && BACKEND_URL=http://160.187.0.108:30900 npm run dev

# Then point Playwright MCP browser at http://localhost:3000
```

## Live URLs

- Frontend (live VPS): http://160.187.0.108:30900
- Streamlit (legacy coach): http://160.187.0.108:30051
- Airflow: http://160.187.0.108:30808 (admin / `kubectl get secret chess-secrets -n chess -o jsonpath='{.data.AIRFLOW_ADMIN_PASSWORD}' | base64 -d`)
- StarRocks MySQL: `mysql -h 160.187.0.108 -P 30930 -u root` (no password after the recent chess-secrets reset)

## Test players known to have data

- `temporalmente` — 64 games across speeds, used as canonical example everywhere
- A real game ID: `aqXZphC1`
