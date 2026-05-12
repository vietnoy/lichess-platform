# On-demand Stockfish analyzer — implementation plan

Status as of 2026-05-12: schema committed (`45cfead`), code pending.
Pick up from here on the other machine.

## Goal

Today the daily `analyze_blunders` DAG can only analyze the top 5 most-active
players per date. That covers ~5 humans/day; the `/api/exercise/{player}`
endpoint returns 404 for ~99.99% of users.

Replace it with a **continuous background worker** that:

- iterates through every player in `player_games`
- for each player, processes new games since the last cursor position
- caches positions by FEN so repeat openings skip Stockfish
- writes evaluations through Postgres staging → daily Iceberg compaction
- never blocks the data pipeline (strict resource limits)

Median user has 7 games. At current Stockfish throughput (~20 evals/s,
single replica, 1 core, depth 12) the median user finishes in ~14s.
The worker just keeps walking the player list at a polite pace.

## Architecture

```
                                                         ┌──────────────────┐
                                                         │ Stockfish (1×1c) │
                                                         └─────────▲────────┘
                                                                   │ ~20/s
                                                                   │
  ┌────────────────────────────────────────────────────────────────┴───────┐
  │ chess-analyzer-worker  (1 pod, cpu 200m/500m, mem 256Mi, sleeps)       │
  │                                                                       │
  │  loop forever:                                                        │
  │    SELECT player_id FROM analyzer_cursor                              │
  │      ORDER BY throttle_until ASC NULLS FIRST LIMIT 5                  │
  │    for each player:                                                   │
  │      games = SELECT game_id, date FROM polaris_catalog.prod.          │
  │              player_games WHERE player_id=?                           │
  │              AND (date,game_id) > (last_game_date,last_game_id)       │
  │              ORDER BY date DESC, game_id LIMIT 20                     │
  │      for each game:                                                   │
  │        plies = SELECT move_number, fen, whose_moved, move             │
  │                FROM polaris_catalog.prod.chess_move_events            │
  │                WHERE game_id=? ORDER BY move_number                   │
  │        for each ply:                                                  │
  │          eval = position_evals[fen] or stockfish_eval(fen)            │
  │          upsert position_evals (fen,cp,mate,best_move,depth)          │
  │        classify each move by cp swing (mover's perspective)           │
  │        INSERT batch into move_evaluations_ondemand                    │
  │      UPDATE analyzer_cursor SET last_game_id=…, throttle_until=now+24h│
  │    sleep 30s                                                          │
  └────────────────┬──────────────────────────────────────────────────────┘
                   │ INSERT (continuous)
                   ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │  Postgres: chess_analyzer_db                                         │
   │    analyzer_cursor             (per-user watermark)                  │
   │    position_evals              (FEN -> eval cache, global)           │
   │    move_evaluations_ondemand   (staging, ~24h ring buffer)           │
   └────────────────┬─────────────────────────────────────────────────────┘
                    │ daily 03:00 UTC (Spark task)
                    │ processing/compact_ondemand_evals.py
                    ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │  Iceberg: polaris.prod.move_evaluations_ondemand   (MinIO)           │
   │   - one snapshot per day                                             │
   │   - permanent home                                                   │
   │   - StarRocks reads via polaris_catalog                              │
   └────────────────┬─────────────────────────────────────────────────────┘
                    │ /api/exercise/{player} UNION ALL
                    ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │  prod.move_evaluations               (daily DAG, top-5 players)      │
   │  prod.move_evaluations_ondemand      (this worker, everyone else)    │
   └──────────────────────────────────────────────────────────────────────┘
```

## Decisions (and what was rejected)

**Why Postgres staging instead of direct PyIceberg writes from the worker?**
PyIceberg `append()` has ~1-3s of commit overhead per call regardless of batch
size, would produce 50-100 snapshots/day requiring periodic `expire_snapshots`
cleanup, and bloats the worker image by ~150 MB (pyarrow + s3fs). Postgres
`INSERT` is ~50ms per 500-row batch, holds <100 MB at peak (cleared nightly),
and the worker image stays at ~80 MB. Final storage in MinIO is preserved
via the daily compaction job. See conversation transcript for the full
A-vs-B-vs-C comparison.

**Why a separate `move_evaluations_ondemand` Iceberg table instead of writing
into the existing `move_evaluations`?** The daily `analyze_blunders` DAG uses
`overwritePartitions()` on `move_evaluations`, which would clobber any
per-row ondemand work in the same partition. Keeping them separate makes the
two writers independent. The exercise endpoint unions both.

**Why throttle_until = now + 24h?** A user's "what I want from the analyzer"
is: pick up my new games. 24h is a fine recheck cadence — they don't play
faster than the rest of the pipeline (process_to_polaris runs daily at
01:15 UTC, so move_events for "today" don't even exist until tomorrow).

**Why cap at 20 games per cycle per user?** Per-user fairness. If we let one
user with 1,000 unprocessed games hog the worker, every other user waits.
20 × ~40 plies = 800 evals = ~40s/user with current Stockfish. After 24h
of throttle, we come back and grab the next 20.

**Bot/inactive filters.** Skip `player_id ~ '(?i)bot|stockfish|maia|leela'`
and skip users whose `last_game_date` is more than 7 days old. Reduces
noise; these don't benefit from the exercise feature anyway.

## Files to create (in order)

| # | File | Purpose |
|---|---|---|
| 1 | `services/analyzer/schema.sql` | ✅ done. Already applied to Postgres. |
| 2 | `services/analyzer/PLAN.md` | ✅ this file. |
| 3 | `services/analyzer/worker.py` | Main loop. ~200 LOC. |
| 4 | `services/analyzer/stockfish.py` | Copy from `serving/backend/stockfish.py`. Same client, no changes. |
| 5 | `services/analyzer/requirements.txt` | psycopg2-binary, mysql-connector-python, requests, python-chess |
| 6 | `services/analyzer/Dockerfile` | python:3.13-slim, install reqs, COPY worker + stockfish.py, CMD python worker.py |
| 7 | `services/analyzer/bootstrap.sql` | One-shot INSERT from StarRocks → Postgres `analyzer_cursor`. Filters: `games >= 5`, last_game within 7d, not bot pattern. Run once after first deploy. |
| 8 | `infra/k8s/analyzer.yaml` | Deployment, 1 replica, cpu 200m/500m, mem 256Mi, envFrom chess-secrets + chess-config. |
| 9 | `processing/compact_ondemand_evals.py` | Spark job. Read from Postgres (jdbc connector), append to `polaris.prod.move_evaluations_ondemand` Iceberg, then DELETE the Postgres rows that succeeded. |
| 10 | `dags/chess_pipeline_dag.py` | Add `run_compact_ondemand_evals` task downstream of `run_build_player_games`. Schedule shares the daily 01:15 UTC slot. |
| 11 | `serving/backend/db.py` | Update `query_exercise` to `UNION ALL` between `polaris_catalog.prod.move_evaluations` and `polaris_catalog.prod.move_evaluations_ondemand`. |

## Schema reference

Postgres `chess_analyzer_db` (already applied):

```sql
analyzer_cursor (
  player_id        TEXT PK,
  last_game_id     TEXT,
  last_game_date   DATE,
  last_processed_at TIMESTAMPTZ,
  games_processed  INT DEFAULT 0,
  throttle_until   TIMESTAMPTZ        -- NULL = process immediately
)
-- INDEX on throttle_until (NULLS FIRST)

position_evals (
  fen          TEXT PK,
  cp           INT,
  mate         INT,
  best_move    TEXT,
  depth        INT NOT NULL,
  evaluated_at TIMESTAMPTZ DEFAULT now()
)

move_evaluations_ondemand (              -- staging
  game_id        TEXT NOT NULL,
  ply            INT NOT NULL,
  player_id      TEXT NOT NULL,
  fen            TEXT,
  played_move    TEXT,
  best_move      TEXT,
  eval_cp        INT,
  mate           INT,
  eval_swing_cp  INT,
  classification TEXT,
  evaluated_at   TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (game_id, ply, player_id)
)
-- INDEX on (player_id, classification) WHERE classification IN ('blunder','mistake')
```

Iceberg `polaris.prod.move_evaluations_ondemand` (to create in the
compaction Spark job — same shape as `prod.move_evaluations` plus a
`player_id` column):

```sql
CREATE TABLE IF NOT EXISTS polaris.prod.move_evaluations_ondemand (
  game_id        STRING NOT NULL,
  ply            INT    NOT NULL,
  player_id      STRING NOT NULL,
  date           DATE   NOT NULL,
  fen            STRING,
  played_move    STRING,
  best_move      STRING,
  eval_cp        INT,
  mate           INT,
  eval_swing_cp  INT,
  classification STRING,
  evaluated_at   TIMESTAMP
)
USING iceberg
PARTITIONED BY (date)
```

## Worker pseudocode (the loop)

```python
import os, time, json, requests, psycopg2, chess
import mysql.connector

PG = psycopg2.connect(...)         # chess_analyzer_db
SR = mysql.connector.connect(...)  # starrocks 9030
STOCKFISH = os.environ["STOCKFISH_URL"]
BATCH_USERS  = 5
BATCH_GAMES  = 20
SLEEP_S      = 30
THROTTLE_H   = 24

def eval_with_cache(fen):
    with PG.cursor() as c:
        c.execute("SELECT cp, mate, best_move FROM position_evals WHERE fen=%s", (fen,))
        row = c.fetchone()
    if row: return {"cp": row[0], "mate": row[1], "best_move": row[2]}
    r = requests.get(STOCKFISH, params={"fen": fen, "depth": 12}, timeout=30).json()
    with PG.cursor() as c:
        c.execute("""INSERT INTO position_evals (fen,cp,mate,best_move,depth)
                     VALUES (%s,%s,%s,%s,12) ON CONFLICT (fen) DO NOTHING""",
                  (fen, r.get("cp"), r.get("mate"), r.get("best_move")))
    PG.commit()
    return r

def classify(cp_before, cp_after, mover):
    if None in (cp_before, cp_after): return None, None
    drop = (cp_before - cp_after) if mover == "white" else (cp_after - cp_before)
    swing = -drop if mover == "white" else drop
    label = ("blunder" if drop >= 200 else "mistake" if drop >= 100
             else "inaccuracy" if drop >= 50 else "good")
    return swing, label

def cycle():
    with PG.cursor() as c:
        c.execute("""SELECT player_id, last_game_id, last_game_date
                     FROM analyzer_cursor
                     WHERE throttle_until IS NULL OR throttle_until < now()
                     ORDER BY throttle_until ASC NULLS FIRST LIMIT %s""", (BATCH_USERS,))
        targets = c.fetchall()

    for pid, last_gid, last_date in targets:
        cur = SR.cursor(dictionary=True)
        cur.execute(f"""
            SELECT game_id, date FROM polaris_catalog.prod.player_games
            WHERE player_id=%s
              AND (date > %s OR (date = %s AND game_id > %s))
            ORDER BY date DESC, game_id LIMIT %s
        """, (pid, last_date or "1900-01-01", last_date or "1900-01-01",
              last_gid or "", BATCH_GAMES))
        games = cur.fetchall()

        for g in games:
            cur.execute(f"""
                SELECT move_number, fen, whose_moved, move
                FROM polaris_catalog.prod.chess_move_events
                WHERE game_id=%s ORDER BY move_number
            """, (g["game_id"],))
            plies = cur.fetchall()

            # First pass: get cp_before for every ply
            evals = []
            for p in plies:
                e = eval_with_cache(p["fen"])
                evals.append(e)

            # Second pass: classify by swing to next ply's cp
            rows_to_insert = []
            for i, p in enumerate(plies):
                cp_b = evals[i].get("cp")
                cp_a = evals[i+1].get("cp") if i+1 < len(evals) else None
                swing, label = classify(cp_b, cp_a, p["whose_moved"])
                rows_to_insert.append((
                    g["game_id"], p["move_number"], pid, p["fen"], p["move"],
                    evals[i].get("best_move"), cp_b, evals[i].get("mate"),
                    swing, label
                ))

            with PG.cursor() as c:
                c.executemany("""
                    INSERT INTO move_evaluations_ondemand
                      (game_id, ply, player_id, fen, played_move, best_move,
                       eval_cp, mate, eval_swing_cp, classification)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (game_id, ply, player_id) DO NOTHING
                """, rows_to_insert)
            PG.commit()

        # Update cursor for this user
        if games:
            top = games[0]   # newest
            with PG.cursor() as c:
                c.execute("""
                    UPDATE analyzer_cursor
                    SET last_game_id=%s, last_game_date=%s,
                        last_processed_at=now(),
                        games_processed=games_processed+%s,
                        throttle_until=now() + interval '%s hours'
                    WHERE player_id=%s
                """, (top["game_id"], top["date"], len(games), THROTTLE_H, pid))
            PG.commit()

while True:
    try: cycle()
    except Exception as e: log.exception("cycle failed: %s", e)
    time.sleep(SLEEP_S)
```

## Bootstrap (run once after deploy)

```sql
-- Populate analyzer_cursor from current player_games.
-- Filters: 5+ games, last_game within 7d, not obviously a bot.

INSERT INTO analyzer_cursor (player_id, games_processed, throttle_until)
SELECT player_id, 0, NULL
FROM (
  -- Run this against StarRocks first, dump CSV, then COPY into Postgres,
  -- OR do it via Spark/JDBC. There's no direct Postgres -> StarRocks
  -- federation; copy explicitly.
  SELECT player_id, MAX(date) AS last_d, COUNT(*) AS games
  FROM polaris_catalog.prod.player_games
  WHERE player_id NOT REGEXP '(?i)bot|stockfish|maia|leela'
  GROUP BY player_id
  HAVING COUNT(*) >= 5 AND MAX(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
) t
ON CONFLICT (player_id) DO NOTHING;
```

Realistically: ~50K-100K eligible players. Even if the worker only touches
20 per cycle and sleeps 30s, that's `100K / 20 * 30s / 86400s = ~1.7 days`
to make one full pass — which is fine because throttle_until=24h means
each user gets visited about once a day. Steady state.

## Resource budget verification

- Stockfish service: 1 replica × 1 core, capped at ~20 evals/s
- Worker: 200m request / 500m limit, 256 MiB memory
- Postgres: holds ≤ 500 MB peak (cleared nightly)
- Iceberg: 1 new commit per day for `move_evaluations_ondemand`

Cluster has ~9.5 GiB / 6 cores of headroom. Worker uses ~0.3 GiB and
~0.5 cores under load. Trivial.

## Acceptance criteria

1. After deploy, `kubectl logs deploy/chess-analyzer-worker` shows
   "cycle complete: N users processed, M rows written" every ~30s.
2. `SELECT COUNT(*) FROM analyzer_cursor` shows ~80K rows after bootstrap.
3. `SELECT COUNT(*) FROM position_evals` grows steadily (cache filling).
4. `SELECT COUNT(*) FROM move_evaluations_ondemand` grows for ~24h then
   drops to 0 after the 03:00 UTC Spark compaction runs.
5. `SELECT COUNT(*) FROM polaris_catalog.prod.move_evaluations_ondemand`
   in StarRocks grows by yesterday's drop after each compaction.
6. `GET /api/exercise/diamonddoll` still returns 200 (existing daily-DAG
   data unaffected).
7. `GET /api/exercise/SomeRandomNewUser` returns 200 (after the worker
   has had a chance to process them; ~minutes to hours).

## Where to start when you come back

1. `services/analyzer/worker.py` — translate the pseudocode above.
2. `services/analyzer/Dockerfile` + `requirements.txt`.
3. `infra/k8s/analyzer.yaml` — copy the structure from `serving/backend`
   deployment, change image name and resources.
4. Build, push, deploy.
5. Run bootstrap once (manual psql or a separate Job).
6. Watch logs for a cycle or two.
7. Then the compaction Spark job + DAG task + backend UNION.

Everything required for the "is it working" check is in Acceptance Criteria
above. If anything is unclear from the schema or pseudocode, re-read the
conversation transcript at the path noted in your memory.
