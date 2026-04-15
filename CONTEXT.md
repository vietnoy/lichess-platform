# Lichess Chess Analytics Platform — Production Codebase

## What this is

A chess analytics data platform (thesis project) that collects real-time game data from Lichess,
processes it through a Lakehouse stack, and serves an AI Chess Coach.

Student: Đỗ Vĩnh Khang — 20224865
Teacher meetings: Tuesdays 10AM

---

## Target Architecture

```
Lichess API
  → ingestion/etl.py          (collect + transform)
  → Confluent Kafka            (3 topics, cloud)
  → storage/minio staging      (raw parquet)
  → processing/transform.py    (clean + enrich)
  → storage/minio prod         (clean parquet, partitioned)
  → StarRocks                  (OLAP query layer)
  → serving/agent.py           (AI Chess Coach — Gemini 2.5 Flash tool use)
```

---

## Folder Structure

```
lichess-platform/
├── ingestion/
│   └── etl.py          ← Lichess API → Kafka (DONE, copy from lichess-test)
├── storage/
│   └── (MinIO config, schemas)
├── processing/
│   └── (Spark/transform jobs)
├── serving/
│   └── (AI agent, tools, API)
├── infra/
│   └── docker-compose.yml   ← MinIO + StarRocks + Kafka UI
└── docs/
    └── (Vietnamese docs for teacher)
```

---

## What is already built (in /Users/khangdo/Desktop/lichess-test/)

| File | Status | Notes |
|------|--------|-------|
| etl.py | Done | Lichess → Kafka, tested live |
| consumer.py | Done | Kafka → PostgreSQL (demo only) |
| seed.py | Done | Generates realistic sample data |
| docker-compose.yml | Partial | PostgreSQL only, needs MinIO + StarRocks |

Copy etl.py into ingestion/ as starting point.

---

## Kafka Topics (Confluent Cloud)

| Topic | Partitions | Key | Content |
|-------|------------|-----|---------|
| lichess.game_start | 6 | game_id | Game metadata |
| lichess.moves | 6 | game_id | Each move with derived fields |
| lichess.game_end | 6 | game_id | Winner + status |

Credentials in .env (never commit):
- KAFKA_BOOTSTRAP_SERVERS
- CLUSTER_API_KEY
- CLUSTER_API_SECRET
- LICHESS_TOKEN

---

## Data Schema (PostgreSQL demo → will move to StarRocks)

### players
player_id (PK), title, last_seen_at, created_at

### games
game_id (PK), white_id, black_id, white_rating, black_rating, white_title, black_title,
speed, rated, variant, source, tournament_id, opening_eco, opening_name,
winner, status, started_at, ended_at, total_moves

### moves
id (PK), game_id (FK), move_number, player_id (FK), move_uci, fen_after,
white_clock, black_clock, time_spent_s, time_pressure, game_phase,
is_check, is_capture, played_at

---

## AI Chess Coach Design

Agent (not simple RAG) using Gemini 2.5 Flash (Vertex AI) tool use:

Tools the agent has:
- get_player_stats(player_id)
- get_weak_phases(player_id)
- get_opening_performance(player_id)
- get_similar_players(rating_band)
- get_game_replay(game_id)

Agent reasons over tool results to give personalized coaching.

### Output Metrics
- Factual Accuracy: AI stat vs actual DB query (target >90%)
- Diagnosis Precision/Recall: vs Stockfish ground truth
- Groundedness: % claims backed by real data
- User Improvement Rate: rating change after following advice

---

## Deployment Status (as of 2026-04-14)

### VPS: 160.187.0.108 (chessanalytics, Vietnix VPS 5+)
- k3s v1.34.6 installed, namespace: `chess`
- All pods healthy, stable for 7+ hours
- CPU: ~2% idle | RAM: 5.6GB / 10GB (56%)

### Running pods
| Pod | Status |
|-----|--------|
| etl | Running — streaming Lichess → Kafka 24/7 |
| kafka-to-minio | Running — flushing to MinIO every 270s |
| minio | Running — buckets: chess-raw, chess-enriched |
| polaris | Running — Iceberg REST catalog |
| starrocks-fe | Running |
| starrocks-cn | Running |
| postgres | Running — airflow_db + polaris_db |
| airflow (apiserver, scheduler, triggerer, dag-processor) | Running |

### Data in MinIO (chess-raw) as of 2026-04-15 ~00:00 UTC
- game_start: 11 records
- moves: 721 records
- game_end: 10 records
- Accumulating every ~4.5 min, will grow significantly overnight

### Airflow UI
- URL: http://160.187.0.108:30808
- Login: admin / see infra/.env AIRFLOW_ADMIN_PASSWORD

### Known fixes applied
- etl.py: removed max_retries cap (was 5) → now retries forever
- etl.py: logging level DEBUG → INFO
- kafka_to_minio.py: removed invalid commit-on-empty-buffer call

## Next Steps (priority order)

1. ~~Buy VPS + install k3s~~ ✅ Done
2. ~~Write k8s manifests (MinIO, Polaris, StarRocks, Airflow)~~ ✅ Done
3. ~~ingestion/ — Kafka → MinIO writer (raw parquet)~~ ✅ Done, running live
4. Set up Polaris catalog + register Iceberg tables pointing at MinIO
5. Configure StarRocks external catalog via Polaris
6. processing/annotate.py — wire up Airflow DAG, run on schedule
7. processing/transform.py — aggregate patterns per player into StarRocks
8. serving/agent.py — Gemini 2.5 Flash (Vertex AI) tool use agent (AI coach)
9. Demo by Monday 21/04/2026

---

## Architecture Decisions (from planning session 2026-04-14)

### Deployment
- **VPS provider**: Vietnix (Vietnamese), VPS 5+ plan (~$19/month)
  - 10GB RAM, 5 vCPU AMD, 80GB NVMe, 1 public IPv4
- **k8s**: k3s (lightweight certified k8s, single binary, single node)
  - Same kubectl/Helm/manifests as full k8s — academically correct
- **No Docker needed on VPS** — k3s bundles containerd runtime
- **OS**: Ubuntu 22.04 LTS

### Storage
- **MinIO on VPS** (not local) — so raw data accumulates from day one, no migration later
- **StarRocks in shared-data mode** — CN nodes are stateless, read parquet from MinIO
  - FE pod: ~1GB RAM (metadata + query planning)
  - CN pod: ~1GB RAM (query execution, reads MinIO over network)
- **Parquet compression**: zstd (best balance of size and speed)
  - 30GB .zst PGN → ~14GB Parquet (zstd) after conversion

### Chess Evaluation
- **Stockfish role**: labeling engine — turns raw moves into evaluated moves
  - Produces: eval_delta (centipawns), classification (blunder/mistake/good), best_move_uci
  - Runs in processing layer (annotate.py), NOT at query time in the agent
- **Use Lichess Cloud Eval API** for now (zero setup, free, rate-limited but generous):
  - `GET https://lichess.org/api/cloud-eval?fen=<FEN>&multiPv=1`
  - Design annotate.py to swap in local Stockfish later via one function swap
- **No fine-tuning Stockfish** — use personalized thresholds per player instead:
  - Classify mistakes relative to player's own historical baseline, not universal thresholds
  - Train a lightweight classifier on top of Stockfish eval (Option B, future work)

### Processing
- **Real-time pipeline** (thesis demo): Python + pandas in annotate.py
  - etl.py → Kafka → MinIO (raw) → annotate.py → MinIO (enriched) → StarRocks
- **Batch pipeline** (future, Lichess monthly dumps): Apache Spark
  - 30GB .zst PGN takes hours to parse in Python — Spark required at that scale
  - Mention in thesis as production strategy, not required for demo
- **Spark NOT deployed for demo** — saves 2-3GB RAM on VPS

### Lichess Public DB (tested 2026-04-14)
- Files: `lichess_db_standard_rated_YYYY-MM.pgn.zst` (~30GB/month compressed)
- 50MB sample → 359MB raw PGN → 162,484 games parsed
- Compression result: 30GB → ~14GB Parquet (zstd), ~20GB Parquet (snappy)
- Also available: `lichess_db_eval.jsonl.zst` — 369M positions pre-evaluated by Stockfish (huge shortcut)
- Parsing speed in Python: ~5 min per 50MB → Spark needed for full monthly files

### AI Coach Design (clarified)
- Stockfish is step 1 (ground truth labels), everything else is step 2
- Cross-game pattern detection is the key thesis contribution — not single game analysis
- Example query the coach answers:
  > "You blunder 3x more often in time pressure during endgame with <10s on clock in rook endgames"
- Agent uses Claude tool use over StarRocks SQL queries — NOT RAG, NOT fine-tuning

---

## Known Issues to Fix

- consumer.py: player_id derived from move_number parity — fragile (low priority, consumer.py is demo-only)
