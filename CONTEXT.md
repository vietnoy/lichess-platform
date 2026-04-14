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
  → serving/agent.py           (AI Chess Coach — Claude API tool use)
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

Agent (not simple RAG) using Claude API tool use:

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

## Next Steps (priority order)

1. infra/docker-compose.yml — add MinIO + StarRocks
2. ingestion/ — add Kafka → MinIO writer (raw parquet)
3. processing/ — transform job staging → prod
4. StarRocks external table on MinIO prod
5. serving/agent.py — Claude API tool use agent
6. Demo by Monday 21/04/2026

---

## Known Issues to Fix

- etl.py: max_retries=5 in _stream_batch — remove cap
- etl.py: logging level DEBUG — change to INFO for production
- consumer.py: player_id derived from move_number parity — fragile
