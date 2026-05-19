# Lichess Platform

Lichess Platform is a real-time chess analytics and coaching system. It collects
live games from Lichess, stores raw events in a lakehouse, evaluates critical
positions, and serves a coach experience grounded in the player's actual games.

The product goal is not a generic chess chatbot. The goal is a personalized
coach that detects recurring weaknesses, explains them with evidence, and turns
real mistakes into targeted exercises.

## Backbone

```text
Lichess API
-> systemd chess-ingestor
-> self-hosted Kafka
-> Spark kafka_to_minio
-> MinIO raw partitions
-> Spark process_to_polaris
-> Polaris / Iceberg analytical tables
-> StarRocks query layer
-> Coach API + single LLM agent with tools
-> dashboard, game review, exercises, training plan
```

## Current Infrastructure

- **Ingestion**: `/opt/chess/ingestion/stream_ingestor.py` runs as
  `chess-ingestor` on the VPS and writes to self-hosted Kafka.
- **Kafka topics**:
  - `lichess.game_start`
  - `lichess.game_end`
  - `lichess.moves`
- **Raw storage**: MinIO bucket/prefixes partitioned by `date`.
- **Processing**: Airflow triggers Spark jobs from `dags/chess_pipeline_dag.py`.
- **Lakehouse**: Polaris REST catalog with Iceberg tables in MinIO.
- **Serving**: StarRocks reads Iceberg through Polaris; backend/agent tools query
  serving tables.
- **Evaluation**: Stockfish analyzer writes move evaluations through Postgres
  staging, then compacts into Iceberg.

## Reliability First

Before adding more coach features, the platform needs explicit health checks:

- Kafka offsets are advancing.
- `chess-ingestor` is active and has recent logs.
- MinIO has today/yesterday partitions for all raw topics.
- Latest Airflow runs succeeded.
- Polaris row counts updated for recent dates.
- Analyzer staging backlog is bounded.
- Spark workers are healthy and not repeatedly evicted.

These checks should become a small script or Airflow DAG that fails loudly when
freshness breaks.

## Coach Data Model

The coach should be built on derived analytical tables, not raw LLM guesses.

Planned core tables:

- `critical_positions`
- `player_weakness_summary`
- `player_opening_stats`
- `player_phase_stats`
- `player_time_pressure_stats`
- `generated_exercises`
- `exercise_attempts`
- `coach_recommendations`

`critical_positions` is the most important table. It should contain one row per
teachable moment:

```text
player_id
game_id
ply
fen
played_move
best_move
eval_before
eval_after
eval_swing_cp
classification
phase
time_remaining
opening
motif
explanation
exercise_type
date
```

## Coach Product

The coach should focus on recurring patterns across many games.

Examples of useful diagnoses:

- Blunder rate by game phase.
- Mistake rate under time pressure.
- Weak openings by result and eval loss.
- Repeated tactical motifs: pins, forks, hanging pieces, back-rank issues.
- Endgame conversion failures.
- Rating-band comparison against similar players.

The coach should answer with evidence:

```text
In your last 64 evaluated blitz games, 18.4% of endgame moves lost more than
150 centipawns, compared with 9.7% in middlegames. Most of those mistakes
happened with less than 20 seconds remaining.
```

## Exercise System

Exercises should come from the player's own games:

- Find the best move.
- Avoid the blunder.
- Identify the opponent threat.
- Convert a winning endgame.
- Defend a worse position.
- Replay opening mistakes.

Exercise attempts should be stored so progress can be measured over time.

## Agent System

Start with one coach agent using safe tools. The LLM should explain and guide,
not invent facts or write arbitrary SQL.

Initial tools:

- `get_player_profile(player_id)`
- `get_weakness_summary(player_id)`
- `get_blunder_examples(player_id, filters)`
- `get_opening_stats(player_id)`
- `get_time_pressure_stats(player_id)`
- `get_game_review(game_id)`
- `get_exercise(player_id, weakness_type)`
- `submit_exercise_attempt(exercise_id, move)`

Rule: the agent may only make player-specific claims that are backed by tool
results.

Multi-agent architecture can wait. A single grounded coach agent is easier to
debug and strong enough for the MVP.

## Build Order

1. Add pipeline health checks.
2. Create `critical_positions`.
3. Create `player_weakness_summary`.
4. Implement safe coach tools.
5. Build the single LLM coach agent.
6. Generate exercises from critical positions.
7. Add dashboard, game review, exercise trainer, and training plan UI.
8. Add thesis/product metrics.

## Evaluation Metrics

- Pipeline freshness.
- Query latency.
- Percentage of coach claims backed by tool results.
- Stockfish agreement for mistake classification.
- Exercise completion and accuracy.
- Player trend: blunder rate, time-pressure performance, and repeated-pattern
  recurrence over time.

## Thesis Angle

The strongest positioning is:

```text
A real-time lakehouse-based chess coaching platform that detects personalized,
recurring weaknesses from large-scale game history and converts them into
targeted exercises.
```
