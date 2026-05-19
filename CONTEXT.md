# Lichess Platform Context

This file is the short operational handoff. The product and architecture
backbone lives in `README.md`.

## Current Direction

Build a real-time chess coach platform on top of the existing lakehouse:

```text
Lichess -> chess-ingestor -> Kafka -> MinIO -> Spark -> Polaris/Iceberg
-> StarRocks -> coach tools -> single grounded LLM coach
```

The coach should detect recurring player weaknesses from evaluated game history
and turn those weaknesses into targeted exercises.

## Current Live Infrastructure

- VPS: `160.187.0.108`
- Kubernetes namespace: `chess`
- Systemd producer: `chess-ingestor`
- Producer path on host: `/opt/chess/ingestion/stream_ingestor.py`
- Kafka bootstrap from host: `160.187.0.108:30092`
- Kafka bootstrap inside cluster: `kafka:9092`
- Raw MinIO prefixes:
  - `chess-dev/game_start/date=YYYY-MM-DD`
  - `chess-dev/game_end/date=YYYY-MM-DD`
  - `chess-dev/moves/date=YYYY-MM-DD`

## Airflow DAGs

- `kafka_to_minio`: Spark Structured Streaming micro-batch into MinIO.
- `process_to_polaris`: raw MinIO partitions into Polaris/Iceberg analytical
  tables, then player game rebuild, then on-demand eval compaction.
- `init_catalog_starrocks`: creates/refreshes StarRocks external catalog over
  Polaris.

## Immediate Priorities

1. Add explicit pipeline health checks.
2. Build `critical_positions`.
3. Build `player_weakness_summary`.
4. Expose safe coach tools over StarRocks/Postgres.
5. Build one LLM coach agent with grounded tool use.
6. Generate exercises from real player mistakes.

## Health Check Command

Run the first read-only pipeline health check from the repo root:

```bash
python ops/pipeline_health.py
```

It checks Kafka offset growth, fresh MinIO raw partitions, recent Airflow DAG
success, StarRocks visibility for `chess_move_events` and `player_games`, Spark
worker availability, and analyzer staging backlog.

## Operational Lessons

- A live process can still fail silently. Check Kafka offsets, not only service
  status.
- Missing Kafka data cannot be recovered after retention expires unless another
  source still has it.
- Keep the host ingestor and repo version of `ingestion/stream_ingestor.py` in
  sync.
- `process_to_polaris.py` maps a same-day scheduled run to yesterday's raw
  partition; `build_player_games.py` must use the same date resolution or the
  derived `player_games` table becomes stale while the DAG still reports
  success.
- StarRocks FE restarts can drop CN registration and external catalog state.
  The CN deployment has a re-registration loop, and `process_to_polaris` now
  refreshes the StarRocks catalog after writing Polaris tables.
- Rotate credentials that were pasted into chat or local notes.
