-- chess_analyzer_db schema.
-- Three tables for the continuous on-demand Stockfish analyzer.
-- Idempotent: safe to run on every container start.

CREATE TABLE IF NOT EXISTS analyzer_cursor (
  player_id            TEXT        PRIMARY KEY,
  last_game_id         TEXT,
  last_game_date       DATE,
  last_processed_at    TIMESTAMPTZ,
  games_processed      INT         NOT NULL DEFAULT 0,
  -- skip this player until this time (e.g. 24h throttle after a successful pass).
  -- NULL = eligible immediately. Workers ORDER BY this ASC NULLS FIRST.
  throttle_until       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS analyzer_cursor_throttle_idx
  ON analyzer_cursor (throttle_until NULLS FIRST);

-- Global FEN -> eval cache. Keys on FEN so two different games arriving at the
-- same opening position share the result. Hit rate is ~30-50% on a typical game
-- because openings repeat constantly.
CREATE TABLE IF NOT EXISTS position_evals (
  fen           TEXT        PRIMARY KEY,
  cp            INT,
  mate          INT,
  best_move     TEXT,
  depth         INT         NOT NULL,
  evaluated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Output table for on-demand classifications. The daily DAG writes to a separate
-- Iceberg table (prod.move_evaluations) which we don't touch — that table uses
-- overwritePartitions() and would clobber per-row writes.
CREATE TABLE IF NOT EXISTS move_evaluations_ondemand (
  game_id              TEXT        NOT NULL,
  ply                  INT         NOT NULL,
  player_id            TEXT        NOT NULL,
  date                 DATE,
  fen                  TEXT,
  played_move          TEXT,
  best_move            TEXT,
  eval_cp              INT,
  mate                 INT,
  eval_swing_cp        INT,
  classification       TEXT,
  evaluated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, ply, player_id)
);
ALTER TABLE move_evaluations_ondemand
  ADD COLUMN IF NOT EXISTS date DATE;
-- /api/exercise/{player} hits this index: filter by player, fetch blunders.
CREATE INDEX IF NOT EXISTS move_eval_ondemand_player_class_idx
  ON move_evaluations_ondemand (player_id, classification)
  WHERE classification IN ('blunder', 'mistake');
CREATE INDEX IF NOT EXISTS move_eval_ondemand_dated_order_idx
  ON move_evaluations_ondemand (evaluated_at, game_id, ply, player_id)
  WHERE date IS NOT NULL;
CREATE INDEX IF NOT EXISTS move_eval_ondemand_date_key_idx
  ON move_evaluations_ondemand (date, game_id, ply, player_id)
  WHERE date IS NOT NULL;
