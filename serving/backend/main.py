"""
Chess Coach API.

Endpoints:
  GET  /healthz                       liveness + dependency status
  GET  /readyz                        readiness (StarRocks reachable)
  GET  /metrics                       prometheus text exposition
  GET  /api/freshness                 latest data partition + ingestion lag
  GET  /api/games/{id}                game moves + metadata
  POST /api/eval                      Stockfish proxy (single position)
  POST /api/whatif                    twin-line analysis (actual vs alt) batched
  GET  /api/players/{name}/profile    player dashboard data
  GET  /api/exercise/{player}         next drill position
  POST /api/coach                     agent SSE stream (rate-limited per session)
"""

import os
import json
import time
import logging
import threading
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, PlainTextResponse
from pydantic import BaseModel

from db import StarRocks, TABLE, query_game, query_player_profile, query_exercise
from stockfish import eval_fen

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("api")


# ─── metrics ──────────────────────────────────────────────────────────────────
class Metrics:
    """Tiny in-memory metrics. Single-pod scope; resets on restart. Good enough for now."""

    _lock = threading.Lock()
    _counts: dict[tuple[str, int], int] = defaultdict(int)
    _latencies_ms: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=500))
    _coach_429: int = 0

    @classmethod
    def record(cls, route: str, status: int, latency_ms: float) -> None:
        with cls._lock:
            cls._counts[(route, status)] += 1
            cls._latencies_ms[route].append(latency_ms)

    @classmethod
    def coach_throttled(cls) -> None:
        with cls._lock:
            cls._coach_429 += 1

    @classmethod
    def render(cls) -> str:
        # Snapshot under the lock so concurrent requests don't see torn reads.
        with cls._lock:
            counts = dict(cls._counts)
            latencies = {k: list(v) for k, v in cls._latencies_ms.items()}
            coach_429 = cls._coach_429
        lines: list[str] = []
        lines.append("# HELP http_requests_total Total HTTP requests by route and status")
        lines.append("# TYPE http_requests_total counter")
        for (route, status), n in sorted(counts.items()):
            lines.append(f'http_requests_total{{route="{route}",status="{status}"}} {n}')
        lines.append("# HELP http_request_latency_ms_p99 99th percentile request latency in ms (rolling 500 samples)")
        lines.append("# TYPE http_request_latency_ms_p99 gauge")
        for route, samples in sorted(latencies.items()):
            if not samples:
                continue
            sorted_s = sorted(samples)
            p99 = sorted_s[int(len(sorted_s) * 0.99)] if len(sorted_s) > 1 else sorted_s[0]
            p50 = sorted_s[len(sorted_s) // 2]
            lines.append(f'http_request_latency_ms_p99{{route="{route}"}} {p99:.2f}')
            lines.append(f'http_request_latency_ms_p50{{route="{route}"}} {p50:.2f}')
        lines.append("# HELP coach_throttled_total Number of /api/coach requests rate-limited (HTTP 429)")
        lines.append("# TYPE coach_throttled_total counter")
        lines.append(f"coach_throttled_total {coach_429}")
        return "\n".join(lines) + "\n"


# ─── coach rate limiter ───────────────────────────────────────────────────────
COACH_RATE_WINDOW_S = 60
COACH_RATE_LIMIT = int(os.getenv("COACH_RATE_LIMIT", "10"))
_coach_buckets: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=COACH_RATE_LIMIT))
_coach_lock = threading.Lock()


def coach_rate_check(session_id: str) -> bool:
    """Returns True if the request is allowed, False if throttled."""
    now = time.monotonic()
    cutoff = now - COACH_RATE_WINDOW_S
    with _coach_lock:
        bucket = _coach_buckets[session_id]
        # Drop expired entries.
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= COACH_RATE_LIMIT:
            return False
        bucket.append(now)
        return True


# ─── lifecycle ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    StarRocks.init()
    log.info("backend ready")
    yield
    StarRocks.close()


app = FastAPI(title="Chess Coach API", version="0.2.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    t0 = time.monotonic()
    response = await call_next(request)
    # Use route template (eg /api/games/{game_id}) not the literal path so cardinality stays bounded.
    route = request.scope.get("route")
    label = getattr(route, "path", request.url.path) if route else request.url.path
    Metrics.record(label, response.status_code, (time.monotonic() - t0) * 1000)
    return response


# ─── liveness / readiness / metrics ───────────────────────────────────────────
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    sr = StarRocks.healthy()
    if not sr:
        raise HTTPException(503, "starrocks unreachable")
    return {"starrocks": True}


@app.get("/metrics")
def metrics():
    return PlainTextResponse(Metrics.render(), media_type="text/plain; version=0.0.4")


# ─── freshness ────────────────────────────────────────────────────────────────
_freshness_cache: dict[str, tuple[float, dict]] = {}
_freshness_lock = threading.Lock()
_FRESHNESS_TTL_S = 300


@app.get("/api/freshness")
def get_freshness():
    """Latest partition date + ingestion lag from chess_move_events. Cached 5 min."""
    now = time.monotonic()
    with _freshness_lock:
        cached = _freshness_cache.get("v")
        if cached and now - cached[0] < _FRESHNESS_TTL_S:
            return cached[1]
    try:
        with StarRocks.cursor() as cur:
            cur.execute(f"SELECT MAX(date) AS max_date, COUNT(DISTINCT date) AS days FROM {TABLE}")
            row = cur.fetchone() or {}
        max_date = row.get("max_date")
        result = {
            "data_through": str(max_date) if max_date else None,
            "days_available": int(row.get("days") or 0),
        }
    except Exception as e:
        log.warning("freshness query failed: %s", e)
        result = {"data_through": None, "days_available": 0, "error": str(e)}
    with _freshness_lock:
        _freshness_cache["v"] = (now, result)
    return result


# ─── games ────────────────────────────────────────────────────────────────────
@app.get("/api/games/{game_id}")
def get_game(game_id: str):
    rows = query_game(game_id)
    if not rows:
        raise HTTPException(404, f"Game {game_id} not found")
    meta = rows[0]
    return {
        "game_id": game_id,
        "metadata": {
            "white_id": meta["white_id"],
            "black_id": meta["black_id"],
            "white_rating": meta["white_rating"],
            "black_rating": meta["black_rating"],
            "opening_eco": meta["opening_eco"],
            "opening_name": meta["opening_name"],
            "speed": meta["speed"],
            "winner": meta["winner"],
            "end_status": meta["end_status"],
        },
        "moves": [
            {
                "ply": r["move_number"],
                "side": r["whose_moved"],
                "san": r["move"],
                "fen": r["fen"],
                "clock_s": r["clock_s"],
            }
            for r in rows
        ],
    }


# ─── eval (single + batched whatif) ───────────────────────────────────────────
class EvalRequest(BaseModel):
    fen: str
    depth: int | None = None


@app.post("/api/eval")
def post_eval(req: EvalRequest):
    result = eval_fen(req.fen, req.depth)
    if result is None:
        raise HTTPException(503, "Stockfish unavailable")
    return result


class WhatIfRequest(BaseModel):
    base_fen: str
    actual_uci: str            # the move actually played in the game from base_fen
    alt_uci: str               # the user's alternative move from base_fen
    plies: int = 6             # how many plies to play forward in each line
    depth: int | None = None


@app.post("/api/whatif")
def post_whatif(req: WhatIfRequest):
    """Play forward two lines from base_fen — the actual game move and a user alternative —
    and return per-step FEN + eval for both. The frontend currently does this with N round-trips
    to /api/eval; this endpoint cuts it to one.
    """
    try:
        import chess  # python-chess for legal move replay
    except ImportError:
        raise HTTPException(500, "python-chess not installed in backend")

    if req.plies < 1 or req.plies > 12:
        raise HTTPException(400, "plies must be between 1 and 12")

    def play_line(first_uci: str) -> list[dict]:
        board = chess.Board(req.base_fen)
        steps: list[dict] = []
        for i in range(req.plies):
            uci = first_uci if i == 0 else None
            if uci is None:
                ev = eval_fen(board.fen(), req.depth)
                if not ev or not ev.get("best_move"):
                    break
                uci = ev["best_move"]
            try:
                move = chess.Move.from_uci(uci)
                if move not in board.legal_moves:
                    break
                board.push(move)
            except Exception:
                break
            ev_after = eval_fen(board.fen(), req.depth) or {"cp": None, "mate": None}
            steps.append({
                "uci": uci,
                "fen": board.fen(),
                "cp": ev_after.get("cp"),
                "mate": ev_after.get("mate"),
            })
        return steps

    return {
        "base_fen": req.base_fen,
        "plies": req.plies,
        "actual": play_line(req.actual_uci),
        "alt": play_line(req.alt_uci),
    }


# ─── player profile ───────────────────────────────────────────────────────────
@app.get("/api/players/{username}/profile")
def get_player_profile(username: str):
    profile = query_player_profile(username)
    if profile is None:
        raise HTTPException(404, f"No data for player '{username}'")
    return profile


# ─── exercise ────────────────────────────────────────────────────────────────
@app.get("/api/exercise/{username}")
def get_exercise(username: str):
    try:
        exercise = query_exercise(username)
    except Exception as e:
        log.warning("exercise query failed for %s: %s", username, e)
        raise HTTPException(503, "The blunder analyzer has not produced any drills yet for this player. Run the analyzer DAG to populate exercises.")
    if exercise is None:
        raise HTTPException(404, f"No exercises available for '{username}'")
    return exercise


# ─── coach ────────────────────────────────────────────────────────────────────
class CoachRequest(BaseModel):
    session_id: str
    message: str
    username: str | None = None
    reset: bool = False


@app.post("/api/coach")
def post_coach(req: CoachRequest):
    if not coach_rate_check(req.session_id):
        Metrics.coach_throttled()
        raise HTTPException(429, f"Rate limit: max {COACH_RATE_LIMIT} requests per {COACH_RATE_WINDOW_S}s. Slow down.")

    from coach import SESSIONS

    if req.reset:
        SESSIONS.reset(req.session_id)
    session = SESSIONS.get(req.session_id)
    msg = f"[Player: {req.username}] {req.message}" if req.username else req.message

    def gen():
        try:
            for event in session.ask_stream(msg):
                yield f"event: {event['type']}\ndata: {json.dumps(event)}\n\n"
        except Exception as e:
            log.exception("coach stream crashed")
            yield f"event: error\ndata: {json.dumps({'type':'error','message':str(e)})}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
