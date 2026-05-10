"""
Chess Coach API.

Endpoints:
  GET  /healthz                       liveness + dependency status
  GET  /api/games/{id}                game moves + metadata
  POST /api/eval                      Stockfish proxy (single position)
  GET  /api/players/{name}/profile    player dashboard data
  POST /api/coach                     agent SSE stream
  GET  /api/exercise/{player}         next drill position
  POST /api/whatif                    twin-line analysis (actual vs alt)
"""

import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import StarRocks, query_game, query_player_profile
from stockfish import eval_fen

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    StarRocks.init()
    log.info("backend ready")
    yield
    StarRocks.close()


app = FastAPI(title="Chess Coach API", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz():
    # Liveness: process is up. Always 200 so k8s does not restart on transient dep blips.
    return {"status": "ok"}


@app.get("/readyz")
def readyz():
    # Readiness: only serve traffic when dependencies we actually need are reachable.
    sr = StarRocks.healthy()
    if not sr:
        raise HTTPException(503, "starrocks unreachable")
    return {"starrocks": True}


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


class EvalRequest(BaseModel):
    fen: str
    depth: int | None = None


@app.post("/api/eval")
def post_eval(req: EvalRequest):
    result = eval_fen(req.fen, req.depth)
    if result is None:
        raise HTTPException(503, "Stockfish unavailable")
    return result


@app.get("/api/players/{username}/profile")
def get_player_profile(username: str):
    profile = query_player_profile(username)
    if profile is None:
        raise HTTPException(404, f"No data for player '{username}'")
    return profile
