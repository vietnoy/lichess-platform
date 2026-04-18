import threading

import chess
import chess.engine
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager

engine: chess.engine.SimpleEngine | None = None
engine_lock = threading.Lock()


@asynccontextmanager
async def lifespan(app):
    global engine
    engine = chess.engine.SimpleEngine.popen_uci("/usr/games/stockfish")
    yield
    engine.quit()


app = FastAPI(title="Stockfish API", lifespan=lifespan)


@app.get("/eval")
def eval_position(fen: str, depth: int = 18):
    if engine is None:
        raise HTTPException(503, "Engine not ready")
    try:
        board = chess.Board(fen)
    except ValueError:
        raise HTTPException(400, f"Invalid FEN: {fen}")

    with engine_lock:
        info = engine.analyse(board, chess.engine.Limit(depth=depth))

    score = info["score"].white()
    cp        = score.score()
    mate      = score.mate()
    best_move = info.get("pv")
    best_move_uci = best_move[0].uci() if best_move else None

    return {"cp": cp, "mate": mate, "best_move": best_move_uci}


@app.get("/health")
def health():
    return {"status": "ok", "engine": "stockfish"}
