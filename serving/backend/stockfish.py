"""Stockfish HTTP proxy."""

import os
import logging
import requests

log = logging.getLogger("stockfish")

STOCKFISH_URL = os.getenv("STOCKFISH_URL", "http://stockfish:8001/eval")
DEFAULT_DEPTH = int(os.getenv("STOCKFISH_DEPTH", "12"))


def eval_fen(fen: str, depth: int | None = None) -> dict | None:
    try:
        r = requests.get(
            STOCKFISH_URL,
            params={"fen": fen, "depth": depth or DEFAULT_DEPTH},
            timeout=15,
        )
        if r.status_code == 200:
            return r.json()
        log.warning("stockfish %s returned %s", STOCKFISH_URL, r.status_code)
    except Exception as e:
        log.warning("stockfish call failed: %s", e)
    return None
