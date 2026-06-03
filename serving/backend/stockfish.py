"""Stockfish HTTP proxy."""

from collections import OrderedDict
import logging
import os
import threading
import time

import requests

log = logging.getLogger("stockfish")

STOCKFISH_URL = os.getenv("STOCKFISH_URL", "http://stockfish:8001/eval")
DEFAULT_DEPTH = int(os.getenv("STOCKFISH_DEPTH", "12"))
MAX_DEPTH = int(os.getenv("STOCKFISH_MAX_DEPTH", "18"))
EVAL_CACHE_MAX_ENTRIES = int(os.getenv("STOCKFISH_EVAL_CACHE_MAX_ENTRIES", "5000"))
EVAL_CACHE_TTL_SECONDS = int(os.getenv("STOCKFISH_EVAL_CACHE_TTL_SECONDS", str(24 * 60 * 60)))

_CacheKey = tuple[str, int]
_eval_cache: OrderedDict[_CacheKey, tuple[float, dict]] = OrderedDict()
_eval_cache_lock = threading.Lock()


def _normalize_depth(depth: int | None) -> int:
    value = DEFAULT_DEPTH if depth is None else int(depth)
    return max(1, min(value, MAX_DEPTH))


def clear_eval_cache() -> None:
    with _eval_cache_lock:
        _eval_cache.clear()


def _cache_get(key: _CacheKey) -> dict | None:
    if EVAL_CACHE_MAX_ENTRIES <= 0 or EVAL_CACHE_TTL_SECONDS <= 0:
        return None
    now = time.monotonic()
    with _eval_cache_lock:
        cached = _eval_cache.get(key)
        if not cached:
            return None
        inserted_at, value = cached
        if now - inserted_at > EVAL_CACHE_TTL_SECONDS:
            _eval_cache.pop(key, None)
            return None
        _eval_cache.move_to_end(key)
        return dict(value)


def _cache_put(key: _CacheKey, value: dict) -> None:
    if EVAL_CACHE_MAX_ENTRIES <= 0 or EVAL_CACHE_TTL_SECONDS <= 0:
        return
    with _eval_cache_lock:
        _eval_cache[key] = (time.monotonic(), dict(value))
        _eval_cache.move_to_end(key)
        while len(_eval_cache) > EVAL_CACHE_MAX_ENTRIES:
            _eval_cache.popitem(last=False)


def eval_fen(fen: str, depth: int | None = None) -> dict | None:
    normalized_depth = _normalize_depth(depth)
    cache_key = (fen, normalized_depth)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        r = requests.get(
            STOCKFISH_URL,
            params={"fen": fen, "depth": normalized_depth},
            timeout=15,
        )
        if r.status_code == 200:
            result = r.json()
            _cache_put(cache_key, result)
            return result
        log.warning("stockfish %s returned %s", STOCKFISH_URL, r.status_code)
    except Exception as e:
        log.warning("stockfish call failed: %s", e)
    return None
