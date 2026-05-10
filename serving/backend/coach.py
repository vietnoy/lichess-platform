"""
Streaming AI Coach.

Exposes a generator that yields event dicts:
  {"type": "token", "text": "..."}        partial text from the model
  {"type": "tool_start", "name", "args"}  about to run a tool
  {"type": "tool_result", "name", "summary"}
  {"type": "done"}                        end of turn
  {"type": "error", "message"}            fatal error this turn

A small in-memory session store keeps per-conversation chat history so multi-turn works.
Tools query StarRocks via the same connection pool as the rest of the API and accept
parameter bindings (the legacy agent.py uses string-interpolated SQL — we don't).
"""

from __future__ import annotations

import os
import json
import logging
import threading
import time
from typing import Any, Iterator

import vertexai
from google.oauth2 import service_account
from vertexai.generative_models import GenerativeModel, Part, Tool, FunctionDeclaration

from db import StarRocks, TABLE
from stockfish import eval_fen

log = logging.getLogger("coach")

_GCP_PROJECT  = os.getenv("GCP_PROJECT", "")
_GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")
_GCP_CREDS    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

_vertex_initialized = False


def _init_vertex():
    global _vertex_initialized
    if _vertex_initialized:
        return
    if not _GCP_PROJECT:
        raise RuntimeError("GCP_PROJECT not set; coach disabled")
    if _GCP_CREDS and os.path.exists(_GCP_CREDS):
        creds = service_account.Credentials.from_service_account_file(
            _GCP_CREDS, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        vertexai.init(project=_GCP_PROJECT, location=_GCP_LOCATION, credentials=creds)
    else:
        vertexai.init(project=_GCP_PROJECT, location=_GCP_LOCATION)
    _vertex_initialized = True


# ─── tools ────────────────────────────────────────────────────────────────────
def _q(sql: str, params: tuple) -> list[dict]:
    with StarRocks.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def get_player_overview(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT speed,
               COUNT(DISTINCT game_id) AS total_games,
               SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN winner IS NOT NULL AND winner != CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS losses,
               SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws,
               ROUND(AVG(CASE WHEN white_id=%s THEN white_rating ELSE black_rating END), 0) AS avg_rating
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        GROUP BY speed ORDER BY total_games DESC
        """,
        (player_id, player_id, player_id, player_id, player_id),
    )
    return {"player_id": player_id, "overview": rows}


def get_time_pressure_stats(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT CASE WHEN clock_remaining < 1000 THEN 'under_10s' ELSE 'normal' END AS pressure,
               COUNT(DISTINCT game_id) AS games,
               SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_rate_pct,
               ROUND(AVG(clock_remaining)/100.0, 1) AS avg_clock_s
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND clock_remaining IS NOT NULL AND move_number=1
        GROUP BY pressure
        """,
        (player_id, player_id, player_id, player_id),
    )
    return {"player_id": player_id, "time_pressure": rows}


def get_opening_stats(player_id: str, top_n: int = 10) -> dict:
    rows = _q(
        f"""
        SELECT opening_eco, opening_name,
               COUNT(DISTINCT game_id) AS games,
               SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_rate_pct
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1 AND opening_eco IS NOT NULL
        GROUP BY opening_eco, opening_name
        HAVING games >= 2
        ORDER BY games DESC
        LIMIT %s
        """,
        (player_id, player_id, player_id, player_id, int(top_n)),
    )
    return {"player_id": player_id, "opening_stats": rows}


def get_clock_usage_by_phase(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT CASE WHEN move_number<=10 THEN 'opening'
                    WHEN move_number<=30 THEN 'middlegame'
                    ELSE 'endgame' END AS phase,
               ROUND(AVG(clock_remaining)/100.0, 1) AS avg_clock_s,
               ROUND(MIN(clock_remaining)/100.0, 1) AS min_clock_s,
               COUNT(*) AS move_count
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND clock_remaining IS NOT NULL
        GROUP BY phase ORDER BY phase
        """,
        (player_id, player_id),
    )
    return {"player_id": player_id, "clock_by_phase": rows}


def get_performance_by_color(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT CASE WHEN white_id=%s THEN 'white' ELSE 'black' END AS color,
               COUNT(DISTINCT game_id) AS games,
               SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_rate_pct
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        GROUP BY color
        """,
        (player_id, player_id, player_id, player_id, player_id),
    )
    return {"player_id": player_id, "by_color": rows}


def get_performance_vs_rating(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT CASE
                 WHEN (CASE WHEN white_id=%s THEN black_rating ELSE white_rating END) < (CASE WHEN white_id=%s THEN white_rating ELSE black_rating END) - 100 THEN 'lower_rated'
                 WHEN (CASE WHEN white_id=%s THEN black_rating ELSE white_rating END) > (CASE WHEN white_id=%s THEN white_rating ELSE black_rating END) + 100 THEN 'higher_rated'
                 ELSE 'equal_rated' END AS opponent_class,
               COUNT(DISTINCT game_id) AS games,
               SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN winner = CASE WHEN white_id=%s THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_rate_pct
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        GROUP BY opponent_class
        """,
        (player_id, player_id, player_id, player_id, player_id, player_id, player_id, player_id),
    )
    return {"player_id": player_id, "vs_rating": rows}


def get_recent_games(player_id: str, limit: int = 10) -> dict:
    rows = _q(
        f"""
        SELECT game_id,
               CASE WHEN white_id=%s THEN black_id ELSE white_id END AS opponent,
               CASE WHEN white_id=%s THEN white_rating ELSE black_rating END AS my_rating,
               CASE WHEN white_id=%s THEN black_rating ELSE white_rating END AS opp_rating,
               opening_eco, opening_name, speed, winner, end_status, date
        FROM {TABLE}
        WHERE (white_id=%s OR black_id=%s) AND move_number=1
        ORDER BY date DESC LIMIT %s
        """,
        (player_id, player_id, player_id, player_id, player_id, int(limit)),
    )
    return {"player_id": player_id, "recent_games": rows}


def analyze_game(game_id: str) -> dict:
    moves = _q(
        f"""
        SELECT move_number, whose_moved, move, fen,
               ROUND(clock_remaining/100.0, 1) AS clock_s,
               white_id, black_id, white_rating, black_rating,
               opening_name, speed, winner, end_status
        FROM {TABLE}
        WHERE game_id = %s
        ORDER BY move_number
        """,
        (game_id,),
    )
    if not moves:
        return {"error": f"Game {game_id} not found"}

    annotated, prev_cp = [], None
    for m in moves:
        ev = eval_fen(m["fen"])
        cp = ev.get("cp") if ev else None
        best_move = ev.get("best_move") if ev else None
        delta, classification = None, None
        if cp is not None and prev_cp is not None:
            drop = -(cp - prev_cp) if m["whose_moved"] == "white" else (prev_cp - cp)
            classification = "blunder" if drop >= 200 else "mistake" if drop >= 100 else "inaccuracy" if drop >= 50 else "good"
            delta = cp - prev_cp
        annotated.append({
            "move_number": m["move_number"], "whose_moved": m["whose_moved"], "move": m["move"],
            "eval_cp": cp, "eval_delta": delta, "best_move": best_move,
            "classification": classification, "clock_s": m["clock_s"],
        })
        prev_cp = cp

    meta = moves[0]
    return {
        "game_id": game_id,
        "white": f"{meta['white_id']} ({meta['white_rating']})",
        "black": f"{meta['black_id']} ({meta['black_rating']})",
        "opening": meta["opening_name"], "speed": meta["speed"],
        "result": meta["winner"] or "draw", "end_status": meta["end_status"],
        "moves": annotated, "total_moves": len(annotated),
        "blunders":     sum(1 for x in annotated if x["classification"] == "blunder"),
        "mistakes":     sum(1 for x in annotated if x["classification"] == "mistake"),
        "inaccuracies": sum(1 for x in annotated if x["classification"] == "inaccuracy"),
    }


_TOOL_FNS: dict[str, Any] = {
    "get_player_overview":       get_player_overview,
    "get_time_pressure_stats":   get_time_pressure_stats,
    "get_opening_stats":         get_opening_stats,
    "get_clock_usage_by_phase":  get_clock_usage_by_phase,
    "get_performance_by_color":  get_performance_by_color,
    "get_performance_vs_rating": get_performance_vs_rating,
    "get_recent_games":          get_recent_games,
    "analyze_game":              analyze_game,
}

_PLAYER_PARAM = {"player_id": {"type_": "STRING", "description": "Lichess username"}}

_TOOLS = Tool(function_declarations=[
    FunctionDeclaration(name="get_player_overview", description="Total games, wins/losses/draws and average rating by time control.",
                        parameters={"type_": "OBJECT", "properties": _PLAYER_PARAM, "required": ["player_id"]}),
    FunctionDeclaration(name="get_time_pressure_stats", description="Win rate when clock is under 10 seconds vs normal.",
                        parameters={"type_": "OBJECT", "properties": _PLAYER_PARAM, "required": ["player_id"]}),
    FunctionDeclaration(name="get_opening_stats", description="Win rate by opening ECO. Identifies structural opening weaknesses.",
                        parameters={"type_": "OBJECT", "properties": {**_PLAYER_PARAM, "top_n": {"type_": "INTEGER", "description": "Top N openings (default 10)"}}, "required": ["player_id"]}),
    FunctionDeclaration(name="get_clock_usage_by_phase", description="Average clock remaining in opening, middlegame, endgame.",
                        parameters={"type_": "OBJECT", "properties": _PLAYER_PARAM, "required": ["player_id"]}),
    FunctionDeclaration(name="get_performance_by_color", description="Win rate as white vs black.",
                        parameters={"type_": "OBJECT", "properties": _PLAYER_PARAM, "required": ["player_id"]}),
    FunctionDeclaration(name="get_performance_vs_rating", description="Win rate vs lower, equal and higher rated opponents.",
                        parameters={"type_": "OBJECT", "properties": _PLAYER_PARAM, "required": ["player_id"]}),
    FunctionDeclaration(name="get_recent_games", description="Last N games with opponent, opening, result and time control.",
                        parameters={"type_": "OBJECT", "properties": {**_PLAYER_PARAM, "limit": {"type_": "INTEGER", "description": "Number of games (default 10)"}}, "required": ["player_id"]}),
    FunctionDeclaration(name="analyze_game", description="Move-by-move Stockfish analysis of a specific game; returns eval, best move, blunder/mistake/inaccuracy classification per move.",
                        parameters={"type_": "OBJECT", "properties": {"game_id": {"type_": "STRING", "description": "Lichess game ID"}}, "required": ["game_id"]}),
])

_SYSTEM_PROMPT = """You are an elite AI Chess Coach with access to a statistical database of real Lichess games.
You think like a grandmaster analyst combined with a sports psychologist. You diagnose, you don't just report.

You may not state any specific number or claim about a player without first calling a tool that returns it. Numbers come from data, never from guessing.

Workflow per question:
1. Use as many tools as the question warrants. Extra tool calls are free.
2. Look for intersections: weakness patterns that appear across multiple tools are the real diagnosis.
3. Rank by impact. Focus on the one or two patterns explaining the most losses.
4. Write like a coach: be direct, quote exact numbers, give 2-3 specific actions ranked by expected impact.

If no data is found for a player, say their games may not be in the system yet."""


# ─── streaming engine ────────────────────────────────────────────────────────
def _short_summary(name: str, raw_json: str, max_len: int = 220) -> str:
    """Compact summary of a tool result for the UI tool-call indicator."""
    try:
        obj = json.loads(raw_json)
    except Exception:
        return raw_json[:max_len]
    if isinstance(obj, dict) and "error" in obj:
        return f"error: {obj['error']}"
    # Pick the most informative top-level list and report its length.
    for key in ("overview", "opening_stats", "recent_games", "by_color", "vs_rating", "time_pressure", "clock_by_phase", "moves"):
        v = obj.get(key) if isinstance(obj, dict) else None
        if isinstance(v, list):
            return f"{name} → {len(v)} rows"
    return f"{name} → ok"


class SessionBusy(Exception):
    pass


class CoachSession:
    """One conversation with the AI coach. The lock serializes turns on the same session."""

    def __init__(self):
        _init_vertex()
        self.model = GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=_SYSTEM_PROMPT,
            tools=[_TOOLS],
        )
        self.chat = self.model.start_chat()
        self.last_used = time.time()
        self.lock = threading.Lock()

    def _dispatch(self, name: str, args: dict) -> str:
        fn = _TOOL_FNS.get(name)
        if fn is None:
            return json.dumps({"error": f"unknown tool: {name}"})
        try:
            return json.dumps(fn(**args), default=str)
        except Exception as e:
            log.exception("tool %s failed", name)
            return json.dumps({"error": str(e)})

    def ask_stream(self, message: str) -> Iterator[dict]:
        # Non-blocking lock acquire — if another turn is in flight on the same session,
        # tell the caller instead of corrupting the chat history with interleaved turns.
        if not self.lock.acquire(blocking=False):
            yield {"type": "error", "message": "Another request is in flight on this session"}
            return
        try:
            self.last_used = time.time()
            try:
                yield from self._round(message)
            except Exception as e:
                log.exception("agent stream failed")
                yield {"type": "error", "message": str(e)}
        finally:
            self.lock.release()

    def _round(self, payload) -> Iterator[dict]:
        # Vertex returns an iterable when stream=True; chunks may carry text or function_call parts.
        # Some SDK versions emit the same function_call across multiple chunks while assembling args;
        # dedupe by (name, json(args)) before dispatching.
        stream = self.chat.send_message(payload, stream=True)
        function_calls: list = []
        seen_keys: set[str] = set()
        for chunk in stream:
            try:
                parts = chunk.candidates[0].content.parts
            except Exception:
                continue
            for p in parts:
                fc = getattr(p, "function_call", None)
                if fc and fc.name:
                    try:
                        key = f"{fc.name}::{json.dumps(dict(fc.args), sort_keys=True, default=str)}"
                    except Exception:
                        key = f"{fc.name}::{id(fc)}"
                    if key in seen_keys:
                        continue
                    seen_keys.add(key)
                    function_calls.append(fc)
                    continue
                text = getattr(p, "text", None)
                if text:
                    yield {"type": "token", "text": text}
        if not function_calls:
            yield {"type": "done"}
            return
        responses = []
        for fc in function_calls:
            name = fc.name
            args = dict(fc.args)
            yield {"type": "tool_start", "name": name, "args": args}
            result_json = self._dispatch(name, args)
            yield {"type": "tool_result", "name": name, "summary": _short_summary(name, result_json)}
            responses.append(Part.from_function_response(name=name, response={"result": result_json}))
        yield from self._round(responses)


class SessionStore:
    """In-memory session map. TTL eviction on read; nothing persists across pod restarts."""
    TTL_SECONDS = 60 * 60

    def __init__(self):
        self._lock = threading.Lock()
        self._sessions: dict[str, CoachSession] = {}

    def get(self, sid: str) -> CoachSession:
        now = time.time()
        with self._lock:
            self._evict_expired(now)
            s = self._sessions.get(sid)
            if s is None:
                s = CoachSession()
                self._sessions[sid] = s
            return s

    def reset(self, sid: str) -> None:
        with self._lock:
            self._sessions.pop(sid, None)

    def _evict_expired(self, now: float):
        dead = [sid for sid, s in self._sessions.items() if now - s.last_used > self.TTL_SECONDS]
        for sid in dead:
            self._sessions.pop(sid, None)


SESSIONS = SessionStore()
