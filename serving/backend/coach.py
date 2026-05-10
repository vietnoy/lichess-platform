"""
Streaming AI Coach.

Uses Groq's OpenAI-compatible API. Yields event dicts for SSE:
  {"type": "token", "text": "..."}        partial text from the model
  {"type": "tool_start", "name", "args"}  about to run a tool
  {"type": "tool_result", "name", "summary"}
  {"type": "done"}                        end of turn
  {"type": "error", "message"}            fatal error this turn

A small in-memory session store keeps per-conversation message history so multi-turn works.
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

from openai import OpenAI

from db import StarRocks, TABLE
from stockfish import eval_fen

log = logging.getLogger("coach")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        if not GROQ_API_KEY:
            raise RuntimeError("GROQ_API_KEY not set; coach disabled")
        _client = OpenAI(api_key=GROQ_API_KEY, base_url=GROQ_BASE_URL)
    return _client


# ─── tools ────────────────────────────────────────────────────────────────────
def _q(sql: str, params: tuple) -> list[dict]:
    with StarRocks.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def get_player_overview(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT speed,
               COUNT(*) AS total_games,
               SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN (white_id=%s AND winner='black') OR (black_id=%s AND winner='white') THEN 1 ELSE 0 END) AS losses,
               SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws,
               ROUND(AVG(CASE WHEN white_id=%s THEN white_rating ELSE black_rating END), 0) AS avg_rating
        FROM (
            SELECT DISTINCT game_id, speed, winner, white_id, black_id, white_rating, black_rating
            FROM {TABLE}
            WHERE (white_id=%s OR black_id=%s) AND move_number=1
        ) t
        GROUP BY speed ORDER BY total_games DESC
        """,
        (player_id, player_id, player_id, player_id, player_id, player_id, player_id),
    )
    return {"player_id": player_id, "overview": rows}


def get_time_pressure_stats(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT pressure,
               COUNT(*) AS games,
               SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct,
               ROUND(AVG(clock_remaining)/100.0, 1) AS avg_clock_s
        FROM (
            SELECT DISTINCT game_id, winner, white_id, black_id, clock_remaining,
                   CASE WHEN clock_remaining < 1000 THEN 'under_10s' ELSE 'normal' END AS pressure
            FROM {TABLE}
            WHERE (white_id=%s OR black_id=%s) AND clock_remaining IS NOT NULL AND move_number=1
        ) t
        GROUP BY pressure
        """,
        (player_id, player_id, player_id, player_id, player_id, player_id),
    )
    return {"player_id": player_id, "time_pressure": rows}


def get_opening_stats(player_id: str, top_n: int = 10) -> dict:
    rows = _q(
        f"""
        SELECT opening_eco, opening_name,
               COUNT(*) AS games,
               SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct
        FROM (
            SELECT DISTINCT game_id, opening_eco, opening_name, winner, white_id, black_id
            FROM {TABLE}
            WHERE (white_id=%s OR black_id=%s) AND move_number=1 AND opening_eco IS NOT NULL
        ) t
        GROUP BY opening_eco, opening_name
        HAVING games >= 2
        ORDER BY games DESC
        LIMIT %s
        """,
        (player_id, player_id, player_id, player_id, player_id, player_id, int(top_n)),
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
        FROM (
            SELECT DISTINCT game_id, move_number, clock_remaining
            FROM {TABLE}
            WHERE (white_id=%s OR black_id=%s) AND clock_remaining IS NOT NULL
        ) t
        GROUP BY phase ORDER BY phase
        """,
        (player_id, player_id),
    )
    return {"player_id": player_id, "clock_by_phase": rows}


def get_performance_by_color(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT CASE WHEN white_id=%s THEN 'white' ELSE 'black' END AS color,
               COUNT(*) AS games,
               SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct
        FROM (
            SELECT DISTINCT game_id, winner, white_id, black_id
            FROM {TABLE}
            WHERE (white_id=%s OR black_id=%s) AND move_number=1
        ) t
        GROUP BY color
        """,
        (player_id, player_id, player_id, player_id, player_id, player_id, player_id),
    )
    return {"player_id": player_id, "by_color": rows}


def get_performance_vs_rating(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT CASE
                 WHEN (CASE WHEN white_id=%s THEN black_rating ELSE white_rating END) < (CASE WHEN white_id=%s THEN white_rating ELSE black_rating END) - 100 THEN 'lower_rated'
                 WHEN (CASE WHEN white_id=%s THEN black_rating ELSE white_rating END) > (CASE WHEN white_id=%s THEN white_rating ELSE black_rating END) + 100 THEN 'higher_rated'
                 ELSE 'equal_rated' END AS opponent_class,
               COUNT(*) AS games,
               SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN (white_id=%s AND winner='white') OR (black_id=%s AND winner='black') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct
        FROM (
            SELECT DISTINCT game_id, winner, white_id, black_id, white_rating, black_rating
            FROM {TABLE}
            WHERE (white_id=%s OR black_id=%s) AND move_number=1
        ) t
        GROUP BY opponent_class
        """,
        (player_id, player_id, player_id, player_id, player_id, player_id, player_id, player_id, player_id, player_id),
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
        FROM (
            SELECT DISTINCT game_id, white_id, black_id, white_rating, black_rating,
                   opening_eco, opening_name, speed, winner, end_status, date
            FROM {TABLE}
            WHERE (white_id=%s OR black_id=%s) AND move_number=1
        ) t
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


def _tool(name: str, description: str, props: dict, required: list[str]) -> dict:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": props, "required": required},
        },
    }


_PLAYER_PROP = {"player_id": {"type": "string", "description": "Lichess username"}}

_OPENAI_TOOLS: list[dict] = [
    _tool("get_player_overview", "Total games, wins/losses/draws and average rating by time control.", _PLAYER_PROP, ["player_id"]),
    _tool("get_time_pressure_stats", "Win rate when clock is under 10 seconds vs normal.", _PLAYER_PROP, ["player_id"]),
    _tool("get_opening_stats", "Win rate by opening ECO. Identifies structural opening weaknesses.",
          {**_PLAYER_PROP, "top_n": {"type": "integer", "description": "Top N openings (default 10)"}}, ["player_id"]),
    _tool("get_clock_usage_by_phase", "Average clock remaining in opening, middlegame, endgame.", _PLAYER_PROP, ["player_id"]),
    _tool("get_performance_by_color", "Win rate as white vs black.", _PLAYER_PROP, ["player_id"]),
    _tool("get_performance_vs_rating", "Win rate vs lower, equal and higher rated opponents.", _PLAYER_PROP, ["player_id"]),
    _tool("get_recent_games", "Last N games with opponent, opening, result and time control.",
          {**_PLAYER_PROP, "limit": {"type": "integer", "description": "Number of games (default 10)"}}, ["player_id"]),
    _tool("analyze_game", "Move-by-move Stockfish analysis of a specific game; returns eval, best move, blunder/mistake/inaccuracy classification per move.",
          {"game_id": {"type": "string", "description": "Lichess game ID"}}, ["game_id"]),
]

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
    for key in ("overview", "opening_stats", "recent_games", "by_color", "vs_rating", "time_pressure", "clock_by_phase", "moves"):
        v = obj.get(key) if isinstance(obj, dict) else None
        if isinstance(v, list):
            return f"{name} → {len(v)} rows"
    return f"{name} → ok"


class CoachSession:
    """One conversation. The lock serializes turns; messages are the chat history."""

    def __init__(self):
        self.messages: list[dict] = [{"role": "system", "content": _SYSTEM_PROMPT}]
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
        if not self.lock.acquire(blocking=False):
            yield {"type": "error", "message": "Another request is in flight on this session"}
            return
        try:
            self.last_used = time.time()
            self.messages.append({"role": "user", "content": message})
            try:
                yield from self._loop()
            except Exception as e:
                log.exception("agent stream failed")
                yield {"type": "error", "message": str(e)}
        finally:
            self.lock.release()

    def _loop(self) -> Iterator[dict]:
        # OpenAI-style agent loop: stream tokens; if the assistant emits tool_calls, dispatch and round-trip.
        client = _get_client()
        while True:
            stream = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=self.messages,
                tools=_OPENAI_TOOLS,
                stream=True,
            )
            assistant_text = ""
            # tool_calls arrive as deltas with index, accumulate by index.
            tool_calls: dict[int, dict] = {}
            for chunk in stream:
                if not chunk.choices:
                    continue
                delta = chunk.choices[0].delta
                if delta.content:
                    assistant_text += delta.content
                    yield {"type": "token", "text": delta.content}
                if delta.tool_calls:
                    for tc in delta.tool_calls:
                        idx = tc.index
                        slot = tool_calls.setdefault(idx, {"id": "", "name": "", "arguments": ""})
                        if tc.id:
                            slot["id"] = tc.id
                        if tc.function and tc.function.name:
                            slot["name"] = tc.function.name
                        if tc.function and tc.function.arguments:
                            slot["arguments"] += tc.function.arguments

            if not tool_calls:
                # Plain text response, conversation turn complete.
                self.messages.append({"role": "assistant", "content": assistant_text})
                yield {"type": "done"}
                return

            # Record the assistant's tool-call message so the next round has correct history.
            self.messages.append({
                "role": "assistant",
                "content": assistant_text or None,
                "tool_calls": [
                    {"id": v["id"], "type": "function", "function": {"name": v["name"], "arguments": v["arguments"]}}
                    for _, v in sorted(tool_calls.items())
                ],
            })

            for _, tc in sorted(tool_calls.items()):
                name = tc["name"]
                try:
                    args = json.loads(tc["arguments"]) if tc["arguments"] else {}
                except Exception:
                    args = {}
                yield {"type": "tool_start", "name": name, "args": args}
                result_json = self._dispatch(name, args)
                yield {"type": "tool_result", "name": name, "summary": _short_summary(name, result_json)}
                self.messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "name": name,
                    "content": result_json,
                })


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
