"""
Streaming AI Coach.

Uses Vertex AI Gemini. Yields event dicts for SSE:
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
import re
import threading
import time
from typing import Any, Iterator

import requests

from db import (
    StarRocks,
    TABLE,
    PLAYER_GAMES,
    query_blunder_examples,
    query_game,
    query_game_evaluations,
    query_opening_stats,
    query_phase_stats,
    query_player_profile,
    query_weakness_summary,
)

log = logging.getLogger("coach")

COACH_FINAL_PROVIDER = os.getenv("COACH_FINAL_PROVIDER", "vertex").lower()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GCP_PROJECT = os.getenv("GCP_PROJECT", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")
VERTEX_MODEL = os.getenv("VERTEX_MODEL", "gemini-2.5-flash")
COACH_MAX_TOOL_ROUNDS = int(os.getenv("COACH_MAX_TOOL_ROUNDS", "8"))


def _gemini_final_answer(messages: list[dict]) -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set; Gemini final answer unavailable")
    last_user_idx = max((idx for idx, msg in enumerate(messages) if msg.get("role") == "user"), default=0)
    user_message = messages[last_user_idx].get("content", "")
    tool_evidence = [
        {
            "name": msg.get("name"),
            "content": msg.get("content"),
        }
        for msg in messages[last_user_idx + 1:]
        if msg.get("role") == "tool"
    ]
    evidence_json = json.dumps(tool_evidence, ensure_ascii=False, default=str)
    if len(evidence_json) > 60_000:
        evidence_json = evidence_json[:60_000] + "...[truncated]"

    response = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": GEMINI_API_KEY,
        },
        json={
            "systemInstruction": {
                "parts": [{"text": _SYSTEM_PROMPT}],
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "text": (
                                "Yêu cầu người dùng:\n"
                                f"{user_message}\n\n"
                                "Hướng dẫn dùng dữ liệu:\n"
                                f"{_TOOL_GUIDE}\n\n"
                                "Dữ liệu tool đã lấy, chỉ được dùng các số liệu trong JSON này:\n"
                                f"{evidence_json}"
                            )
                        }
                    ],
                }
            ],
            "generationConfig": {
                "temperature": 0.35,
                "maxOutputTokens": 2200,
            },
        },
        timeout=45,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Gemini API error {response.status_code}: {response.text[:1000]}")
    payload = response.json()
    parts = (
        payload.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    text = "".join(part.get("text", "") for part in parts)
    if not text.strip():
        raise RuntimeError("Gemini returned an empty response")
    return text


def _final_answer_payload(messages: list[dict]) -> dict[str, Any]:
    last_user_idx = max((idx for idx, msg in enumerate(messages) if msg.get("role") == "user"), default=0)
    user_message = messages[last_user_idx].get("content", "")
    tool_evidence = [
        {
            "name": msg.get("name"),
            "content": msg.get("content"),
        }
        for msg in messages[last_user_idx + 1:]
        if msg.get("role") == "tool"
    ]
    evidence_json = json.dumps(tool_evidence, ensure_ascii=False, default=str)
    if len(evidence_json) > 60_000:
        evidence_json = evidence_json[:60_000] + "...[truncated]"
    return {
        "systemInstruction": {
            "parts": [{"text": _SYSTEM_PROMPT}],
        },
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": (
                            "Yêu cầu người dùng:\n"
                            f"{user_message}\n\n"
                            "Hướng dẫn dùng dữ liệu:\n"
                            f"{_TOOL_GUIDE}\n\n"
                            "Dữ liệu tool đã lấy, chỉ được dùng các số liệu trong JSON này:\n"
                            f"{evidence_json}"
                        )
                    }
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0.35,
            "maxOutputTokens": 2200,
        },
    }


def _vertex_access_token() -> str:
    try:
        import google.auth
        from google.auth.transport.requests import Request
    except ImportError as exc:
        raise RuntimeError("google-auth not installed; Vertex final answer unavailable") from exc

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    if not credentials.token:
        raise RuntimeError("Vertex authentication did not return an access token")
    return credentials.token


def _vertex_generate_content(payload: dict[str, Any], timeout: int = 45) -> str:
    response_payload = _vertex_generate_content_payload(payload, timeout=timeout)
    text = _content_text(
        response_payload.get("candidates", [{}])[0].get("content", {})
    )
    if not text.strip():
        raise RuntimeError("Vertex returned an empty response")
    return text


def _vertex_generate_content_payload(payload: dict[str, Any], timeout: int = 45) -> dict[str, Any]:
    if not GCP_PROJECT:
        raise RuntimeError("GCP_PROJECT not set; Vertex unavailable")

    response = requests.post(
        (
            f"https://{GCP_LOCATION}-aiplatform.googleapis.com/v1/"
            f"projects/{GCP_PROJECT}/locations/{GCP_LOCATION}/publishers/google/"
            f"models/{VERTEX_MODEL}:generateContent"
        ),
        headers={
            "Authorization": f"Bearer {_vertex_access_token()}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Vertex API error {response.status_code}: {response.text[:1000]}")
    return response.json()


def _content_text(content: dict[str, Any]) -> str:
    return "".join(part.get("text", "") for part in content.get("parts", []))


def _content_function_calls(content: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        part["functionCall"]
        for part in content.get("parts", [])
        if isinstance(part, dict) and isinstance(part.get("functionCall"), dict)
    ]


def _vertex_final_answer(messages: list[dict]) -> str:
    return _vertex_generate_content(_final_answer_payload(messages), timeout=45)


def vertex_text_answer(system_prompt: str, user_prompt: str, *, temperature: float = 0.35, max_output_tokens: int = 1200) -> str:
    """Generate a plain text answer through the same Vertex Gemini model."""
    return _vertex_generate_content(
        {
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "temperature": temperature,
                "maxOutputTokens": max_output_tokens,
            },
        },
        timeout=45,
    )


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
               SUM(CASE WHEN winner = color THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN winner IS NOT NULL AND winner <> color THEN 1 ELSE 0 END) AS losses,
               SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws,
               ROUND(AVG(my_rating), 0) AS avg_rating
        FROM {PLAYER_GAMES}
        WHERE player_id = %s
        GROUP BY speed ORDER BY total_games DESC
        """,
        (player_id,),
    )
    return {"player_id": player_id, "overview": rows}


def get_weakness_summary(player_id: str, days: int = 60) -> dict:
    return query_weakness_summary(player_id, days=days)


def _coach_brief(profile: dict, weakness: dict, phase_stats: list[dict], opening_stats: list[dict], examples: list[dict]) -> dict:
    totals = profile.get("totals") or {}
    by_color = profile.get("by_color") or []
    worst_color = min(by_color, key=lambda row: row.get("win_pct") or 0) if by_color else None
    best_color = max(by_color, key=lambda row: row.get("win_pct") or 0) if by_color else None
    color_gap = None
    if worst_color and best_color:
        color_gap = round((best_color.get("win_pct") or 0) - (worst_color.get("win_pct") or 0), 1)

    top_phase = phase_stats[0] if phase_stats else {}
    top_opening = opening_stats[0] if opening_stats else {}
    top_example = examples[0] if examples else {}

    diagnosis = []
    if totals:
        diagnosis.append(
            f"Win rate {totals.get('win_pct')}% trên {totals.get('games')} ván, rating TB {totals.get('avg_rating')}."
        )
    if worst_color:
        diagnosis.append(
            f"Cầm {worst_color.get('color')} là điểm rơi lớn nhất: {worst_color.get('win_pct')}% thắng"
            + (f", lệch {color_gap} điểm % so với màu tốt hơn." if color_gap is not None else ".")
        )
    if top_phase:
        diagnosis.append(
            f"Phase yếu nhất là {top_phase.get('phase')}: {top_phase.get('critical_positions')} critical positions, "
            f"{top_phase.get('blunders')} blunders, {top_phase.get('mistakes')} mistakes."
        )
    if top_opening:
        diagnosis.append(
            f"Opening cần ưu tiên: {top_opening.get('opening_eco')} {top_opening.get('opening_name')} khi cầm "
            f"{top_opening.get('color')}, win rate {top_opening.get('win_rate_pct')}%, "
            f"{top_opening.get('critical_positions')} critical positions."
        )

    drills = []
    if top_opening:
        drills.append(
            f"Review 5 ván gần nhất trong {top_opening.get('opening_eco')} {top_opening.get('opening_name')}; "
            "dừng sau khai cuộc và ghi lại kế hoạch quân trước khi xem engine."
        )
    if top_phase:
        drills.append(
            f"Làm drill theo phase {top_phase.get('phase')}: mỗi vị trí tự chọn 3 candidate moves, "
            "loại tactical blunder trước rồi mới so engine."
        )
    if top_example:
        drills.append(
            f"Review game {top_example.get('game_id')} ply {top_example.get('ply')}: so sánh "
            f"{top_example.get('played_move') or 'nước đã chơi'} với {top_example.get('best_move') or 'best move'}."
        )
    elif worst_color:
        drills.append(f"Tạo session 20 phút chỉ luyện các ván cầm {worst_color.get('color')} trong opening yếu nhất.")

    return {
        "diagnosis": diagnosis[:4],
        "drills": drills[:3],
    }


def inspect_student_style(player_id: str, days: int = 60) -> dict:
    """One-shot scouting report for the coach before giving advice."""
    days = max(1, min(int(days), 365))
    profile = query_player_profile(player_id, days=days) or {}
    weakness = query_weakness_summary(player_id, days=days)
    phase_stats = query_phase_stats(player_id, days=days)
    opening_stats = query_opening_stats(player_id, days=days, top_n=8)
    top_phase = weakness.get("top_phase") if isinstance(weakness, dict) else None
    examples = query_blunder_examples(
        player_id,
        limit=5,
        phase=top_phase if top_phase in {"opening", "middlegame", "endgame"} else None,
    )
    profile_slice = {
        "range": profile.get("range"),
        "totals": profile.get("totals"),
        "by_speed": profile.get("by_speed", [])[:5],
        "by_color": profile.get("by_color", []),
        "vs_rating": profile.get("vs_rating", []),
        "rating_history": profile.get("rating_history", [])[-20:],
    }
    return {
        "player_id": player_id,
        "days": days,
        "coach_brief": _coach_brief(profile_slice, weakness, phase_stats, opening_stats, examples),
        "profile": profile_slice,
        "weakness": weakness,
        "phase_stats": phase_stats,
        "opening_stats": opening_stats,
        "critical_examples": examples,
        "recent_games": profile.get("recent_games", [])[:8],
    }


def get_blunder_examples(
    player_id: str,
    limit: int = 5,
    phase: str | None = None,
    time_pressure: str | None = None,
) -> dict:
    return {
        "player_id": player_id,
        "examples": query_blunder_examples(
            player_id,
            limit=limit,
            phase=phase,
            time_pressure=time_pressure,
        ),
    }


def get_time_pressure_stats(player_id: str, days: int = 60) -> dict:
    days = max(1, min(int(days), 365))
    weakness = query_weakness_summary(player_id, days=days)
    phase_stats = query_phase_stats(player_id, days=days)
    critical = int(weakness.get("critical_positions") or 0)
    pressure_positions = int(weakness.get("time_pressure_positions") or 0)
    share_pct = round(pressure_positions * 100.0 / critical, 1) if critical else 0.0
    by_phase = []
    for row in phase_stats:
        phase_critical = int(row.get("critical_positions") or 0)
        phase_pressure = int(row.get("time_pressure_positions") or 0)
        by_phase.append(
            {
                "phase": row.get("phase"),
                "critical_positions": phase_critical,
                "time_pressure_positions": phase_pressure,
                "share_pct": round(phase_pressure * 100.0 / phase_critical, 1) if phase_critical else 0.0,
            }
        )
    return {
        "player_id": player_id,
        "days": days,
        "time_pressure": {
            "critical_positions": critical,
            "time_pressure_positions": pressure_positions,
            "share_pct": share_pct,
            "top_time_pressure": weakness.get("top_time_pressure"),
        },
        "by_phase": by_phase,
    }


def get_opening_stats(player_id: str, top_n: int = 10, days: int = 60) -> dict:
    return {
        "player_id": player_id,
        "opening_stats": query_opening_stats(player_id, days=days, top_n=top_n),
    }


def get_phase_stats(player_id: str, days: int = 60) -> dict:
    return {
        "player_id": player_id,
        "phase_stats": query_phase_stats(player_id, days=days),
    }


def get_clock_usage_by_phase(player_id: str, days: int = 60) -> dict:
    # Move-level clock data — narrow by dates this player played.
    days = max(1, min(int(days), 365))
    date_rows = _q(
        f"""
        SELECT DISTINCT date, game_id
        FROM {PLAYER_GAMES}
        WHERE player_id = %s
          AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL %s DAY)
        """,
        (player_id, days),
    )
    if not date_rows:
        return {"player_id": player_id, "days": days, "clock_by_phase": []}
    dates = sorted({r["date"].isoformat() for r in date_rows if r["date"]})
    game_ids = [r["game_id"] for r in date_rows]
    date_ph = ",".join(["%s"] * len(dates))
    game_ph = ",".join(["%s"] * len(game_ids))
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
            WHERE date IN ({date_ph})
              AND game_id IN ({game_ph})
              AND clock_remaining IS NOT NULL
        ) t
        GROUP BY phase ORDER BY phase
        """,
        tuple(dates) + tuple(game_ids),
    )
    return {"player_id": player_id, "days": days, "clock_by_phase": rows}


def get_performance_by_color(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT color,
               COUNT(*) AS games,
               SUM(CASE WHEN winner = color THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN winner = color THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct
        FROM {PLAYER_GAMES}
        WHERE player_id = %s
        GROUP BY color
        """,
        (player_id,),
    )
    return {"player_id": player_id, "by_color": rows}


def get_performance_vs_rating(player_id: str) -> dict:
    rows = _q(
        f"""
        SELECT CASE
                 WHEN opp_rating < my_rating - 100 THEN 'lower_rated'
                 WHEN opp_rating > my_rating + 100 THEN 'higher_rated'
                 ELSE 'equal_rated' END AS opponent_class,
               COUNT(*) AS games,
               SUM(CASE WHEN winner = color THEN 1 ELSE 0 END) AS wins,
               ROUND(SUM(CASE WHEN winner = color THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate_pct
        FROM {PLAYER_GAMES}
        WHERE player_id = %s AND my_rating IS NOT NULL AND opp_rating IS NOT NULL
        GROUP BY opponent_class
        """,
        (player_id,),
    )
    return {"player_id": player_id, "vs_rating": rows}


def get_recent_games(player_id: str, limit: int = 10) -> dict:
    rows = _q(
        f"""
        SELECT game_id, opponent_id AS opponent, my_rating, opp_rating,
               opening_eco, opening_name, speed, winner, end_status, date
        FROM {PLAYER_GAMES}
        WHERE player_id = %s
        ORDER BY date DESC LIMIT %s
        """,
        (player_id, int(limit)),
    )
    return {"player_id": player_id, "recent_games": rows}


def analyze_game(game_id: str) -> dict:
    moves = query_game(game_id)
    if not moves:
        return {"error": f"Game {game_id} not found"}

    evaluations = query_game_evaluations(game_id)
    if not evaluations:
        return {
            "error": (
                f"Game {game_id} exists, but stored analyzer evaluations are not available yet. "
                "The coach will not run live Stockfish over the full game on the production API."
            )
        }

    eval_by_ply = {int(row["ply"]): row for row in evaluations}
    annotated = []
    for m in moves:
        ply = int(m["move_number"])
        ev = eval_by_ply.get(ply, {})
        annotated.append({
            "move_number": ply,
            "whose_moved": m["whose_moved"],
            "move": m["move"],
            "eval_cp": ev.get("eval_cp"),
            "mate": ev.get("mate"),
            "eval_delta": ev.get("eval_swing_cp_from_prev"),
            "best_move": ev.get("best_move"),
            "classification": ev.get("classification"),
            "clock_s": m["clock_s"],
        })

    meta = moves[0]
    return {
        "game_id": game_id,
        "white": f"{meta['white_id']} ({meta['white_rating']})",
        "black": f"{meta['black_id']} ({meta['black_rating']})",
        "opening": meta["opening_name"], "speed": meta["speed"],
        "result": meta["winner"] or "draw", "end_status": meta["end_status"],
        "moves": annotated,
        "total_moves": len(annotated),
        "evaluated_positions": len(evaluations),
        "blunders":     sum(1 for x in annotated if x["classification"] == "blunder"),
        "mistakes":     sum(1 for x in annotated if x["classification"] == "mistake"),
        "inaccuracies": sum(1 for x in annotated if x["classification"] == "inaccuracy"),
    }


_TOOL_FNS: dict[str, Any] = {
    "inspect_student_style":     inspect_student_style,
    "get_player_overview":       get_player_overview,
    "get_weakness_summary":      get_weakness_summary,
    "get_blunder_examples":      get_blunder_examples,
    "get_time_pressure_stats":   get_time_pressure_stats,
    "get_opening_stats":         get_opening_stats,
    "get_phase_stats":           get_phase_stats,
    "get_clock_usage_by_phase":  get_clock_usage_by_phase,
    "get_performance_by_color":  get_performance_by_color,
    "get_performance_vs_rating": get_performance_vs_rating,
    "get_recent_games":          get_recent_games,
    "analyze_game":              analyze_game,
}


def _object_schema(properties: dict[str, Any], required: list[str]) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": required}


def _string_schema(description: str, *, enum: list[str] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "string", "description": description}
    if enum:
        schema["enum"] = enum
    return schema


def _integer_schema(description: str, *, minimum: int | None = None, maximum: int | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "integer", "description": description}
    if minimum is not None:
        schema["minimum"] = minimum
    if maximum is not None:
        schema["maximum"] = maximum
    return schema


_PLAYER_ID = _string_schema("Lichess username/player id, for example benirks.")
_DAYS = _integer_schema("Lookback window in days. Use 30 for recent form, 60 for default scouting, 365 for all available history.", minimum=1, maximum=365)
_TOP_N = _integer_schema("Maximum number of rows to return.", minimum=1, maximum=20)
_LIMIT = _integer_schema("Maximum number of examples/games to return.", minimum=1, maximum=20)
_PHASE = _string_schema("Game phase filter.", enum=["opening", "middlegame", "endgame"])
_TIME_PRESSURE = _string_schema("Clock pressure filter.", enum=["under_10s", "normal"])


_TOOL_DECLARATIONS: list[dict[str, Any]] = [
    {
        "name": "inspect_student_style",
        "description": "Start here for any player-level coaching question. Returns a compact scouting report: profile, color performance, rating history, phase/opening weaknesses, critical examples, and recent games.",
        "parameters": _object_schema({"player_id": _PLAYER_ID, "days": _DAYS}, ["player_id"]),
    },
    {
        "name": "get_player_overview",
        "description": "Use for high-level volume and result breakdown by time control/speed.",
        "parameters": _object_schema({"player_id": _PLAYER_ID}, ["player_id"]),
    },
    {
        "name": "get_weakness_summary",
        "description": "Use when you need aggregate counts of critical positions, blunders, mistakes, inaccuracies, top phase, and time-pressure weakness.",
        "parameters": _object_schema({"player_id": _PLAYER_ID, "days": _DAYS}, ["player_id"]),
    },
    {
        "name": "get_blunder_examples",
        "description": "Use when you need concrete drill material: game id, ply, played move, best move, phase, classification, and eval swing. Good for exercises and specific recommendations.",
        "parameters": _object_schema(
            {"player_id": _PLAYER_ID, "limit": _LIMIT, "phase": _PHASE, "time_pressure": _TIME_PRESSURE},
            ["player_id"],
        ),
    },
    {
        "name": "get_time_pressure_stats",
        "description": "Use for questions about time pressure, bullet/blitz tempo, low-clock mistakes, or whether the player collapses under the clock. Uses derived aggregates and is safe for interactive chat.",
        "parameters": _object_schema({"player_id": _PLAYER_ID, "days": _DAYS}, ["player_id"]),
    },
    {
        "name": "get_opening_stats",
        "description": "Use for opening/repertoire questions. Returns openings by ECO/name/color with games, win rate, critical positions, blunders, and mistakes.",
        "parameters": _object_schema({"player_id": _PLAYER_ID, "top_n": _TOP_N, "days": _DAYS}, ["player_id"]),
    },
    {
        "name": "get_phase_stats",
        "description": "Use for questions about opening vs middlegame vs endgame weakness, or to decide which phase should become the training priority.",
        "parameters": _object_schema({"player_id": _PLAYER_ID, "days": _DAYS}, ["player_id"]),
    },
    {
        "name": "get_clock_usage_by_phase",
        "description": "Use only when the user specifically asks about clock usage by opening/middlegame/endgame phase. This reads move-level clock data, so prefer get_time_pressure_stats for general time-pressure questions.",
        "parameters": _object_schema({"player_id": _PLAYER_ID, "days": _DAYS}, ["player_id"]),
    },
    {
        "name": "get_performance_by_color",
        "description": "Use for White-vs-Black questions or when color imbalance may explain results.",
        "parameters": _object_schema({"player_id": _PLAYER_ID}, ["player_id"]),
    },
    {
        "name": "get_performance_vs_rating",
        "description": "Use for questions about performance against stronger, equal, or weaker opponents.",
        "parameters": _object_schema({"player_id": _PLAYER_ID}, ["player_id"]),
    },
    {
        "name": "get_recent_games",
        "description": "Use when selecting recent games to review, checking recency, or grounding advice in concrete recent examples.",
        "parameters": _object_schema({"player_id": _PLAYER_ID, "limit": _LIMIT}, ["player_id"]),
    },
    {
        "name": "analyze_game",
        "description": "Use only when the user asks about a specific game id or needs move-by-move analysis. Uses stored analyzer evaluations; it does not run live Stockfish over the full game.",
        "parameters": _object_schema({"game_id": _string_schema("Lichess game id, usually 8-12 alphanumeric characters.")}, ["game_id"]),
    },
]


_TOOL_DECLARATION_NAMES = {tool["name"] for tool in _TOOL_DECLARATIONS}


_TOOL_GUIDE = """Các nguồn dữ liệu có thể xuất hiện trong JSON:
- inspect_student_style: ảnh chụp tổng quan về người chơi gồm phong độ, màu quân, nhóm rating đối thủ, phase yếu, opening yếu, ví dụ lỗi và vài game gần đây. Dùng để chẩn đoán tổng quát.
- get_opening_stats: thống kê khai cuộc theo người chơi, màu quân, số ván, tỷ lệ thắng và số vị trí then chốt. Dùng khi câu hỏi hỏi về repertoire/opening.
- get_phase_stats: lỗi theo khai cuộc, trung cuộc, tàn cuộc. Dùng khi câu hỏi hỏi người chơi yếu giai đoạn nào.
- get_blunder_examples: ví dụ cụ thể từ vị trí then chốt. Dùng để đưa bài tập, game id, ply, nước đã chơi và best move.
- get_time_pressure_stats: dữ liệu áp lực thời gian từ bảng tổng hợp. Dùng cho câu hỏi về cạn giờ, tempo, blitz/bullet hoặc ra quyết định nhanh.
- get_clock_usage_by_phase: thời gian theo opening/middlegame/endgame từ move-level data. Chỉ dùng khi người dùng hỏi cụ thể về cách dùng đồng hồ theo phase.
- get_performance_by_color: hiệu suất cầm Trắng/Đen. Dùng khi câu hỏi hỏi về màu quân.
- get_performance_vs_rating: hiệu suất gặp đối thủ mạnh/yếu/ngang trình. Dùng khi câu hỏi hỏi về matchmaking hoặc rating.
- get_recent_games: danh sách game gần đây. Dùng khi cần chọn game để review.
- analyze_game: phân tích một game cụ thể theo từng nước bằng kết quả analyzer đã lưu sẵn. Không dùng để chạy Stockfish trực tiếp trên toàn bộ game.

Bạn có thể gọi nhiều nguồn dữ liệu trong một lượt nếu cần kiểm chứng chéo. Ví dụ: câu hỏi về opening nên bắt đầu từ inspect_student_style rồi gọi get_opening_stats; câu hỏi về bài tập nên gọi get_blunder_examples; câu hỏi về một game cụ thể nên gọi analyze_game. Luôn truyền đúng tên tham số trong schema của tool.

Không nói tên nguồn dữ liệu ở câu trả lời cuối. Hãy biến chúng thành nhận định cờ vua, ví dụ: "khi cầm Đen", "ở trung cuộc", "trong Philidor Defense", "dưới áp lực thời gian"."""


_SYSTEM_PROMPT = """Bạn là AI Coach cờ vua cho một nền tảng phân tích Lichess có dữ liệu thật.
Trả lời bằng tiếng Việt như một huấn luyện viên cờ vua chuyên nghiệp: rõ ràng, sắc bén, dễ hiểu với người chơi câu lạc bộ. Có thể giữ thuật ngữ chess tiếng Anh khi quen thuộc như opening, middlegame, blunder, tactic, candidate moves, engine.

Quy tắc bắt buộc:
- Bạn không được bịa số liệu. Mọi con số, tỷ lệ thắng, số ván, điểm yếu hoặc nhận định về người chơi phải đến từ tool.
- Nếu chưa gọi tool thì chỉ được nói ở mức phương pháp, không được khẳng định dữ liệu cụ thể.
- Nếu tool không có dữ liệu, nói rõ dữ liệu của người chơi/ván đó chưa có trong hệ thống.
- Không nhắc tên tool, tên bảng, tên hệ thống nội bộ, hoặc nói "tôi sẽ gọi tool". Người dùng chỉ cần thấy insight.
- Không dùng lời khuyên chung chung như "cải thiện kỹ năng phân tích" nếu không kèm bài tập cụ thể.
- Không biến mọi câu hỏi thành một bản báo cáo giống nhau. Trả lời đúng trọng tâm câu hỏi trước.

Workflow khi trả lời:
1. Tự quyết định cần gọi tool nào dựa trên câu hỏi và schema tool được cung cấp. Không cần gọi mọi tool.
2. Nếu câu hỏi có username/người chơi, thường bắt đầu bằng inspect_student_style để lấy bối cảnh trước khi gọi tool chuyên sâu.
3. Nếu câu hỏi có game id hoặc hỏi một ván cụ thể, ưu tiên analyze_game.
4. Nếu chưa có username/game id và chưa có dữ liệu, giải thích phương pháp phân tích và hỏi người dùng cung cấp username hoặc game id.
5. Chẩn đoán bằng cách tìm mẫu lặp lại giữa nhiều nguồn dữ liệu, không chỉ đọc lại bảng.
6. Ưu tiên 1-2 vấn đề có tác động lớn nhất đến kết quả.
7. Nếu dữ liệu có trường coach_brief, xem đó là bản tóm tắt scout report, nhưng chỉ dùng nguyên khung đó khi người dùng hỏi phân tích tổng quan.
8. Nếu phân tích một game cụ thể, tập trung vào turning point, nước thay thế, kế hoạch của hai bên và bài học chiến thuật."""


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


def _extract_player(message: str) -> str | None:
    match = re.search(r"\[Player:\s*([^\]\s]+)\]", message)
    if match:
        return match.group(1).strip()
    match = re.search(r"\b(?:player|người chơi|username|user)\s*[:=]\s*([A-Za-z0-9_-]{3,32})\b", message, re.I)
    if match:
        return match.group(1).strip()
    return None


def _extract_game_id(message: str) -> str | None:
    match = re.search(r"\b(?:game|ván|v[aá]n đấu)\s*[:#]?\s*([A-Za-z0-9]{8,12})\b", message, re.I)
    if match:
        return match.group(1).strip()
    return None


def _history_context(messages: list[dict], max_messages: int = 8, max_chars: int = 5000) -> str:
    relevant = [
        msg for msg in messages
        if msg.get("role") in {"user", "assistant"} and msg.get("content")
    ][:-1]
    if not relevant:
        return ""

    lines = []
    for msg in relevant[-max_messages:]:
        role = "Người dùng" if msg.get("role") == "user" else "Coach"
        content = str(msg.get("content", "")).strip().replace("\n", " ")
        if len(content) > 900:
            content = content[:900] + "..."
        lines.append(f"{role}: {content}")

    text = "\n".join(lines)
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text


def _agent_user_text(message: str, history_context: str = "") -> str:
    history = (
        "Bối cảnh hội thoại gần đây trong cùng session:\n"
        f"{history_context}\n\n"
        if history_context
        else ""
    )
    return (
        history +
        "Yêu cầu người dùng:\n"
        f"{message}\n\n"
        "Nếu yêu cầu hiện tại dùng các từ như 'okay', 'cái đó', 'đi sâu hơn', 'phân tích cụ thể', "
        "hãy dùng bối cảnh hội thoại gần đây để hiểu người dùng đang nhắc tới player/game/opening nào.\n\n"
        "Bạn là agent có quyền gọi tool. Hãy tự chọn tool phù hợp trước khi kết luận.\n\n"
        "Hướng dẫn tool:\n"
        f"{_TOOL_GUIDE}"
    )


def _vertex_agent_payload(contents: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "systemInstruction": {"parts": [{"text": _SYSTEM_PROMPT}]},
        "contents": contents,
        "tools": [{"functionDeclarations": _TOOL_DECLARATIONS}],
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "AUTO",
            }
        },
        "generationConfig": {
            "temperature": 0.25,
            "maxOutputTokens": 2200,
        },
    }


def _function_response_part(name: str, result_json: str) -> dict[str, Any]:
    try:
        response = json.loads(result_json)
    except Exception:
        response = {"result": result_json}
    return {
        "functionResponse": {
            "name": name,
            "response": response,
        }
    }


def _function_response_content(parts: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "role": "user",
        "parts": parts,
    }


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
        fn = globals().get(name, fn)
        try:
            return json.dumps(fn(**args), default=str)
        except Exception as e:
            log.exception("tool %s failed", name)
            return json.dumps({"error": str(e)})

    def _run_vertex_agent(self, message: str) -> Iterator[dict]:
        contents: list[dict[str, Any]] = [
            {"role": "user", "parts": [{"text": _agent_user_text(message, _history_context(self.messages))}]}
        ]

        for _ in range(max(1, COACH_MAX_TOOL_ROUNDS)):
            payload = _vertex_generate_content_payload(_vertex_agent_payload(contents), timeout=45)
            model_content = payload.get("candidates", [{}])[0].get("content", {})
            function_calls = _content_function_calls(model_content)
            if not function_calls:
                final_text = _content_text(model_content)
                if not final_text.strip():
                    raise RuntimeError("Vertex returned an empty agent response")
                self.messages.append({"role": "assistant", "content": final_text})
                yield {"type": "token", "text": final_text}
                yield {"type": "done"}
                return

            contents.append(model_content)
            response_parts: list[dict[str, Any]] = []
            for call in function_calls:
                name = call.get("name")
                args = call.get("args") or {}
                if not isinstance(name, str) or name not in _TOOL_FNS:
                    result_json = json.dumps({"error": f"unknown tool: {name}"})
                elif not isinstance(args, dict):
                    result_json = json.dumps({"error": f"invalid args for tool {name}"})
                else:
                    yield {"type": "tool_start", "name": name, "args": args}
                    result_json = self._dispatch(name, args)
                    yield {"type": "tool_result", "name": name, "summary": _short_summary(name, result_json)}
                    self.messages.append({"role": "tool", "name": name, "content": result_json})
                response_parts.append(_function_response_part(str(name), result_json))
            contents.append(_function_response_content(response_parts))

        contents.append(
            {
                "role": "user",
                "parts": [
                    {
                        "text": (
                            "Bạn đã dùng hết số lượt gọi tool cho phép trong request này. "
                            "Hãy trả lời tốt nhất có thể chỉ dựa trên dữ liệu tool đã có, "
                            "không gọi thêm tool và không bịa số liệu."
                        )
                    }
                ],
            }
        )
        payload = _vertex_generate_content_payload(_vertex_agent_payload(contents), timeout=45)
        model_content = payload.get("candidates", [{}])[0].get("content", {})
        final_text = _content_text(model_content)
        if not final_text.strip():
            raise RuntimeError("Vertex returned an empty final response after tool limit")
        self.messages.append({"role": "assistant", "content": final_text})
        yield {"type": "token", "text": final_text}
        yield {"type": "done"}

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
        message = self.messages[-1].get("content", "")
        if COACH_FINAL_PROVIDER == "vertex":
            yield from self._run_vertex_agent(message)
            return
        if COACH_FINAL_PROVIDER == "gemini":
            final_text = _gemini_final_answer(self.messages)
        else:
            final_text = _vertex_final_answer(self.messages)
        self.messages.append({"role": "assistant", "content": final_text})
        yield {"type": "token", "text": final_text}
        yield {"type": "done"}
        return


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
