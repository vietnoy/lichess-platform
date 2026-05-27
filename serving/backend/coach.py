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
    query_opening_stats,
    query_phase_stats,
    query_player_profile,
    query_weakness_summary,
)
from stockfish import eval_fen

log = logging.getLogger("coach")

COACH_FINAL_PROVIDER = os.getenv("COACH_FINAL_PROVIDER", "vertex").lower()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GCP_PROJECT = os.getenv("GCP_PROJECT", "")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")
VERTEX_MODEL = os.getenv("VERTEX_MODEL", "gemini-2.5-flash")


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
    payload = response.json()
    parts = (
        payload.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    text = "".join(part.get("text", "") for part in parts)
    if not text.strip():
        raise RuntimeError("Vertex returned an empty response")
    return text


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


def get_time_pressure_stats(player_id: str) -> dict:
    # Time-pressure needs move-level clock data — stays on chess_move_events,
    # but narrows by the dates this player actually played to prune partitions.
    date_rows = _q(
        f"SELECT DISTINCT date FROM {PLAYER_GAMES} WHERE player_id = %s",
        (player_id,),
    )
    if not date_rows:
        return {"player_id": player_id, "time_pressure": []}
    dates = [r["date"].isoformat() for r in date_rows if r["date"]]
    date_ph = ",".join(["%s"] * len(dates))
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
            WHERE date IN ({date_ph})
              AND (white_id=%s OR black_id=%s)
              AND clock_remaining IS NOT NULL
              AND move_number=1
        ) t
        GROUP BY pressure
        """,
        (player_id, player_id, player_id, player_id) + tuple(dates) + (player_id, player_id),
    )
    return {"player_id": player_id, "time_pressure": rows}


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


def get_clock_usage_by_phase(player_id: str) -> dict:
    # Move-level clock data — narrow by dates this player played.
    date_rows = _q(
        f"SELECT DISTINCT date, game_id FROM {PLAYER_GAMES} WHERE player_id = %s",
        (player_id,),
    )
    if not date_rows:
        return {"player_id": player_id, "clock_by_phase": []}
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
    return {"player_id": player_id, "clock_by_phase": rows}


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


_SYSTEM_PROMPT = """Bạn là AI Coach cờ vua cho một nền tảng phân tích Lichess có dữ liệu thật.
Trả lời bằng tiếng Việt, dễ hiểu với người chơi cờ, nhưng vẫn đủ rõ để showcase năng lực data engineering của hệ thống.

Quy tắc bắt buộc:
- Bạn không được bịa số liệu. Mọi con số, tỷ lệ thắng, số ván, điểm yếu hoặc nhận định về người chơi phải đến từ tool.
- Nếu chưa gọi tool thì chỉ được nói ở mức phương pháp, không được khẳng định dữ liệu cụ thể.
- Nếu tool không có dữ liệu, nói rõ dữ liệu của người chơi/ván đó chưa có trong hệ thống.
- Không nhắc tên tool, tên bảng, tên hệ thống nội bộ, hoặc nói "tôi sẽ gọi tool". Người dùng chỉ cần thấy insight.
- Không dùng lời khuyên chung chung như "cải thiện kỹ năng phân tích" nếu không kèm bài tập cụ thể.

Workflow khi trả lời:
1. Nếu dữ liệu người chơi hoặc ván cờ đã được cung cấp, dùng dữ liệu đó làm nguồn sự thật duy nhất.
2. Nếu chưa có dữ liệu, giải thích phương pháp phân tích và hỏi người dùng cung cấp username hoặc game id.
3. Chẩn đoán bằng cách tìm mẫu lặp lại giữa nhiều nguồn dữ liệu, không chỉ đọc lại bảng.
4. Ưu tiên 1-2 vấn đề có tác động lớn nhất đến kết quả.
5. Nếu tool trả về coach_brief, dùng coach_brief làm khung chính và chỉ diễn đạt lại cho tự nhiên hơn.
6. Trả lời đúng 3 phần:
   - Chẩn đoán chính: 2-3 câu, nói thẳng vấn đề lớn nhất.
   - Bằng chứng từ dữ liệu: 3-5 bullet có số liệu cụ thể.
   - Bài tập tiếp theo: 3 bullet hành động cụ thể, có phase/opening/game/example nếu dữ liệu có.
7. Viết như huấn luyện viên: trực tiếp, cụ thể, có nước đi/giai đoạn/kế hoạch luyện tập khi có dữ liệu."""


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
        player = _extract_player(message)
        game_id = _extract_game_id(message)

        if player:
            args = {"player_id": player}
            yield {"type": "tool_start", "name": "inspect_student_style", "args": args}
            result_json = self._dispatch("inspect_student_style", args)
            yield {"type": "tool_result", "name": "inspect_student_style", "summary": _short_summary("inspect_student_style", result_json)}
            self.messages.append({"role": "tool", "name": "inspect_student_style", "content": result_json})
        elif game_id:
            args = {"game_id": game_id}
            yield {"type": "tool_start", "name": "analyze_game", "args": args}
            result_json = self._dispatch("analyze_game", args)
            yield {"type": "tool_result", "name": "analyze_game", "summary": _short_summary("analyze_game", result_json)}
            self.messages.append({"role": "tool", "name": "analyze_game", "content": result_json})

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
