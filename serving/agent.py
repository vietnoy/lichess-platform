"""
agent.py — Gemini 2.5 Flash AI Chess Coach

Connects to StarRocks (via polaris_catalog Iceberg external tables) and answers
chess coaching questions using tool use.

Run locally:
  python serving/agent.py

Or as a FastAPI service:
  uvicorn serving.agent:app --host 0.0.0.0 --port 8000
"""

import os
import json
import logging
from typing import Any

import mysql.connector
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

SR_HOST = os.getenv("STARROCKS_HOST", "localhost")
SR_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
SR_USER = os.getenv("STARROCKS_USER", "root")
SR_PASS = os.getenv("STARROCKS_PASSWORD", "")
TABLE   = "polaris_catalog.prod.chess_move_events"


def _sr_conn():
    return mysql.connector.connect(
        host=SR_HOST, port=SR_PORT,
        user=SR_USER, password=SR_PASS,
        database="",
        connection_timeout=10,
    )


def _query(sql: str) -> list[dict]:
    conn = _sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


# ── Tool implementations ───────────────────────────────────────────────────────

def get_player_overview(player_id: str) -> dict:
    """
    Overall stats: total games, win/loss/draw, win rate, avg rating,
    most played time controls.
    """
    sql = f"""
        SELECT
            COUNT(DISTINCT game_id)                                         AS total_games,
            SUM(CASE WHEN winner = whose_moved THEN 1 ELSE 0 END)          AS wins,
            SUM(CASE WHEN winner IS NOT NULL
                      AND winner != whose_moved THEN 1 ELSE 0 END)         AS losses,
            SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END)                AS draws,
            ROUND(AVG(CASE WHEN whose_moved = 'white' THEN white_rating
                           ELSE black_rating END), 0)                      AS avg_rating,
            speed
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND move_number = 1
        GROUP BY speed
        ORDER BY total_games DESC
    """
    rows = _query(sql)
    return {"player_id": player_id, "overview": rows}


def get_time_pressure_stats(player_id: str) -> dict:
    """
    Compare win rate and move count when clock < 10s vs normal.
    High loss rate under time pressure = clock management issue.
    """
    sql = f"""
        SELECT
            CASE WHEN clock_remaining < 1000 THEN 'under_10s' ELSE 'normal' END AS pressure,
            COUNT(DISTINCT game_id)                                               AS games,
            SUM(CASE WHEN winner = whose_moved THEN 1 ELSE 0 END)                AS wins,
            ROUND(SUM(CASE WHEN winner = whose_moved THEN 1 ELSE 0 END) * 100.0
                  / COUNT(DISTINCT game_id), 1)                                  AS win_rate_pct,
            ROUND(AVG(clock_remaining) / 100.0, 1)                              AS avg_clock_s
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND clock_remaining IS NOT NULL
        GROUP BY pressure
    """
    rows = _query(sql)
    return {"player_id": player_id, "time_pressure": rows}


def get_opening_stats(player_id: str, top_n: int = 8) -> dict:
    """
    Win rate by opening ECO code. Shows which openings the player
    performs best and worst in.
    """
    sql = f"""
        SELECT
            opening_eco,
            opening_name,
            COUNT(DISTINCT game_id)                                        AS games,
            SUM(CASE WHEN winner = whose_moved THEN 1 ELSE 0 END)         AS wins,
            ROUND(SUM(CASE WHEN winner = whose_moved THEN 1 ELSE 0 END)
                  * 100.0 / COUNT(DISTINCT game_id), 1)                   AS win_rate_pct
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND move_number = 1
          AND opening_eco IS NOT NULL
        GROUP BY opening_eco, opening_name
        HAVING games >= 3
        ORDER BY win_rate_pct ASC
        LIMIT {top_n}
    """
    rows = _query(sql)
    return {"player_id": player_id, "opening_stats": rows}


def get_endgame_clock_usage(player_id: str) -> dict:
    """
    How much clock time does the player have left in the endgame (move > 30)?
    Compares avg clock remaining early vs late game to detect time trouble pattern.
    """
    sql = f"""
        SELECT
            CASE
                WHEN move_number <= 10 THEN 'opening'
                WHEN move_number <= 30 THEN 'middlegame'
                ELSE 'endgame'
            END AS phase,
            ROUND(AVG(clock_remaining) / 100.0, 1) AS avg_clock_s,
            ROUND(MIN(clock_remaining) / 100.0, 1) AS min_clock_s,
            COUNT(*)                                AS move_count
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND clock_remaining IS NOT NULL
        GROUP BY phase
        ORDER BY move_count DESC
    """
    rows = _query(sql)
    return {"player_id": player_id, "clock_by_phase": rows}


def get_recent_games(player_id: str, limit: int = 10) -> dict:
    """
    Last N games with result, opponent, opening, and time control.
    """
    sql = f"""
        SELECT
            game_id,
            CASE WHEN white_id = '{player_id}' THEN black_id ELSE white_id END AS opponent,
            CASE WHEN white_id = '{player_id}' THEN white_rating ELSE black_rating END AS my_rating,
            CASE WHEN white_id = '{player_id}' THEN black_rating ELSE white_rating END AS opp_rating,
            opening_eco,
            opening_name,
            speed,
            winner,
            end_status,
            date
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND move_number = 1
        ORDER BY date DESC
        LIMIT {limit}
    """
    rows = _query(sql)
    return {"player_id": player_id, "recent_games": rows}


def get_game_moves(game_id: str) -> dict:
    """
    Full move list for a specific game with clock remaining per move.
    Useful for exploring a specific game step by step.
    """
    sql = f"""
        SELECT
            move_number,
            whose_moved,
            move,
            fen,
            ROUND(clock_remaining / 100.0, 1) AS clock_s
        FROM {TABLE}
        WHERE game_id = '{game_id}'
        ORDER BY move_number
    """
    moves = _query(sql)

    sql_meta = f"""
        SELECT white_id, black_id, white_rating, black_rating,
               speed, opening_eco, opening_name, winner, end_status
        FROM {TABLE}
        WHERE game_id = '{game_id}' AND move_number = 1
    """
    meta = _query(sql_meta)
    return {"game_id": game_id, "meta": meta[0] if meta else {}, "moves": moves}


# ── Gemini tool declarations ───────────────────────────────────────────────────

TOOLS = [
    {
        "name": "get_player_overview",
        "description": "Get overall stats for a player: total games, win/loss/draw, average rating, broken down by time control (bullet/blitz/rapid).",
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_time_pressure_stats",
        "description": "Compare a player's win rate when the clock is under 10 seconds vs normal conditions. Reveals clock management weaknesses.",
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_opening_stats",
        "description": "Win rate by opening ECO code. Shows which openings the player performs best and worst in.",
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
                "top_n":     {"type": "integer", "description": "Number of openings to return (default 8)"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_endgame_clock_usage",
        "description": "Shows average clock remaining in opening/middlegame/endgame phases. Detects if a player runs out of time in the endgame.",
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_recent_games",
        "description": "Get the last N games for a player with opponent, opening, result and time control.",
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
                "limit":     {"type": "integer", "description": "Number of games to return (default 10)"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_game_moves",
        "description": "Get the full move list for a specific game with clock time per move. Use this to explore a game step by step.",
        "parameters": {
            "type": "object",
            "properties": {
                "game_id": {"type": "string", "description": "Lichess game ID (8 chars, e.g. 'AbCdEfGh')"},
            },
            "required": ["game_id"],
        },
    },
]

TOOL_FN_MAP: dict[str, Any] = {
    "get_player_overview":     get_player_overview,
    "get_time_pressure_stats": get_time_pressure_stats,
    "get_opening_stats":       get_opening_stats,
    "get_endgame_clock_usage": get_endgame_clock_usage,
    "get_recent_games":        get_recent_games,
    "get_game_moves":          get_game_moves,
}

SYSTEM_PROMPT = """You are an expert AI Chess Coach with access to real game data from Lichess.
You help players improve by analysing their actual game history — not generic advice.

When a player asks about their weaknesses or performance, use the available tools to pull
real statistics from the database before answering. Always ground your advice in the data.
Be specific: quote win rates, clock times, mention exact openings where they struggle.

Key metrics you can analyse:
- Overall win/loss/draw rate by time control
- Time pressure: performance when clock < 10 seconds vs normal
- Opening repertoire: which ECO codes they win/lose most
- Clock usage across game phases: do they rush in the endgame?

If the database returns no data for a player, tell them their games may not be
in the system yet.

Keep answers concise and actionable. Use bullet points for improvement tips."""


# ── Agent loop ─────────────────────────────────────────────────────────────────

class ChessCoachAgent:
    def __init__(self):
        self.model = genai.GenerativeModel(
            model_name="gemini-2.5-flash-preview-04-17",
            system_instruction=SYSTEM_PROMPT,
            tools=[{"function_declarations": TOOLS}],
        )
        self.chat = self.model.start_chat()

    def _dispatch_tool(self, name: str, args: dict) -> str:
        fn = TOOL_FN_MAP.get(name)
        if fn is None:
            return json.dumps({"error": f"unknown tool: {name}"})
        try:
            result = fn(**args)
            return json.dumps(result, default=str)
        except Exception as e:
            logger.error(f"Tool {name} failed: {e}")
            return json.dumps({"error": str(e)})

    def ask(self, user_message: str) -> str:
        response = self.chat.send_message(user_message)

        while True:
            part = response.candidates[0].content.parts[0]

            if hasattr(part, "function_call") and part.function_call.name:
                fc   = part.function_call
                args = dict(fc.args)
                logger.info(f"Tool call: {fc.name}({args})")
                result_str = self._dispatch_tool(fc.name, args)
                logger.info(f"Tool result: {result_str[:300]}")

                response = self.chat.send_message(
                    genai.protos.Content(
                        parts=[genai.protos.Part(
                            function_response=genai.protos.FunctionResponse(
                                name=fc.name,
                                response={"result": result_str},
                            )
                        )],
                        role="tool",
                    )
                )
            else:
                return response.text


# ── FastAPI ────────────────────────────────────────────────────────────────────

try:
    from fastapi import FastAPI
    from pydantic import BaseModel

    app = FastAPI(title="Chess Coach Agent")
    _agent = ChessCoachAgent()

    class ChatRequest(BaseModel):
        message: str

    class ChatResponse(BaseModel):
        reply: str

    @app.post("/chat", response_model=ChatResponse)
    def chat(req: ChatRequest):
        reply = _agent.ask(req.message)
        return ChatResponse(reply=reply)

    @app.get("/health")
    def health():
        return {"status": "ok"}

except ImportError:
    app = None


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Chess Coach Agent — type 'quit' to exit\n")
    agent = ChessCoachAgent()
    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break
        if not user_input or user_input.lower() in {"quit", "exit"}:
            break
        reply = agent.ask(user_input)
        print(f"\nCoach: {reply}\n")
