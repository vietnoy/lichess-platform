"""
agent.py — Gemini 2.5 Flash AI Chess Coach

Connects to StarRocks (via polaris_catalog Iceberg external tables) and answers
chess coaching questions using tool use. The agent can:
  - Look up a player's historical blunder/mistake rate
  - Find the most common mistake positions for a player
  - Recommend what openings a player struggles against
  - Analyse a specific game by game_id

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

# ── Gemini ────────────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

# ── StarRocks ─────────────────────────────────────────────────────────────────
SR_HOST = os.getenv("STARROCKS_HOST", "localhost")
SR_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
SR_USER = os.getenv("STARROCKS_USER", "root")
SR_PASS = os.getenv("STARROCKS_PASSWORD", "")
SR_DB   = "polaris_catalog.prod"   # external catalog.namespace


def _sr_conn():
    return mysql.connector.connect(
        host=SR_HOST, port=SR_PORT,
        user=SR_USER, password=SR_PASS,
        database="",
        connection_timeout=10,
    )


def _query(sql: str) -> list[dict]:
    """Run a read-only SQL query against StarRocks and return rows as dicts."""
    conn = _sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


# ── Tool implementations ───────────────────────────────────────────────────────

def get_player_stats(player_id: str, limit_days: int = 30) -> dict:
    """
    Return move classification breakdown for a player over the last N days.
    Shows how often they blunder, make mistakes, or play excellent moves.
    """
    sql = f"""
        SELECT
            m.classification,
            COUNT(*) AS cnt,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM {SR_DB}.moves m
        JOIN {SR_DB}.game_start gs ON m.game_id = gs.game_id
        WHERE (gs.white_id = '{player_id}' OR gs.black_id = '{player_id}')
          AND gs.timestamp >= DATE_SUB(NOW(), INTERVAL {limit_days} DAY)
          AND m.classification IS NOT NULL
        GROUP BY m.classification
        ORDER BY cnt DESC
    """
    rows = _query(sql)
    return {"player_id": player_id, "days": limit_days, "breakdown": rows}


def get_blunder_positions(player_id: str, top_n: int = 5) -> dict:
    """
    Return the N positions (FEN) where a player most often blunders,
    along with the best move they should have played.
    """
    sql = f"""
        SELECT
            m.fen,
            m.move           AS played_move,
            m.best_move,
            m.eval_delta,
            COUNT(*)         AS times
        FROM {SR_DB}.moves m
        JOIN {SR_DB}.game_start gs ON m.game_id = gs.game_id
        WHERE (gs.white_id = '{player_id}' OR gs.black_id = '{player_id}')
          AND m.classification = 'blunder'
          AND m.fen IS NOT NULL
        GROUP BY m.fen, m.move, m.best_move, m.eval_delta
        ORDER BY times DESC, ABS(m.eval_delta) DESC
        LIMIT {top_n}
    """
    rows = _query(sql)
    return {"player_id": player_id, "blunder_positions": rows}


def get_opening_weaknesses(player_id: str, top_n: int = 5) -> dict:
    """
    Return openings (by first 4 moves) where a player's blunder rate is highest.
    Helps identify which opening systems are problematic.
    """
    sql = f"""
        SELECT
            first_moves,
            total_moves,
            blunders,
            ROUND(blunders * 100.0 / total_moves, 2) AS blunder_rate_pct
        FROM (
            SELECT
                SUBSTRING_INDEX(GROUP_CONCAT(m.move ORDER BY m.move_number SEPARATOR ' '), ' ', 4) AS first_moves,
                COUNT(*)                                                              AS total_moves,
                SUM(CASE WHEN m.classification = 'blunder' THEN 1 ELSE 0 END)        AS blunders
            FROM {SR_DB}.moves m
            JOIN {SR_DB}.game_start gs ON m.game_id = gs.game_id
            WHERE (gs.white_id = '{player_id}' OR gs.black_id = '{player_id}')
              AND m.move_number <= 20
            GROUP BY gs.game_id
        ) t
        WHERE total_moves >= 10
        GROUP BY first_moves
        ORDER BY blunder_rate_pct DESC
        LIMIT {top_n}
    """
    rows = _query(sql)
    return {"player_id": player_id, "opening_weaknesses": rows}


def get_game_analysis(game_id: str) -> dict:
    """
    Return move-by-move analysis for a specific game, including eval swing
    and classification for each move.
    """
    sql = f"""
        SELECT
            m.move_number,
            m.move,
            m.fen,
            m.eval_cp,
            m.eval_delta,
            m.best_move,
            m.classification,
            m.time_spent_s,
            m.time_pressure
        FROM {SR_DB}.moves m
        WHERE m.game_id = '{game_id}'
        ORDER BY m.move_number
    """
    rows = _query(sql)

    sql_meta = f"""
        SELECT white_id, black_id, speed, rated, variant
        FROM {SR_DB}.game_start
        WHERE game_id = '{game_id}'
    """
    meta = _query(sql_meta)
    return {"game_id": game_id, "meta": meta[0] if meta else {}, "moves": rows}


def get_time_pressure_stats(player_id: str) -> dict:
    """
    Show how a player performs under time pressure vs normal conditions.
    High blunder rate under time pressure = clock management issue.
    """
    sql = f"""
        SELECT
            m.time_pressure,
            m.classification,
            COUNT(*) AS cnt
        FROM {SR_DB}.moves m
        JOIN {SR_DB}.game_start gs ON m.game_id = gs.game_id
        WHERE (gs.white_id = '{player_id}' OR gs.black_id = '{player_id}')
          AND m.classification IS NOT NULL
          AND m.time_pressure IS NOT NULL
        GROUP BY m.time_pressure, m.classification
        ORDER BY m.time_pressure, cnt DESC
    """
    rows = _query(sql)
    return {"player_id": player_id, "time_pressure_stats": rows}


# ── Gemini tool declarations ───────────────────────────────────────────────────

TOOLS = [
    {
        "name": "get_player_stats",
        "description": (
            "Get a breakdown of move quality (blunder/mistake/inaccuracy/good/excellent) "
            "for a specific Lichess player over the last N days."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "player_id":  {"type": "string", "description": "Lichess username"},
                "limit_days": {"type": "integer", "description": "Look-back window in days (default 30)"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_blunder_positions",
        "description": (
            "Find the positions (FEN) where a player most often blunders, "
            "along with the best move they should have played instead."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
                "top_n":     {"type": "integer", "description": "Number of positions to return (default 5)"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_opening_weaknesses",
        "description": (
            "Identify which openings a player struggles in the most, "
            "ranked by blunder rate during the first 20 moves."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
                "top_n":     {"type": "integer", "description": "Number of openings to return (default 5)"},
            },
            "required": ["player_id"],
        },
    },
    {
        "name": "get_game_analysis",
        "description": "Get move-by-move analysis for a specific game ID, including centipawn eval and classification.",
        "parameters": {
            "type": "object",
            "properties": {
                "game_id": {"type": "string", "description": "Lichess game ID (8 chars, e.g. 'AbCdEfGh')"},
            },
            "required": ["game_id"],
        },
    },
    {
        "name": "get_time_pressure_stats",
        "description": (
            "Show how a player's move quality changes under time pressure "
            "(less than 30s on clock) vs normal conditions."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "player_id": {"type": "string", "description": "Lichess username"},
            },
            "required": ["player_id"],
        },
    },
]

TOOL_FN_MAP: dict[str, Any] = {
    "get_player_stats":       get_player_stats,
    "get_blunder_positions":  get_blunder_positions,
    "get_opening_weaknesses": get_opening_weaknesses,
    "get_game_analysis":      get_game_analysis,
    "get_time_pressure_stats": get_time_pressure_stats,
}

SYSTEM_PROMPT = """You are an expert AI Chess Coach with access to real game data
from Lichess. You help players improve by analysing their actual game history —
not generic advice.

When a player asks about their weaknesses or a specific game, use the available
tools to pull real statistics from the database before answering. Always ground
your advice in the data. Be specific: quote the blunder rate, mention the exact
positions or openings where they struggle.

If the database returns no data for a player, tell them their games may not be
in the system yet and suggest they play more rated games on Lichess.

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

        # Agentic loop: keep calling tools until model returns text
        while True:
            part = response.candidates[0].content.parts[0]

            # If it's a function call, execute and feed back result
            if hasattr(part, "function_call") and part.function_call.name:
                fc   = part.function_call
                args = dict(fc.args)
                logger.info(f"Tool call: {fc.name}({args})")
                result_str = self._dispatch_tool(fc.name, args)
                logger.info(f"Tool result: {result_str[:200]}")

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
                # Model returned text — we're done
                return response.text


# ── FastAPI wrapper (optional) ─────────────────────────────────────────────────

try:
    from fastapi import FastAPI
    from pydantic import BaseModel

    app = FastAPI(title="Chess Coach Agent")
    _agent = ChessCoachAgent()

    class ChatRequest(BaseModel):
        message: str
        session_id: str | None = None   # future: per-session history

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
    app = None   # FastAPI not installed — CLI mode only


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Chess Coach Agent (Gemini 2.5 Flash) — type 'quit' to exit\n")
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
