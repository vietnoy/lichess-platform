import os
import json
import logging
import requests
import mysql.connector
from typing import Any
from dotenv import load_dotenv
import vertexai
from google.oauth2 import service_account
from vertexai.generative_models import GenerativeModel, Part, Tool, FunctionDeclaration

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
logger = logging.getLogger(__name__)

_sa_path  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
_project  = os.getenv("GCP_PROJECT", "sonat-game-new-installs-dash")
_location = os.getenv("GCP_LOCATION", "us-central1")

_creds = service_account.Credentials.from_service_account_file(
    _sa_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
vertexai.init(project=_project, location=_location, credentials=_creds)

SR_HOST         = os.getenv("STARROCKS_HOST", "localhost")
SR_PORT         = int(os.getenv("STARROCKS_PORT", "9030"))
SR_USER         = os.getenv("STARROCKS_USER", "root")
SR_PASS         = os.getenv("STARROCKS_PASSWORD", "")
STOCKFISH_URL   = os.getenv("STOCKFISH_URL", "http://stockfish:8001/eval")
STOCKFISH_DEPTH = int(os.getenv("STOCKFISH_DEPTH", "8"))
TABLE           = "polaris_catalog.prod.chess_move_events"


def query(sql: str) -> list[dict]:
    conn = mysql.connector.connect(
        host=SR_HOST, port=SR_PORT,
        user=SR_USER, password=SR_PASS,
        database="", connection_timeout=10,
    )
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(sql)
        return cur.fetchall()
    finally:
        conn.close()


def stockfish_eval(fen: str) -> dict | None:
    try:
        r = requests.get(STOCKFISH_URL, params={"fen": fen, "depth": STOCKFISH_DEPTH}, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def get_player_overview(player_id: str) -> dict:
    rows = query(f"""
        SELECT
            speed,
            COUNT(DISTINCT game_id)  AS total_games,
            SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END)   AS wins,
            SUM(CASE WHEN winner IS NOT NULL
                      AND winner != CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END)   AS losses,
            SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws,
            ROUND(AVG(CASE WHEN white_id = '{player_id}' THEN white_rating ELSE black_rating END), 0) AS avg_rating
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND move_number = 1
        GROUP BY speed
        ORDER BY total_games DESC
    """)
    return {"player_id": player_id, "overview": rows}


def get_time_pressure_stats(player_id: str) -> dict:
    rows = query(f"""
        SELECT
            CASE WHEN clock_remaining < 1000 THEN 'under_10s' ELSE 'normal' END AS pressure,
            COUNT(DISTINCT game_id)                                               AS games,
            SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END)                                                AS wins,
            ROUND(SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1)         AS win_rate_pct,
            ROUND(AVG(clock_remaining) / 100.0, 1)                               AS avg_clock_s
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND clock_remaining IS NOT NULL
          AND move_number = 1
        GROUP BY pressure
    """)
    return {"player_id": player_id, "time_pressure": rows}


def get_opening_stats(player_id: str, top_n: int = 10) -> dict:
    rows = query(f"""
        SELECT
            opening_eco,
            opening_name,
            COUNT(DISTINCT game_id)  AS games,
            SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END)   AS wins,
            ROUND(SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_rate_pct
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND move_number = 1
          AND opening_eco IS NOT NULL
        GROUP BY opening_eco, opening_name
        HAVING games >= 2
        ORDER BY games DESC
        LIMIT {top_n}
    """)
    return {"player_id": player_id, "opening_stats": rows}


def get_clock_usage_by_phase(player_id: str) -> dict:
    rows = query(f"""
        SELECT
            CASE
                WHEN move_number <= 10 THEN 'opening'
                WHEN move_number <= 30 THEN 'middlegame'
                ELSE 'endgame'
            END                                        AS phase,
            ROUND(AVG(clock_remaining) / 100.0, 1)    AS avg_clock_s,
            ROUND(MIN(clock_remaining) / 100.0, 1)    AS min_clock_s,
            COUNT(*)                                   AS move_count
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND clock_remaining IS NOT NULL
        GROUP BY phase
        ORDER BY phase
    """)
    return {"player_id": player_id, "clock_by_phase": rows}


def get_performance_by_color(player_id: str) -> dict:
    rows = query(f"""
        SELECT
            CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END AS color,
            COUNT(DISTINCT game_id)  AS games,
            SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END)   AS wins,
            ROUND(SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_rate_pct
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND move_number = 1
        GROUP BY color
    """)
    return {"player_id": player_id, "by_color": rows}


def get_performance_vs_rating(player_id: str) -> dict:
    rows = query(f"""
        SELECT
            CASE
                WHEN (CASE WHEN white_id = '{player_id}' THEN black_rating ELSE white_rating END)
                     < (CASE WHEN white_id = '{player_id}' THEN white_rating ELSE black_rating END) - 100
                THEN 'lower_rated'
                WHEN (CASE WHEN white_id = '{player_id}' THEN black_rating ELSE white_rating END)
                     > (CASE WHEN white_id = '{player_id}' THEN white_rating ELSE black_rating END) + 100
                THEN 'higher_rated'
                ELSE 'equal_rated'
            END                                                           AS opponent_class,
            COUNT(DISTINCT game_id)  AS games,
            SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END)   AS wins,
            ROUND(SUM(CASE WHEN winner = CASE WHEN white_id = '{player_id}' THEN 'white' ELSE 'black' END
                THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_rate_pct
        FROM {TABLE}
        WHERE (white_id = '{player_id}' OR black_id = '{player_id}')
          AND move_number = 1
        GROUP BY opponent_class
    """)
    return {"player_id": player_id, "vs_rating": rows}


def get_recent_games(player_id: str, limit: int = 10) -> dict:
    rows = query(f"""
        SELECT
            game_id,
            CASE WHEN white_id = '{player_id}' THEN black_id ELSE white_id END  AS opponent,
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
    """)
    return {"player_id": player_id, "recent_games": rows}


def analyze_game(game_id: str) -> dict:
    moves = query(f"""
        SELECT move_number, whose_moved, move, fen,
               ROUND(clock_remaining / 100.0, 1) AS clock_s,
               white_id, black_id, white_rating, black_rating,
               opening_name, speed, winner, end_status
        FROM {TABLE}
        WHERE game_id = '{game_id}'
        ORDER BY move_number
    """)

    if not moves:
        return {"error": f"Game {game_id} not found"}

    annotated = []
    prev_cp = None

    for m in moves:
        ev        = stockfish_eval(m["fen"])
        cp        = ev.get("cp")       if ev else None
        best_move = ev.get("best_move") if ev else None

        delta = None
        classification = None
        if cp is not None and prev_cp is not None:
            if m["whose_moved"] == "white":
                drop = -(cp - prev_cp)
            else:
                drop = prev_cp - cp

            if drop >= 200:
                classification = "blunder"
            elif drop >= 100:
                classification = "mistake"
            elif drop >= 50:
                classification = "inaccuracy"
            else:
                classification = "good"
            delta = cp - prev_cp

        annotated.append({
            "move_number":    m["move_number"],
            "whose_moved":    m["whose_moved"],
            "move":           m["move"],
            "eval_cp":        cp,
            "eval_delta":     delta,
            "best_move":      best_move,
            "classification": classification,
            "clock_s":        m["clock_s"],
        })
        prev_cp = cp

    meta = moves[0]
    return {
        "game_id":      game_id,
        "white":        f"{meta['white_id']} ({meta['white_rating']})",
        "black":        f"{meta['black_id']} ({meta['black_rating']})",
        "opening":      meta["opening_name"],
        "speed":        meta["speed"],
        "result":       meta["winner"] or "draw",
        "end_status":   meta["end_status"],
        "moves":        annotated,
        "total_moves":  len(annotated),
        "blunders":     sum(1 for x in annotated if x["classification"] == "blunder"),
        "mistakes":     sum(1 for x in annotated if x["classification"] == "mistake"),
        "inaccuracies": sum(1 for x in annotated if x["classification"] == "inaccuracy"),
    }


_player_id_param = {"player_id": {"type_": "STRING", "description": "Lichess username"}}

TOOLS = Tool(function_declarations=[
    FunctionDeclaration(
        name="get_player_overview",
        description="Overall stats for a player: total games, win/loss/draw and average rating by time control.",
        parameters={"type_": "OBJECT", "properties": _player_id_param, "required": ["player_id"]},
    ),
    FunctionDeclaration(
        name="get_time_pressure_stats",
        description="Win rate when clock is under 10 seconds vs normal. Reveals time management weaknesses.",
        parameters={"type_": "OBJECT", "properties": _player_id_param, "required": ["player_id"]},
    ),
    FunctionDeclaration(
        name="get_opening_stats",
        description="Win rate by opening ECO code. Shows which openings the player wins and loses most.",
        parameters={
            "type_": "OBJECT",
            "properties": {
                "player_id": {"type_": "STRING", "description": "Lichess username"},
                "top_n":     {"type_": "INTEGER", "description": "Number of openings to return (default 10)"},
            },
            "required": ["player_id"],
        },
    ),
    FunctionDeclaration(
        name="get_clock_usage_by_phase",
        description="Average clock remaining in opening, middlegame and endgame. Detects if player runs out of time late.",
        parameters={"type_": "OBJECT", "properties": _player_id_param, "required": ["player_id"]},
    ),
    FunctionDeclaration(
        name="get_performance_by_color",
        description="Win rate as white vs black. Reveals color-specific weaknesses.",
        parameters={"type_": "OBJECT", "properties": _player_id_param, "required": ["player_id"]},
    ),
    FunctionDeclaration(
        name="get_performance_vs_rating",
        description="Win rate against lower, equal and higher rated opponents.",
        parameters={"type_": "OBJECT", "properties": _player_id_param, "required": ["player_id"]},
    ),
    FunctionDeclaration(
        name="get_recent_games",
        description="Last N games with opponent, opening, result and time control.",
        parameters={
            "type_": "OBJECT",
            "properties": {
                "player_id": {"type_": "STRING", "description": "Lichess username"},
                "limit":     {"type_": "INTEGER", "description": "Number of games (default 10)"},
            },
            "required": ["player_id"],
        },
    ),
    FunctionDeclaration(
        name="analyze_game",
        description=(
            "Full move-by-move Stockfish analysis of a specific game. "
            "Returns eval, best move, and classification (blunder/mistake/inaccuracy/good) for every move. "
            "Use this when the player asks to review a specific game or understand what went wrong."
        ),
        parameters={
            "type_": "OBJECT",
            "properties": {"game_id": {"type_": "STRING", "description": "Lichess game ID (8 chars, e.g. 'RPJr6MMX')"}},
            "required": ["game_id"],
        },
    ),
])

TOOL_FN_MAP: dict[str, Any] = {
    "get_player_overview":       get_player_overview,
    "get_time_pressure_stats":   get_time_pressure_stats,
    "get_opening_stats":         get_opening_stats,
    "get_clock_usage_by_phase":  get_clock_usage_by_phase,
    "get_performance_by_color":  get_performance_by_color,
    "get_performance_vs_rating": get_performance_vs_rating,
    "get_recent_games":          get_recent_games,
    "analyze_game":              analyze_game,
}

SYSTEM_PROMPT = """You are an elite AI Chess Coach with access to a full statistical database of real Lichess games.
You think like a combination of a grandmaster analyst and a sports psychologist — someone who reads data the way a doctor reads test results: not just reporting what the numbers say, but diagnosing what is actually wrong and why.

## Your core purpose
Surface hidden patterns that the player cannot see themselves. Raw results alone are misleading — a player might have a winning record but be one mistake away from collapse every game. Your job is to go deeper:
- What phase of the game does this player consistently lose? Opening preparation gap, middlegame calculation, or endgame technique?
- Is there a color imbalance? Some players are built for attacking as white but have no plan when forced to defend as black.
- Are the losses concentrated in specific openings? A player might be strong overall but have one or two opening traps they consistently fall into that explain most of their losses.
- Does this player deteriorate under time pressure, or are they equally strong in scrambles? Time pressure reveals a lot about instinctive pattern recognition.
- How does this player perform against weaker vs stronger opponents? Consistent underperformance against lower-rated players signals overconfidence or lack of focus. A mental block against higher-rated players is different — it suggests psychological intimidation rather than skill gap.
- What does the recent trend look like? Is the player improving, declining, or stuck in a plateau?

## Tools available to you

- **get_player_overview** — Total games, wins/losses/draws and average rating by time control. The foundation — understand who this player is and how much data we have on them.

- **get_performance_by_color** — Win rate as white vs black. Critical for diagnosing color imbalance. If white win rate >> black win rate, the player likely depends on initiative and struggles when they have to react.

- **get_opening_stats** — Win rate per opening ECO. This is where many hidden weaknesses live. Players often don't realize they consistently lose from certain positions. Look for openings with many games but low win rate — those are structural problems, not bad luck.

- **get_clock_usage_by_phase** — Average seconds remaining in opening, middlegame, endgame. A player spending too long in the opening is memorizing instead of understanding. A player running out in the endgame is avoiding calculation. The clock tells the truth about where their thinking is hardest.

- **get_time_pressure_stats** — Win rate with normal time vs under 10 seconds. If win rate collapses under pressure, the player's calculation depends on clock — they can't trust their instincts. If win rate holds, they have strong pattern recognition.

- **get_performance_vs_rating** — Win rate vs lower/equal/higher rated. Losing to higher-rated is normal; losing to lower-rated or drawing too much against weaker opponents is a red flag for inconsistency or mental lapses.

- **get_recent_games** — Last games with results and openings. Use this to contextualize: are recent losses all in the same opening? Is the player on a streak in one direction? Context matters for coaching.

- **analyze_game** — Full Stockfish move-by-move evaluation for a specific game ID. Use this to find the exact moment the game was decided, classify every move, and explain what the player should have done. This is the most powerful tool for post-game review.

## How to think and respond

**Step 1 — Gather broadly.** Use as many tools as the question warrants before writing a single word. There is no penalty for extra tool calls. The richer your data, the better your coaching.

**Step 2 — Find the intersection.** The most powerful insights come from patterns that appear across multiple tools. Example: low black win rate + high clock usage in middlegame + weak win rate under time pressure = player struggles in complex defensive positions and runs out of time trying to find a plan. That is a real diagnosis, not a list of facts.

**Step 3 — Rank by impact.** Not all weaknesses are equal. Focus on the one or two patterns that explain the most losses. A player who fixes their worst opening will improve faster than one who polishes their already-strong time management.

**Step 4 — Write like a coach, not a reporter.** Don't just say "your win rate with black is 42%." Say "You win 42% with black vs 68% with white — that 26-point gap tells me you're uncomfortable in reactive positions. When you don't have the first-mover advantage, you likely play too passively and let your opponent dictate the game."

**Step 5 — End with a prioritized action plan.** Give 2-3 specific things the player should do next, ordered by expected impact. Make them concrete: not "study openings" but "stop playing the Sicilian Dragon as black — you've lost 70% of those games. Switch to the Caro-Kann where your stats are 60% and the positions suit reactive play."

Always quote exact numbers. Be direct. Be honest about weaknesses. Great coaching requires truth.

If no data is found for a player, say their games may not be in the system yet."""


class ChessCoachAgent:
    def __init__(self):
        self.model = GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=SYSTEM_PROMPT,
            tools=[TOOLS],
        )
        self.chat = self.model.start_chat()

    def _dispatch(self, name: str, args: dict) -> str:
        fn = TOOL_FN_MAP.get(name)
        if fn is None:
            return json.dumps({"error": f"unknown tool: {name}"})
        try:
            return json.dumps(fn(**args), default=str)
        except Exception as e:
            logger.error(f"Tool {name} failed: {e}")
            return json.dumps({"error": str(e)})

    def ask(self, message: str) -> str:
        response = self.chat.send_message(message)
        while True:
            parts = response.candidates[0].content.parts
            fn_calls = [p for p in parts if getattr(p, "function_call", None) and p.function_call.name]
            if not fn_calls:
                return response.text
            responses = []
            for fc in fn_calls:
                args = dict(fc.function_call.args)
                logger.info(f"Tool: {fc.function_call.name}({args})")
                result = self._dispatch(fc.function_call.name, args)
                logger.info(f"Result: {result[:200]}")
                responses.append(
                    Part.from_function_response(name=fc.function_call.name, response={"result": result})
                )
            response = self.chat.send_message(responses)


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
        return ChatResponse(reply=_agent.ask(req.message))

    @app.get("/health")
    def health():
        return {"status": "ok"}

except ImportError:
    app = None


if __name__ == "__main__":
    print("Chess Coach — type 'quit' to exit\n")
    agent = ChessCoachAgent()
    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not user_input or user_input.lower() in {"quit", "exit"}:
            break
        print(f"\nCoach: {agent.ask(user_input)}\n")
