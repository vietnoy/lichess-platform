import os
import requests
import chess
import chess.svg
import streamlit as st
import mysql.connector
from dotenv import load_dotenv
from agent import ChessCoachAgent

load_dotenv()

STOCKFISH_URL   = os.getenv("STOCKFISH_URL", "http://stockfish:8001/eval")
STOCKFISH_DEPTH = int(os.getenv("STOCKFISH_DEPTH", "8"))
SR_HOST         = os.getenv("STARROCKS_HOST", "localhost")
SR_PORT         = int(os.getenv("STARROCKS_PORT", "9030"))
SR_USER         = os.getenv("STARROCKS_USER", "root")
SR_PASS         = os.getenv("STARROCKS_PASSWORD", "")
TABLE           = "polaris_catalog.prod.chess_move_events"


def query_starrocks(sql):
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


def eval_fen(fen):
    try:
        r = requests.get(STOCKFISH_URL, params={"fen": fen, "depth": STOCKFISH_DEPTH}, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def load_game(game_id):
    return query_starrocks(f"""
        SELECT move_number, whose_moved, move, fen,
               ROUND(clock_remaining / 100.0, 1) AS clock_s,
               white_id, black_id, white_rating, black_rating,
               opening_eco, opening_name, speed, winner, end_status
        FROM {TABLE}
        WHERE game_id = '{game_id}'
        ORDER BY move_number
    """)


def render_board(fen, last_move_uci=None, suggested_move_uci=None):
    board = chess.Board(fen)
    arrows = []
    if last_move_uci:
        try:
            m = chess.Move.from_uci(last_move_uci)
            arrows.append(chess.svg.Arrow(m.from_square, m.to_square, color="#aaaaff"))
        except Exception:
            pass
    if suggested_move_uci:
        try:
            m = chess.Move.from_uci(suggested_move_uci)
            arrows.append(chess.svg.Arrow(m.from_square, m.to_square, color="#00cc44"))
        except Exception:
            pass
    return chess.svg.board(board, arrows=arrows, size=420)


def cp_to_label(cp):
    if cp is None:
        return "unknown"
    if cp > 300:
        return f"+{cp/100:.1f} — White is winning"
    if cp < -300:
        return f"{cp/100:.1f} — Black is winning"
    return f"{cp/100:+.1f} — Balanced"


st.set_page_config(page_title="Chess Coach", page_icon="♟", layout="wide")

st.markdown("""
    <h1 style='text-align:center; margin-bottom:0'>♟ Chess Coach</h1>
    <p style='text-align:center; color:gray; margin-top:4px'>Powered by real Lichess game data</p>
    <hr>
""", unsafe_allow_html=True)

if "agent" not in st.session_state:
    st.session_state.agent = ChessCoachAgent()
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "game_moves" not in st.session_state:
    st.session_state.game_moves = []
if "move_index" not in st.session_state:
    st.session_state.move_index = 0

tab_explorer, tab_coach = st.tabs(["♟ Game Explorer", "🤖 AI Coach"])


with tab_explorer:
    st.subheader("Explore a Game")

    col_input, col_btn = st.columns([4, 1])
    with col_input:
        game_id_input = st.text_input("Game ID", placeholder="e.g. RPJr6MMX", label_visibility="collapsed")
    with col_btn:
        load_clicked = st.button("Load Game", use_container_width=True)

    if load_clicked and game_id_input:
        with st.spinner("Loading game..."):
            moves = load_game(game_id_input.strip())
        if moves:
            st.session_state.game_moves = moves
            st.session_state.move_index = 0
        else:
            st.error("Game not found in the database.")

    if st.session_state.game_moves:
        moves = st.session_state.game_moves
        idx   = st.session_state.move_index
        cur   = moves[idx]
        meta  = moves[0]

        st.markdown(f"""
            <div style='background:#f0f2f6; border-radius:8px; padding:10px 16px; margin-bottom:12px'>
                ♙ <b>{meta['white_id']}</b> ({meta['white_rating']}) &nbsp;vs&nbsp;
                ♟ <b>{meta['black_id']}</b> ({meta['black_rating']}) &nbsp;|&nbsp;
                {meta['opening_name']} ({meta['opening_eco']}) &nbsp;|&nbsp;
                {meta['speed'].capitalize()} &nbsp;|&nbsp;
                Result: <b>{'Draw' if not meta['winner'] else meta['winner'].capitalize() + ' wins'}</b>
            </div>
        """, unsafe_allow_html=True)

        with st.spinner("Evaluating position..."):
            eval_result   = eval_fen(cur["fen"])
        suggested_move = eval_result.get("best_move") if eval_result else None
        last_move      = moves[idx - 1]["move"] if idx > 0 else None
        board_svg      = render_board(cur["fen"], last_move, suggested_move)

        col_board, col_info = st.columns([1, 1])

        with col_board:
            st.image(board_svg.encode(), use_container_width=False)

            c1, c2, c3 = st.columns([1, 2, 1])
            with c1:
                if st.button("⬅ Prev") and idx > 0:
                    st.session_state.move_index -= 1
                    st.rerun()
            with c2:
                st.markdown(
                    f"<p style='text-align:center; margin:6px 0'>Move <b>{idx + 1}</b> of {len(moves)}</p>",
                    unsafe_allow_html=True,
                )
            with c3:
                if st.button("Next ➡") and idx < len(moves) - 1:
                    st.session_state.move_index += 1
                    st.rerun()

        with col_info:
            st.markdown("#### Position Analysis")

            st.markdown(f"""
                <div style='background:#f9f9f9; border-left:4px solid #4a90e2;
                            padding:10px 14px; border-radius:4px; margin-bottom:12px'>
                    <b>{cur['whose_moved'].capitalize()}</b> played <code>{cur['move']}</code>
                    &nbsp;|&nbsp; Clock: <b>{cur['clock_s']}s</b>
                </div>
            """, unsafe_allow_html=True)

            if eval_result:
                cp   = eval_result.get("cp")
                mate = eval_result.get("mate")

                if mate is not None:
                    label = f"Mate in {abs(mate)}"
                    color = "#cc0000"
                else:
                    label = cp_to_label(cp)
                    color = "#2e7d32" if cp and cp > 0 else "#c62828" if cp and cp < 0 else "#555"

                st.markdown(f"""
                    <div style='background:#fff; border:1px solid #ddd; border-radius:6px;
                                padding:10px 14px; margin-bottom:10px'>
                        Eval: <span style='color:{color}; font-weight:bold'>{label}</span>
                    </div>
                """, unsafe_allow_html=True)

                if suggested_move:
                    st.markdown(f"""
                        <div style='background:#e8f5e9; border-left:4px solid #00cc44;
                                    padding:8px 14px; border-radius:4px; margin-bottom:12px'>
                            Best move: <code>{suggested_move}</code> (green arrow on board)
                        </div>
                    """, unsafe_allow_html=True)

            st.markdown("#### Move List")
            move_rows = []
            for i, m in enumerate(moves):
                active = "background:#e3f2fd; font-weight:bold;" if i == idx else ""
                move_rows.append(
                    f"<div style='padding:2px 6px; border-radius:3px; {active}'>"
                    f"{m['move_number']}. {m['move']} "
                    f"<span style='color:gray; font-size:0.85em'>({m['whose_moved'][0].upper()}) "
                    f"{m['clock_s']}s</span></div>"
                )
            st.markdown(
                f"<div style='height:300px; overflow-y:auto; border:1px solid #eee; "
                f"border-radius:6px; padding:6px'>{''.join(move_rows)}</div>",
                unsafe_allow_html=True,
            )


with tab_coach:
    st.subheader("Ask Your AI Chess Coach")

    username_input = st.text_input(
        "Lichess Username",
        placeholder="Enter your Lichess username",
        help="The coach will use your game history to answer questions."
    )

    if username_input:
        st.markdown(
            f"<div style='color:#1565c0; font-size:0.9em; margin-bottom:8px'>"
            f"Coaching session for: <b>{username_input}</b></div>",
            unsafe_allow_html=True,
        )

    for msg in st.session_state.chat_history:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    user_input = st.chat_input("Ask anything about your chess performance...")

    if user_input:
        prompt = user_input
        if username_input and username_input.lower() not in user_input.lower():
            prompt = f"[Player: {username_input}] {user_input}"

        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.write(user_input)

        with st.chat_message("assistant"):
            with st.spinner("Analyzing your games..."):
                reply = st.session_state.agent.ask(prompt)
            st.write(reply)

        st.session_state.chat_history.append({"role": "assistant", "content": reply})
