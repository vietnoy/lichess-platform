import os
import requests
import chess
import chess.svg
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from agent import ChessCoachAgent, analyze_game as agent_analyze_game
import vertexai
from vertexai.generative_models import GenerativeModel

load_dotenv()

STOCKFISH_URL   = os.getenv("STOCKFISH_URL", "http://stockfish:8001/eval")
STOCKFISH_DEPTH = int(os.getenv("STOCKFISH_DEPTH", "8"))
SR_HOST         = os.getenv("STARROCKS_HOST", "localhost")
SR_PORT         = int(os.getenv("STARROCKS_PORT", "9030"))
SR_USER         = os.getenv("STARROCKS_USER", "root")
SR_PASS         = os.getenv("STARROCKS_PASSWORD", "")
TABLE           = "polaris_catalog.prod.chess_move_events"
GCP_PROJECT     = os.getenv("GCP_PROJECT", "")
GCP_LOCATION    = os.getenv("GCP_LOCATION", "us-central1")


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


def get_ai_game_analysis(game_id: str, moves: list, evals: dict) -> str:
    lines = [f"Game ID: {game_id}"]
    meta = moves[0]
    lines.append(f"Players: {meta['white_id']} (White, {meta['white_rating']}) vs {meta['black_id']} (Black, {meta['black_rating']})")
    lines.append(f"Opening: {meta['opening_name']} ({meta['opening_eco']}), Speed: {meta['speed']}")
    winner = meta['winner']
    lines.append(f"Result: {'Draw' if not winner else winner.capitalize() + ' wins'}")
    lines.append("")
    lines.append("Move-by-move evaluations (centipawns, positive = White advantage):")

    for i, m in enumerate(moves):
        ev = evals.get(i)
        if ev:
            cp   = ev.get("cp")
            mate = ev.get("mate")
            eval_str = f"Mate in {abs(mate)}" if mate is not None else (f"{cp/100:+.2f}" if cp is not None else "?")
        else:
            eval_str = "?"
        lines.append(f"  Move {m['move_number']} ({m['whose_moved'][0].upper()}) {m['move']}: eval={eval_str}")

    prompt = "\n".join(lines) + """

Based on the above evaluation data, identify the critical turning points of this game:
- Which moves were blunders or serious mistakes (large eval swings)?
- Where did one player gain a decisive advantage?
- What was the key moment that decided the game?
- Any notable tactical or strategic themes?

Be specific about move numbers and the magnitude of eval changes. Keep the analysis concise but insightful."""

    vertexai.init(project=GCP_PROJECT, location=GCP_LOCATION)
    model = GenerativeModel("gemini-2.5-flash")
    response = model.generate_content(prompt)
    return response.text


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Chess Coach", page_icon="♟", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
[data-testid="collapsedControl"] { display: none; }
section[data-testid="stSidebar"] { display: none; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
    <h1 style='text-align:center; margin-bottom:0'>♟ Chess Coach</h1>
    <p style='text-align:center; color:gray; margin-top:4px'>Powered by real Lichess game data</p>
    <hr>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
for key, default in [
    ("agent", None),
    ("chat_history", []),
    ("game_moves", []),
    ("move_index", 0),
    ("game_id_loaded", ""),
    ("game_evals", {}),
    ("game_ai_analysis", None),
    ("dashboard_username", ""),
    ("dashboard_data", None),
    ("sql_result", None),
    ("sql_last_query", ""),
    ("coach_username", ""),
]:
    if key not in st.session_state:
        st.session_state[key] = default

if st.session_state.agent is None:
    st.session_state.agent = ChessCoachAgent()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_explorer, tab_dashboard, tab_coach, tab_sql = st.tabs([
    "♟ Game Explorer", "📊 Player Dashboard", "🤖 AI Coach", "🗄️ SQL Explorer"
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — GAME EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
with tab_explorer:
    st.subheader("Explore a Game")

    col_input, col_btn = st.columns([4, 1])
    with col_input:
        game_id_input = st.text_input("Game ID", placeholder="e.g. RPJr6MMX", label_visibility="collapsed", key="game_id_input")
    with col_btn:
        load_clicked = st.button("Load Game", use_container_width=True)

    if load_clicked and game_id_input:
        gid = game_id_input.strip()
        if gid != st.session_state.game_id_loaded:
            try:
                with st.spinner("Loading game..."):
                    moves = load_game(gid)
                if moves:
                    st.session_state.game_moves    = moves
                    st.session_state.move_index    = 0
                    st.session_state.game_id_loaded = gid
                    st.session_state.game_evals    = {}
                    st.session_state.game_ai_analysis = None
                else:
                    st.session_state.game_moves = []
                    st.error("Game not found in the database.")
            except Exception as e:
                st.session_state.game_moves = []
                st.error(f"Error loading game: {e}")

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

        # Eval current position (cached per move index)
        if idx not in st.session_state.game_evals:
            try:
                with st.spinner("Evaluating position..."):
                    st.session_state.game_evals[idx] = eval_fen(cur["fen"])
            except Exception:
                st.session_state.game_evals[idx] = None

        eval_result   = st.session_state.game_evals[idx]
        suggested_move = eval_result.get("best_move") if eval_result else None
        last_move      = moves[idx - 1]["move"] if idx > 0 else None
        board_svg      = render_board(cur["fen"], last_move, suggested_move)

        col_board, col_info = st.columns([1, 1])

        with col_board:
            st.markdown(board_svg, unsafe_allow_html=True)

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

        # ── AI Game Analysis ──────────────────────────────────────────────────
        st.divider()
        st.markdown("#### AI Critical Point Analysis")

        analyze_btn = st.button("🔍 Analyze Full Game with AI", key="analyze_game_btn")
        if analyze_btn:
            gid = st.session_state.game_id_loaded
            with st.spinner("Evaluating all moves and analyzing with AI..."):
                for i, m in enumerate(moves):
                    if i not in st.session_state.game_evals:
                        st.session_state.game_evals[i] = eval_fen(m["fen"])
                try:
                    analysis = get_ai_game_analysis(gid, moves, st.session_state.game_evals)
                    st.session_state.game_ai_analysis = analysis
                except Exception as e:
                    st.session_state.game_ai_analysis = f"Error: {e}"

        # Show eval chart if we have enough evals
        cached_evals = st.session_state.game_evals
        if len(cached_evals) > 1:
            eval_points = []
            for i, m in enumerate(moves):
                ev = cached_evals.get(i)
                if ev:
                    cp_val = ev.get("cp")
                    mate   = ev.get("mate")
                    if mate is not None:
                        cp_val = 3000 * (1 if mate > 0 else -1)
                    eval_points.append({"move": m["move_number"], "eval": cp_val / 100.0 if cp_val is not None else 0})

            if eval_points:
                df_eval = pd.DataFrame(eval_points)
                prev_vals = [0] + df_eval["eval"].tolist()[:-1]
                swings = [abs(cur - prev) for cur, prev in zip(df_eval["eval"].tolist(), prev_vals)]
                colors = ["#c62828" if s > 2.0 else "#2e7d32" if s > 0.5 else "#1565c0" for s in swings]

                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_eval["move"], y=df_eval["eval"],
                    mode="lines+markers",
                    line=dict(color="#555", width=1.5),
                    marker=dict(color=colors, size=6),
                    name="Eval",
                ))
                fig.add_hline(y=0, line_dash="dash", line_color="#aaa")
                fig.update_layout(
                    title="Evaluation Chart (red = big swing, green = small swing)",
                    xaxis_title="Move", yaxis_title="Eval (pawns)",
                    height=280, margin=dict(t=40, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

        if st.session_state.game_ai_analysis:
            st.markdown(f"""
                <div style='background:#f8f9fa; border-left:4px solid #4a90e2;
                            padding:14px 18px; border-radius:6px; white-space:pre-wrap'>
                    {st.session_state.game_ai_analysis}
                </div>
            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — PLAYER DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab_dashboard:
    st.subheader("Player Performance Dashboard")

    col_u, col_b = st.columns([4, 1])
    with col_u:
        dash_username = st.text_input(
            "Username", placeholder="Enter Lichess username",
            label_visibility="collapsed", key="dash_username_input"
        )
    with col_b:
        dash_go = st.button("Analyze", use_container_width=True, key="dash_go")

    if dash_go and dash_username:
        pid = dash_username.strip()
        st.session_state.dashboard_username = pid
        try:
            with st.spinner(f"Loading stats for {pid}..."):
                overview_rows = query_starrocks(f"""
                    SELECT speed,
                        COUNT(DISTINCT game_id) AS total_games,
                        SUM(CASE WHEN winner = CASE WHEN white_id='{pid}' THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS wins,
                        SUM(CASE WHEN winner IS NOT NULL AND winner != CASE WHEN white_id='{pid}' THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) AS losses,
                        SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS draws,
                        ROUND(AVG(CASE WHEN white_id='{pid}' THEN white_rating ELSE black_rating END), 0) AS avg_rating
                    FROM {TABLE}
                    WHERE (white_id='{pid}' OR black_id='{pid}') AND move_number=1
                    GROUP BY speed ORDER BY total_games DESC
                """)
                if not overview_rows:
                    st.session_state.dashboard_data = None
                    st.error(f"No data found for player '{pid}'.")
                else:
                    color_rows = query_starrocks(f"""
                        SELECT CASE WHEN white_id='{pid}' THEN 'White' ELSE 'Black' END AS color,
                            COUNT(DISTINCT game_id) AS games,
                            ROUND(SUM(CASE WHEN winner = CASE WHEN white_id='{pid}' THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_pct
                        FROM {TABLE}
                        WHERE (white_id='{pid}' OR black_id='{pid}') AND move_number=1
                        GROUP BY color
                    """)
                    opening_rows = query_starrocks(f"""
                        SELECT opening_eco, opening_name,
                            COUNT(DISTINCT game_id) AS games,
                            ROUND(SUM(CASE WHEN winner = CASE WHEN white_id='{pid}' THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_pct
                        FROM {TABLE}
                        WHERE (white_id='{pid}' OR black_id='{pid}') AND move_number=1 AND opening_eco IS NOT NULL
                        GROUP BY opening_eco, opening_name HAVING games >= 3
                        ORDER BY games DESC LIMIT 10
                    """)
                    clock_rows = query_starrocks(f"""
                        SELECT CASE WHEN move_number<=10 THEN 'Opening' WHEN move_number<=30 THEN 'Middlegame' ELSE 'Endgame' END AS phase,
                            ROUND(AVG(clock_remaining)/100.0, 1) AS avg_clock_s
                        FROM {TABLE}
                        WHERE (white_id='{pid}' OR black_id='{pid}') AND clock_remaining IS NOT NULL
                        GROUP BY phase ORDER BY phase
                    """)
                    vs_rating_rows = query_starrocks(f"""
                        SELECT CASE
                            WHEN (CASE WHEN white_id='{pid}' THEN black_rating ELSE white_rating END) < (CASE WHEN white_id='{pid}' THEN white_rating ELSE black_rating END) - 100 THEN 'Lower rated'
                            WHEN (CASE WHEN white_id='{pid}' THEN black_rating ELSE white_rating END) > (CASE WHEN white_id='{pid}' THEN white_rating ELSE black_rating END) + 100 THEN 'Higher rated'
                            ELSE 'Equal rated' END AS opponent,
                            COUNT(DISTINCT game_id) AS games,
                            ROUND(SUM(CASE WHEN winner = CASE WHEN white_id='{pid}' THEN 'white' ELSE 'black' END THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT game_id), 1) AS win_pct
                        FROM {TABLE}
                        WHERE (white_id='{pid}' OR black_id='{pid}') AND move_number=1
                        GROUP BY opponent
                    """)
                    recent_rows = query_starrocks(f"""
                        SELECT game_id,
                            CASE WHEN white_id='{pid}' THEN black_id ELSE white_id END AS opponent,
                            CASE WHEN white_id='{pid}' THEN white_rating ELSE black_rating END AS my_rating,
                            CASE WHEN white_id='{pid}' THEN black_rating ELSE white_rating END AS opp_rating,
                            opening_eco, opening_name, speed,
                            CASE WHEN winner IS NULL THEN 'Draw'
                                 WHEN winner = CASE WHEN white_id='{pid}' THEN 'white' ELSE 'black' END THEN 'Win'
                                 ELSE 'Loss' END AS result,
                            date
                        FROM {TABLE}
                        WHERE (white_id='{pid}' OR black_id='{pid}') AND move_number=1
                        ORDER BY date DESC LIMIT 15
                    """)
                    st.session_state.dashboard_data = {
                        "pid": pid,
                        "overview": overview_rows,
                        "color": color_rows,
                        "opening": opening_rows,
                        "clock": clock_rows,
                        "vs_rating": vs_rating_rows,
                        "recent": recent_rows,
                    }
        except Exception as e:
            st.session_state.dashboard_data = None
            st.error(f"Error loading player data: {e}")

    data = st.session_state.dashboard_data
    if data:
        pid           = data["pid"]
        overview_rows = data["overview"]
        color_rows    = data["color"]
        opening_rows  = data["opening"]
        clock_rows    = data["clock"]
        vs_rating_rows= data["vs_rating"]
        recent_rows   = data["recent"]

        total_games = sum(r["total_games"] for r in overview_rows)
        total_wins  = sum(r["wins"]        for r in overview_rows)
        total_losses= sum(r["losses"]      for r in overview_rows)
        total_draws = sum(r["draws"]       for r in overview_rows)
        avg_rating  = round(sum(r["avg_rating"] * r["total_games"] for r in overview_rows) / total_games)
        win_pct     = round(total_wins * 100 / total_games, 1) if total_games else 0

        st.markdown(f"### {pid} — Overview")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Games", f"{total_games:,}")
        m2.metric("Win Rate",    f"{win_pct}%")
        m3.metric("Wins",        f"{total_wins:,}")
        m4.metric("Losses",      f"{total_losses:,}")
        m5.metric("Avg Rating",  f"{avg_rating}")

        st.divider()

        row1_l, row1_r = st.columns(2)

        with row1_l:
            st.markdown("##### Result Distribution")
            fig = go.Figure(go.Pie(
                labels=["Wins", "Losses", "Draws"],
                values=[total_wins, total_losses, total_draws],
                hole=0.55,
                marker_colors=["#2e7d32", "#c62828", "#9e9e9e"],
            ))
            fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=280)
            st.plotly_chart(fig, use_container_width=True)

        with row1_r:
            st.markdown("##### Win Rate by Color")
            if color_rows:
                df_c = pd.DataFrame(color_rows)
                fig = px.bar(df_c, x="color", y="win_pct", text="win_pct",
                             color="color",
                             color_discrete_map={"White": "#bdbdbd", "Black": "#424242"},
                             labels={"win_pct": "Win %", "color": ""})
                fig.update_traces(texttemplate="%{text}%", textposition="outside")
                fig.update_layout(margin=dict(t=10, b=10), height=280, showlegend=False, yaxis_range=[0, 100])
                st.plotly_chart(fig, use_container_width=True)

        row2_l, row2_r = st.columns(2)

        with row2_l:
            st.markdown("##### Top Openings (Win Rate)")
            if opening_rows:
                df_o = pd.DataFrame(opening_rows)
                df_o["label"] = df_o["opening_eco"] + " — " + df_o["opening_name"].str[:25]
                fig = px.bar(df_o, x="win_pct", y="label", orientation="h",
                             text="win_pct", color="win_pct",
                             color_continuous_scale=["#c62828", "#ffd54f", "#2e7d32"],
                             range_color=[30, 70],
                             labels={"win_pct": "Win %", "label": ""})
                fig.update_traces(texttemplate="%{text}%", textposition="outside")
                fig.update_layout(margin=dict(t=10, b=10), height=340, coloraxis_showscale=False, yaxis={"autorange": "reversed"})
                st.plotly_chart(fig, use_container_width=True)

        with row2_r:
            st.markdown("##### Avg Clock Remaining by Phase")
            if clock_rows:
                df_cl = pd.DataFrame(clock_rows)
                fig = px.bar(df_cl, x="phase", y="avg_clock_s", text="avg_clock_s",
                             color="phase",
                             color_discrete_map={"Opening": "#1565c0", "Middlegame": "#f57c00", "Endgame": "#6a1b9a"},
                             labels={"avg_clock_s": "Avg seconds left", "phase": ""})
                fig.update_traces(texttemplate="%{text}s", textposition="outside")
                fig.update_layout(margin=dict(t=10, b=10), height=340, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("##### Performance vs Opponent Strength")
        if vs_rating_rows:
            df_vr = pd.DataFrame(vs_rating_rows)
            fig = px.bar(df_vr, x="opponent", y="win_pct", text="win_pct",
                         color="win_pct",
                         color_continuous_scale=["#c62828", "#ffd54f", "#2e7d32"],
                         range_color=[30, 70],
                         labels={"win_pct": "Win %", "opponent": ""})
            fig.update_traces(texttemplate="%{text}%", textposition="outside")
            fig.update_layout(margin=dict(t=10, b=10), height=260, coloraxis_showscale=False, yaxis_range=[0, 100])
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("##### Recent Games")
        if recent_rows:
            df_r = pd.DataFrame(recent_rows)
            def color_result(val):
                if val == "Win":  return "background-color:#e8f5e9; color:#2e7d32; font-weight:bold"
                if val == "Loss": return "background-color:#ffebee; color:#c62828; font-weight:bold"
                return "background-color:#f5f5f5; color:#555"
            st.dataframe(
                df_r.style.map(color_result, subset=["result"]),
                use_container_width=True, hide_index=True
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — AI COACH
# ═══════════════════════════════════════════════════════════════════════════════
with tab_coach:
    st.subheader("🤖 AI Chess Coach")
    st.caption("Ask anything about your game — openings, tactics, time management, improvement areas.")

    col_name, col_clear = st.columns([4, 1])
    with col_name:
        coach_username = st.text_input(
            "Analyzing player:", placeholder="Enter Lichess username",
            label_visibility="collapsed", key="coach_username_input",
            value=st.session_state.coach_username,
        )
        if coach_username:
            st.session_state.coach_username = coach_username.strip()
    with col_clear:
        if st.button("🗑 Clear", use_container_width=True, key="clear_coach"):
            st.session_state.chat_history = []
            st.session_state.agent = ChessCoachAgent()
            st.rerun()

    chat_container = st.container(height=500)
    with chat_container:
        for msg in st.session_state.chat_history:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])

    user_input = st.chat_input("Ask your coach...", key="coach_input")

    if user_input:
        uname  = st.session_state.coach_username
        prompt = f"[Player: {uname}] {user_input}" if uname and uname.lower() not in user_input.lower() else user_input

        st.session_state.chat_history.append({"role": "user", "content": user_input})
        with chat_container:
            with st.chat_message("user"):
                st.write(user_input)
            with st.chat_message("assistant"):
                with st.spinner("Analyzing..."):
                    reply = st.session_state.agent.ask(prompt)
                st.write(reply)
        st.session_state.chat_history.append({"role": "assistant", "content": reply})
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SQL EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
with tab_sql:
    st.subheader("SQL Explorer")
    st.caption(f"Query against `{TABLE}`")

    default_sql = f"""SELECT date, COUNT(DISTINCT game_id) AS games, COUNT(*) AS moves
FROM {TABLE}
GROUP BY date
ORDER BY date DESC"""

    sql_input = st.text_area("SQL Query", value=default_sql, height=160, key="sql_input")

    col_run, col_hint = st.columns([1, 5])
    with col_run:
        run_sql = st.button("▶ Run", use_container_width=True)
    with col_hint:
        st.caption("⚠️ SELECT only. Results capped at 500 rows.")

    if run_sql and sql_input.strip():
        upper = sql_input.strip().upper()
        if not upper.startswith("SELECT") and not upper.startswith("WITH"):
            st.error("Only SELECT queries are allowed.")
        else:
            safe_sql = sql_input.strip()
            if "LIMIT" not in upper:
                safe_sql += " LIMIT 500"
            try:
                with st.spinner("Running query..."):
                    rows = query_starrocks(safe_sql)
                if rows:
                    st.session_state.sql_result     = rows
                    st.session_state.sql_last_query = sql_input.strip()
                else:
                    st.session_state.sql_result = []
                    st.info("Query returned no rows.")
            except Exception as e:
                st.session_state.sql_result = None
                st.error(f"Query error: {e}")

    if st.session_state.sql_result:
        df = pd.DataFrame(st.session_state.sql_result)
        st.success(f"{len(df):,} rows returned")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("⬇ Download CSV", df.to_csv(index=False).encode(), "result.csv", "text/csv")
