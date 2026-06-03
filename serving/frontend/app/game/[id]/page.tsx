"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Chess } from "chess.js";
import {
  ResponsiveContainer, LineChart, Line, ReferenceLine, XAxis, YAxis, Tooltip,
} from "recharts";

import Link from "next/link";

import Header from "@/components/Header";
import Board from "@/components/Board";
import EvalBar from "@/components/EvalBar";
import StatusPill from "@/components/StatusPill";
import { api, ApiError } from "@/lib/api";
import type { Game, EvalResult } from "@/lib/types";
import type { Key } from "chessground/types";

interface MoveEval {
  ply: number;
  played_move: string | null;
  best_move: string | null;
  eval_cp: number | null;
  mate: number | null;
  eval_swing_cp_from_prev: number | null;
  classification: "blunder" | "mistake" | "inaccuracy" | "good" | string;
}

const CLASS_COLOR: Record<string, string> = {
  blunder: "#f43f5e",
  mistake: "#f59e0b",
  inaccuracy: "#facc15",
  good: "#10b981",
};

const STARTING_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";

// The API returns `fen` as the *pre-move* position. To render the board after
// move N is played, take moves[N-1].fen and replay its UCI move with chess.js.
function fenAfter(moves: Game["moves"], ply: number): string {
  if (ply === 0 || !moves[ply - 1]) return STARTING_FEN;
  const m = moves[ply - 1];
  const g = new Chess(m.fen);
  try {
    g.move({
      from: m.san.slice(0, 2),
      to: m.san.slice(2, 4),
      promotion: m.san.length > 4 ? m.san[4] : undefined,
    });
  } catch {
    // Fall back to pre-move FEN if the UCI parse fails.
    return m.fen;
  }
  return g.fen();
}

type Verdict = {
  tone: "ok" | "warn" | "error";
  title: string;
  detail: string;
};

function classifySwing(playerSide: "white" | "black", actualCp: number | null, userCp: number | null): Verdict {
  if (actualCp === null || userCp === null) {
    return { tone: "warn", title: "Engine offline", detail: "Could not evaluate." };
  }
  // Normalize so positive = good for player.
  const sign = playerSide === "white" ? 1 : -1;
  const aBefore = sign * actualCp;
  const aUser   = sign * userCp;
  const delta   = aUser - aBefore;
  if (delta >= 30)   return { tone: "ok",    title: "Stronger than the game",    detail: `Your move improves the position by ${(delta / 100).toFixed(1)} pawns.` };
  if (delta >= -20)  return { tone: "ok",    title: "Equivalent",                 detail: `Within engine noise of the game move.` };
  if (delta >= -100) return { tone: "warn",  title: "Slightly worse",             detail: `Loses about ${Math.abs(delta / 100).toFixed(1)} pawns vs the game move.` };
  return { tone: "error", title: "Significantly worse", detail: `Loses about ${Math.abs(delta / 100).toFixed(1)} pawns. Engine prefers a different idea.` };
}

export default function GameExplorerPage({ params }: { params: { id: string } }) {
  const gameId = decodeURIComponent(params.id);

  const [game, setGame] = useState<Game | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [ply, setPly] = useState(0);                // 0 = starting position, n = after move n
  const [evalNow, setEvalNow] = useState<EvalResult | null>(null);
  const [evalLoading, setEvalLoading] = useState(false);
  const [playMode, setPlayMode] = useState(false);
  const [userTry, setUserTry] = useState<{ san: string; verdict: Verdict; userEval: EvalResult; bestMove: string | null } | null>(null);
  const evalCache = useRef<Map<number, EvalResult>>(new Map());
  const userEvalAbortRef = useRef<AbortController | null>(null);

  // Per-ply Stockfish evaluation timeline (from move_evaluations, populated by the daily DAG).
  const [evals, setEvals] = useState<MoveEval[]>([]);
  const [evalsAvailable, setEvalsAvailable] = useState<"unknown" | "yes" | "no">("unknown");
  // AI narrative analysis state.
  const [analysis, setAnalysis] = useState<string | null>(null);
  const [analyzing, setAnalyzing] = useState(false);
  const [analyzeError, setAnalyzeError] = useState<string | null>(null);

  // Load game on mount.
  useEffect(() => {
    const controller = new AbortController();
    let alive = true;
    setError(null);
    api<Game>(`/games/${encodeURIComponent(gameId)}`, { signal: controller.signal })
      .then((g) => { if (alive) setGame(g); })
      .catch((e) => { if (alive) setError(String(e.message ?? e)); });
    return () => {
      alive = false;
      controller.abort();
    };
  }, [gameId]);

  // Load per-ply evaluations (best-effort; 404 if DAG hasn't run for this date).
  useEffect(() => {
    const controller = new AbortController();
    let alive = true;
    api<{ evaluations: MoveEval[] }>(`/games/${encodeURIComponent(gameId)}/evaluations`, { signal: controller.signal })
      .then((r) => { if (alive) { setEvals(r.evaluations); setEvalsAvailable("yes"); } })
      .catch((e) => {
        if (!alive) return;
        if (e instanceof ApiError && e.status === 404) setEvalsAvailable("no");
        else setEvalsAvailable("no");
      });
    return () => {
      alive = false;
      controller.abort();
    };
  }, [gameId]);

  const evalsByPly = useMemo(() => {
    const m = new Map<number, MoveEval>();
    for (const e of evals) m.set(e.ply, e);
    return m;
  }, [evals]);

  const evalChart = useMemo(() => {
    return evals.map((e) => ({
      ply: e.ply,
      // Cap at +-10 so a single mate-in-1 doesn't swamp the rest of the chart.
      eval: e.mate !== null
        ? (e.mate > 0 ? 10 : -10)
        : Math.max(-10, Math.min(10, (e.eval_cp ?? 0) / 100)),
      classification: e.classification,
    }));
  }, [evals]);

  function runAnalyze() {
    setAnalyzing(true);
    setAnalyzeError(null);
    api<{ narrative: string }>(`/games/${encodeURIComponent(gameId)}/analyze`, { method: "POST", body: "{}" })
      .then((r) => setAnalysis(r.narrative))
      .catch((e) => setAnalyzeError(e instanceof Error ? e.message : String(e)))
      .finally(() => setAnalyzing(false));
  }

  const moves = game?.moves ?? [];
  const fen = useMemo(() => fenAfter(moves, ply), [moves, ply]);
  const lastMove = useMemo<[Key, Key] | undefined>(() => {
    if (ply === 0) return undefined;
    const uci = moves[ply - 1]?.san;
    if (!uci || uci.length < 4) return undefined;
    return [uci.slice(0, 2) as Key, uci.slice(2, 4) as Key];
  }, [moves, ply]);
  const sideToMove: "white" | "black" = fen.split(" ")[1] === "w" ? "white" : "black";

  // Evaluate current position (cached).
  useEffect(() => {
    if (!game) return;
    if (playMode) return; // freeze eval while user is trying their own line
    const cached = evalCache.current.get(ply);
    if (cached) { setEvalNow(cached); return; }
    const controller = new AbortController();
    let alive = true;
    setEvalLoading(true);
    api<EvalResult>("/eval", { method: "POST", signal: controller.signal, body: JSON.stringify({ fen }) })
      .then((r) => {
        if (!alive) return;
        evalCache.current.set(ply, r);
        setEvalNow(r);
      })
      .catch(() => {
        if (alive) setEvalNow(null);
      })
      .finally(() => {
        if (alive) setEvalLoading(false);
      });
    return () => {
      alive = false;
      controller.abort();
    };
  }, [game, ply, fen, playMode]);

  useEffect(() => {
    return () => userEvalAbortRef.current?.abort();
  }, []);

  function jumpTo(p: number) {
    setUserTry(null);
    setPly(Math.max(0, Math.min(moves.length, p)));
  }

  async function handleUserMove(uci: string, san: string, nextFen: string) {
    if (!game) return;
    userEvalAbortRef.current?.abort();
    const controller = new AbortController();
    userEvalAbortRef.current = controller;
    const baseEval = evalCache.current.get(ply) ?? evalNow;
    setEvalLoading(true);
    try {
      const userEval = await api<EvalResult>("/eval", {
        method: "POST",
        signal: controller.signal,
        body: JSON.stringify({ fen: nextFen }),
      });
      if (controller.signal.aborted) return;
      const verdict = classifySwing(
        sideToMove,
        baseEval?.cp ?? null,
        userEval.cp ?? null,
      );
      setUserTry({ san, verdict, userEval, bestMove: baseEval?.best_move ?? null });
    } catch {
      if (controller.signal.aborted) return;
      setUserTry({
        san,
        verdict: { tone: "warn", title: "Engine offline", detail: "Could not evaluate your move." },
        userEval: { cp: null, mate: null, best_move: null },
        bestMove: null,
      });
    } finally {
      if (userEvalAbortRef.current === controller) userEvalAbortRef.current = null;
      if (!controller.signal.aborted) setEvalLoading(false);
    }
  }

  const meta = game?.metadata;
  const subtitle = meta
    ? `${meta.white_id} (${meta.white_rating}) vs ${meta.black_id} (${meta.black_rating}) · ${meta.opening_name ?? "—"}`
    : "Loading…";

  return (
    <>
      <Header subtitle={game ? subtitle : `Game ${gameId}`} />

      <main className="max-w-6xl mx-auto px-6 py-6">
        <div className="flex items-center gap-3 mb-4 flex-wrap">
          {!game && !error && <StatusPill tone="loading">Loading game</StatusPill>}
          {error && <StatusPill tone="error">{error}</StatusPill>}
          {game && (
            <>
              <StatusPill tone="ok">Loaded · {moves.length} moves</StatusPill>
              {evalsAvailable === "yes" && (
                <StatusPill tone="ok">{evals.length} positions analyzed</StatusPill>
              )}
              {evalLoading && <StatusPill tone="loading">Evaluating</StatusPill>}
              {playMode && <StatusPill tone="warn">Play-from-here mode</StatusPill>}
              {evalsAvailable === "yes" && (
                <button
                  onClick={runAnalyze}
                  disabled={analyzing}
                  className="ml-auto bg-accent text-bg font-medium px-3 py-1.5 rounded-md text-sm hover:opacity-90 disabled:opacity-40"
                >
                  {analyzing ? "Analyzing…" : analysis ? "Re-analyze with AI" : "Analyze with AI"}
                </button>
              )}
            </>
          )}
        </div>

        {game && evalsAvailable === "yes" && evalChart.length > 0 && (
          <section className="bg-surface border border-border rounded-md p-4 mb-4">
            <h3 className="text-xs uppercase tracking-wider text-muted mb-2">Evaluation timeline</h3>
            <ResponsiveContainer width="100%" height={140}>
              <LineChart data={evalChart} margin={{ left: 0, right: 12, top: 8, bottom: 0 }}>
                <XAxis dataKey="ply" stroke="#666" fontSize={10} tickLine={false} axisLine={false} />
                <YAxis
                  domain={[-10, 10]}
                  stroke="#666"
                  fontSize={10}
                  width={24}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(v) => v === 0 ? "0" : v > 0 ? `+${v}` : `${v}`}
                />
                <ReferenceLine y={0} stroke="#444" />
                <Tooltip
                  contentStyle={{ background: "rgb(255 255 255)", border: "1px solid rgb(220 220 228)", borderRadius: 6, fontSize: 12, color: "rgb(18 18 22)" }}
                  formatter={(v: number, _n, p: any) => [`${v >= 0 ? "+" : ""}${Number(v).toFixed(2)}  (${p?.payload?.classification ?? "—"})`, "eval"]}
                  labelFormatter={(v) => `ply ${v}`}
                />
                <Line type="monotone" dataKey="eval" stroke="#f59e0b" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </section>
        )}

        <AnimatePresence>
          {(analysis || analyzeError) && (
            <motion.div
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              className="bg-surface border border-border rounded-md p-4 mb-4 prose prose-sm max-w-none prose-p:my-2 prose-ol:my-2 prose-ul:my-2 prose-li:my-0.5 prose-headings:mt-3 prose-headings:mb-1.5"
            >
              {analyzeError && <p className="text-rose-400 not-prose">{analyzeError}</p>}
              {analysis && <ReactMarkdown remarkPlugins={[remarkGfm]}>{analysis}</ReactMarkdown>}
            </motion.div>
          )}
        </AnimatePresence>

        {game && (
          <div className="grid grid-cols-1 md:grid-cols-[1fr_320px] gap-6">
            <div className="flex gap-3 items-start">
              <EvalBar cp={evalNow?.cp ?? null} mate={evalNow?.mate ?? null} />
              <div className="flex-1 space-y-3">
                <Board
                  fen={fen}
                  lastMove={lastMove}
                  bestMove={!playMode && ply > 0 ? evalNow?.best_move ?? undefined : undefined}
                  movable={playMode}
                  onUserMove={playMode ? handleUserMove : undefined}
                />
                <div className="flex items-center justify-between gap-2">
                  <div className="flex gap-2">
                    <button
                      onClick={() => jumpTo(ply - 1)}
                      disabled={ply === 0}
                      className="px-3 py-1.5 rounded-md border border-border text-sm disabled:opacity-40 hover:border-accent"
                    >
                      Prev
                    </button>
                    <button
                      onClick={() => jumpTo(ply + 1)}
                      disabled={ply >= moves.length}
                      className="px-3 py-1.5 rounded-md border border-border text-sm disabled:opacity-40 hover:border-accent"
                    >
                      Next
                    </button>
                  </div>
                  <span className="text-xs text-muted">Move {ply} / {moves.length}</span>
                  <div className="flex gap-2">
                    <button
                      onClick={() => { setPlayMode((v) => !v); setUserTry(null); }}
                      title={playMode ? "Stop trying alternative moves and resume game review" : "Try your own move from this position. The engine will tell you if it's better or worse than what was played."}
                      className={`px-3 py-1.5 rounded-md text-sm border ${
                        playMode ? "border-accent text-accent" : "border-border hover:border-accent"
                      }`}
                    >
                      {playMode ? "Exit play mode" : "Play from here"}
                    </button>
                    {ply > 0 && ply < moves.length && (
                      <Link
                        href={`/whatif/${encodeURIComponent(gameId)}/${ply}`}
                        className="px-3 py-1.5 rounded-md text-sm border border-border hover:border-accent"
                      >
                        What if?
                      </Link>
                    )}
                  </div>
                </div>

                <AnimatePresence>
                  {userTry && (
                    <motion.div
                      key={userTry.san}
                      initial={{ opacity: 0, y: 6 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0, y: -6 }}
                      transition={{ duration: 0.2 }}
                      className="bg-surface border border-border rounded-md p-4 space-y-1"
                    >
                      <div className="flex items-center gap-2">
                        <StatusPill tone={userTry.verdict.tone}>{userTry.verdict.title}</StatusPill>
                        <span className="text-xs text-muted font-mono">You played {userTry.san}</span>
                      </div>
                      <p className="text-sm text-muted">{userTry.verdict.detail}</p>
                      {userTry.bestMove && (
                        <p className="text-xs text-muted">
                          Engine top choice from this position: <span className="font-mono text-text">{userTry.bestMove}</span>
                        </p>
                      )}
                    </motion.div>
                  )}
                </AnimatePresence>
              </div>
            </div>

            <aside className="bg-surface border border-border rounded-md p-3 max-h-[520px] overflow-y-auto self-start">
              <h3 className="text-xs uppercase tracking-wider text-muted mb-2">Moves</h3>
              <div className="grid grid-cols-[auto_1fr_1fr] gap-x-2 text-sm font-mono">
                {Array.from({ length: Math.ceil(moves.length / 2) }).map((_, i) => {
                  const w = moves[i * 2];
                  const b = moves[i * 2 + 1];
                  const wEval = w ? evalsByPly.get(w.ply) : undefined;
                  const bEval = b ? evalsByPly.get(b.ply) : undefined;
                  const wColor = wEval && wEval.classification !== "good" ? CLASS_COLOR[wEval.classification] : undefined;
                  const bColor = bEval && bEval.classification !== "good" ? CLASS_COLOR[bEval.classification] : undefined;
                  return (
                    <div key={i} className="contents">
                      <span className="text-muted">{i + 1}.</span>
                      {w ? (
                        <button
                          onClick={() => jumpTo(w.ply)}
                          title={wEval ? `${wEval.classification}${wEval.eval_cp != null ? ` · eval ${(wEval.eval_cp/100).toFixed(2)}` : ""}` : undefined}
                          className={`text-left px-1.5 rounded ${ply === w.ply ? "bg-accent/15 text-accent" : "hover:bg-border/40"}`}
                          style={wColor && ply !== w.ply ? { color: wColor } : undefined}
                        >
                          {w.san}
                        </button>
                      ) : <span />}
                      {b ? (
                        <button
                          onClick={() => jumpTo(b.ply)}
                          className={`text-left px-1.5 rounded ${ply === b.ply ? "bg-accent/15 text-accent" : "hover:bg-border/40"}`}
                        >
                          {b.san}
                        </button>
                      ) : <span />}
                    </div>
                  );
                })}
              </div>
            </aside>
          </div>
        )}
      </main>
    </>
  );
}
