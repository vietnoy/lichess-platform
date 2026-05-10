"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

import Link from "next/link";

import Header from "@/components/Header";
import Board from "@/components/Board";
import EvalBar from "@/components/EvalBar";
import StatusPill from "@/components/StatusPill";
import { api } from "@/lib/api";
import type { Game, EvalResult } from "@/lib/types";
import type { Key } from "chessground/types";

const STARTING_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";

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

  // Load game on mount.
  useEffect(() => {
    let alive = true;
    setError(null);
    api<Game>(`/games/${encodeURIComponent(gameId)}`)
      .then((g) => { if (alive) setGame(g); })
      .catch((e) => { if (alive) setError(String(e.message ?? e)); });
    return () => { alive = false; };
  }, [gameId]);

  const moves = game?.moves ?? [];
  const fen = ply === 0 ? STARTING_FEN : moves[ply - 1]?.fen ?? STARTING_FEN;
  const lastMoveUci = useMemo(() => {
    if (ply === 0) return undefined;
    // chessground last-move arrow: derived from chess.js? Easier: skip exact lastMove and let chessground compute none.
    return undefined;
  }, [ply]);
  const sideToMove: "white" | "black" = fen.split(" ")[1] === "w" ? "white" : "black";

  // Evaluate current position (cached).
  useEffect(() => {
    if (!game) return;
    if (playMode) return; // freeze eval while user is trying their own line
    const cached = evalCache.current.get(ply);
    if (cached) { setEvalNow(cached); return; }
    setEvalLoading(true);
    api<EvalResult>("/eval", { method: "POST", body: JSON.stringify({ fen }) })
      .then((r) => { evalCache.current.set(ply, r); setEvalNow(r); })
      .catch(() => setEvalNow(null))
      .finally(() => setEvalLoading(false));
  }, [game, ply, fen, playMode]);

  function jumpTo(p: number) {
    setUserTry(null);
    setPly(Math.max(0, Math.min(moves.length, p)));
  }

  async function handleUserMove(uci: string, san: string, nextFen: string) {
    if (!game) return;
    const baseEval = evalCache.current.get(ply) ?? evalNow;
    setEvalLoading(true);
    try {
      const userEval = await api<EvalResult>("/eval", { method: "POST", body: JSON.stringify({ fen: nextFen }) });
      const verdict = classifySwing(
        sideToMove,
        baseEval?.cp ?? null,
        userEval.cp ?? null,
      );
      setUserTry({ san, verdict, userEval, bestMove: baseEval?.best_move ?? null });
    } catch {
      setUserTry({
        san,
        verdict: { tone: "warn", title: "Engine offline", detail: "Could not evaluate your move." },
        userEval: { cp: null, mate: null, best_move: null },
        bestMove: null,
      });
    } finally {
      setEvalLoading(false);
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
        <div className="flex items-center gap-3 mb-4">
          {!game && !error && <StatusPill tone="loading">Loading game</StatusPill>}
          {error && <StatusPill tone="error">{error}</StatusPill>}
          {game && (
            <>
              <StatusPill tone="ok">Loaded · {moves.length} moves</StatusPill>
              {evalLoading && <StatusPill tone="loading">Evaluating</StatusPill>}
              {playMode && <StatusPill tone="warn">Play-from-here mode</StatusPill>}
            </>
          )}
        </div>

        {game && (
          <div className="grid grid-cols-1 md:grid-cols-[1fr_320px] gap-6">
            <div className="flex gap-3 items-start">
              <EvalBar cp={evalNow?.cp ?? null} mate={evalNow?.mate ?? null} />
              <div className="flex-1 space-y-3">
                <Board
                  fen={fen}
                  bestMove={!playMode ? evalNow?.best_move ?? undefined : undefined}
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

            <aside className="bg-surface border border-border rounded-md p-3 max-h-[600px] overflow-y-auto">
              <h3 className="text-xs uppercase tracking-wider text-muted mb-2">Moves</h3>
              <div className="grid grid-cols-[auto_1fr_1fr] gap-x-2 text-sm font-mono">
                {Array.from({ length: Math.ceil(moves.length / 2) }).map((_, i) => {
                  const w = moves[i * 2];
                  const b = moves[i * 2 + 1];
                  return (
                    <div key={i} className="contents">
                      <span className="text-muted">{i + 1}.</span>
                      {w ? (
                        <button
                          onClick={() => jumpTo(w.ply)}
                          className={`text-left px-1.5 rounded ${ply === w.ply ? "bg-accent/15 text-accent" : "hover:bg-border/40"}`}
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
