"use client";

import { useEffect, useMemo, useState } from "react";
import { motion } from "framer-motion";
import { Chess } from "chess.js";

import Header from "@/components/Header";
import Board from "@/components/Board";
import StatusPill from "@/components/StatusPill";
import { api } from "@/lib/api";
import type { Game, EvalResult } from "@/lib/types";

const FORWARD_PLIES = 6;
const STARTING_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";

interface LineStep {
  san: string;
  uci: string;
  fen: string;
  cp: number | null;
  mate: number | null;
}

interface Line {
  label: string;
  steps: LineStep[];
}

function winProbability(cp: number | null, mate: number | null, side: "white" | "black"): number {
  // Lichess-style mapping: cp → win% (always from the named side's perspective).
  if (mate !== null) return (mate > 0) === (side === "white") ? 0.99 : 0.01;
  if (cp === null) return 0.5;
  const sided = side === "white" ? cp : -cp;
  return 1 / (1 + Math.exp(-sided / 400));
}

export default function WhatIfPage({ params }: { params: { id: string; ply: string } }) {
  const gameId = decodeURIComponent(params.id);
  const targetPly = parseInt(params.ply, 10);

  const [game, setGame] = useState<Game | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [actualLine, setActualLine] = useState<Line | null>(null);
  const [altLine, setAltLine] = useState<Line | null>(null);
  const [phase, setPhase] = useState<"idle" | "computing" | "ready" | "running">("idle");
  const [activeStep, setActiveStep] = useState(0);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // Load game
  useEffect(() => {
    let alive = true;
    api<Game>(`/games/${encodeURIComponent(gameId)}`)
      .then((g) => { if (alive) setGame(g); })
      .catch((e) => { if (alive) setError(String(e.message ?? e)); });
    return () => { alive = false; };
  }, [gameId]);

  const baseFen = useMemo(() => {
    if (!game || targetPly === 0) return STARTING_FEN;
    return game.moves[targetPly - 1]?.fen ?? STARTING_FEN;
  }, [game, targetPly]);
  const sideToMove: "white" | "black" = baseFen.split(" ")[1] === "w" ? "white" : "black";

  async function evaluatePosition(fen: string): Promise<EvalResult> {
    return api<EvalResult>("/eval", { method: "POST", body: JSON.stringify({ fen, depth: 12 }) });
  }

  async function playForward(startFen: string, firstMoveUci?: string): Promise<LineStep[]> {
    const steps: LineStep[] = [];
    const board = new Chess(startFen);
    let nextFen = startFen;
    for (let i = 0; i < FORWARD_PLIES; i++) {
      let uci: string;
      if (i === 0 && firstMoveUci) {
        uci = firstMoveUci;
      } else {
        const ev = await evaluatePosition(nextFen);
        if (!ev.best_move) break;
        uci = ev.best_move;
      }
      const move = board.move({ from: uci.slice(0, 2), to: uci.slice(2, 4), promotion: uci.slice(4) || "q" });
      if (!move) break;
      nextFen = board.fen();
      const ev = await evaluatePosition(nextFen);
      steps.push({ san: move.san, uci, fen: nextFen, cp: ev.cp, mate: ev.mate });
    }
    return steps;
  }

  async function handleAltMove(uci: string, _san: string, _nextFen: string) {
    if (!game) return;
    setPhase("computing");
    setErrorMsg(null);
    setActiveStep(0);
    try {
      const actualMoveUci = computeActualMoveUci(baseFen, game.moves[targetPly]?.san ?? "");
      const [actual, alt] = await Promise.all([
        playForward(baseFen, actualMoveUci),
        playForward(baseFen, uci),
      ]);
      setActualLine({ label: "Game line", steps: actual });
      setAltLine({ label: "Your alternative", steps: alt });
      setPhase("ready");
    } catch (e) {
      setErrorMsg(e instanceof Error ? e.message : String(e));
      setPhase("idle");
    }
  }

  // Auto-step animation through both lines once ready.
  useEffect(() => {
    if (phase !== "ready" || !actualLine || !altLine) return;
    const max = Math.max(actualLine.steps.length, altLine.steps.length);
    setPhase("running");
    setActiveStep(0);
    let i = 0;
    const id = setInterval(() => {
      i += 1;
      setActiveStep(i);
      if (i >= max) clearInterval(id);
    }, 800);
    return () => clearInterval(id);
  }, [phase, actualLine, altLine]);

  function reset() {
    setActualLine(null);
    setAltLine(null);
    setPhase("idle");
    setErrorMsg(null);
    setActiveStep(0);
  }

  const status = useMemo(() => {
    if (error) return { tone: "error" as const, label: error };
    if (errorMsg) return { tone: "error" as const, label: errorMsg };
    if (!game) return { tone: "loading" as const, label: "Loading game" };
    if (phase === "computing") return { tone: "loading" as const, label: "Engine computing both lines" };
    if (phase === "ready" || phase === "running") return { tone: "ok" as const, label: "Lines ready" };
    return { tone: "idle" as const, label: `Pick an alternative move at ply ${targetPly}` };
  }, [error, errorMsg, game, phase, targetPly]);

  const actualStep = actualLine?.steps[Math.min(activeStep, actualLine.steps.length - 1)];
  const altStep = altLine?.steps[Math.min(activeStep, altLine.steps.length - 1)];
  const actualFen = phase === "idle" ? baseFen : (actualStep?.fen ?? baseFen);
  const altFen    = phase === "idle" ? baseFen : (altStep?.fen ?? baseFen);

  const actualWp = winProbability(actualStep?.cp ?? null, actualStep?.mate ?? null, sideToMove);
  const altWp    = winProbability(altStep?.cp ?? null,    altStep?.mate ?? null,    sideToMove);

  return (
    <>
      <Header subtitle={`What If · ${gameId} · move ${targetPly}`} />
      <main className="max-w-6xl mx-auto px-6 py-6 space-y-4">
        <div className="flex items-center gap-3">
          <StatusPill tone={status.tone}>{status.label}</StatusPill>
          {phase !== "idle" && (
            <button
              onClick={reset}
              className="text-xs px-3 py-1 rounded-md border border-border hover:border-accent text-muted hover:text-text"
            >
              Try a different alternative
            </button>
          )}
        </div>

        {game && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <BoardPanel
              title="Game line"
              fen={actualFen}
              wp={actualWp}
              step={actualStep}
              steps={actualLine?.steps ?? []}
              activeIdx={activeStep}
              tint="rose"
              movable={false}
            />
            <BoardPanel
              title="Your alternative"
              fen={altFen}
              wp={altWp}
              step={altStep}
              steps={altLine?.steps ?? []}
              activeIdx={activeStep}
              tint="emerald"
              movable={phase === "idle"}
              onUserMove={phase === "idle" ? handleAltMove : undefined}
            />
          </div>
        )}

        {phase === "idle" && (
          <p className="text-sm text-muted max-w-prose">
            Make a move on the right board to set the alternative line. Both lines play forward {FORWARD_PLIES} plies using the engine's best continuation.
          </p>
        )}
      </main>
    </>
  );
}

function BoardPanel({
  title, fen, wp, step, steps, activeIdx, tint, movable, onUserMove,
}: {
  title: string;
  fen: string;
  wp: number;
  step?: LineStep;
  steps: LineStep[];
  activeIdx: number;
  tint: "rose" | "emerald";
  movable: boolean;
  onUserMove?: (uci: string, san: string, nextFen: string) => void;
}) {
  const wpPct = Math.round(wp * 100);
  return (
    <section className="bg-surface border border-border rounded-md p-4 space-y-3">
      <header className="flex items-center justify-between">
        <h3 className={`text-xs uppercase tracking-wider ${tint === "rose" ? "text-rose-400" : "text-emerald-400"}`}>{title}</h3>
        <span className="text-xs text-muted font-mono tabular-nums">Win prob: {wpPct}%</span>
      </header>
      <Board fen={fen} movable={movable} onUserMove={onUserMove} />
      <motion.div
        className="h-1 rounded-full bg-border overflow-hidden"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
      >
        <motion.div
          className={tint === "rose" ? "h-full bg-rose-500" : "h-full bg-emerald-500"}
          animate={{ width: `${wpPct}%` }}
          transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
        />
      </motion.div>
      <ol className="text-xs font-mono space-y-1">
        {steps.length === 0 && <li className="text-muted">—</li>}
        {steps.map((s, i) => (
          <li
            key={i}
            className={`flex items-center justify-between px-1 py-0.5 rounded ${
              i === activeIdx ? "bg-accent/15 text-accent" : "text-muted"
            }`}
          >
            <span>{i + 1}. {s.san}</span>
            <span className="tabular-nums">{s.mate !== null ? `M${Math.abs(s.mate)}` : s.cp !== null ? `${(s.cp / 100).toFixed(1)}` : "—"}</span>
          </li>
        ))}
      </ol>
    </section>
  );
}

// Compute the UCI for the actual game move at this ply by replaying SAN against the base position.
function computeActualMoveUci(fen: string, san: string): string | undefined {
  if (!san) return undefined;
  try {
    const board = new Chess(fen);
    const m = board.move(san);
    if (!m) return undefined;
    return m.from + m.to + (m.promotion ?? "");
  } catch {
    return undefined;
  }
}
