"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

import Header from "@/components/Header";
import Board from "@/components/Board";
import StatusPill from "@/components/StatusPill";
import { api, ApiError } from "@/lib/api";
import type { EvalResult } from "@/lib/types";

interface Exercise {
  game_id: string;
  ply: number;
  fen_before: string;
  played_move: string;
  best_move: string | null;
  eval_cp: number | null;
  eval_swing_cp: number | null;
  classification: "blunder" | "mistake" | "inaccuracy" | "good" | null;
  clock_remaining_s: number;
  side_to_move: "white" | "black";
  move_number: number;
  opening_name: string | null;
  opening_eco: string | null;
  speed: string;
}

type Outcome =
  | { kind: "pending" }
  | { kind: "correct"; san: string; matchedBest: boolean }
  | { kind: "wrong"; san: string; userEval: EvalResult; engineDelta: number | null }
  | { kind: "timeout" };

const HINT_TIERS = [
  "Look at every undefended piece on the board, including yours.",
  "The strongest move involves the side to move's most active minor piece.",
  null, // tier 3 reveals via the green arrow on the board
] as const;

export default function DrillPage() {
  const [username, setUsername] = useState("");
  const [exercise, setExercise] = useState<Exercise | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [secondsLeft, setSecondsLeft] = useState(0);
  const [outcome, setOutcome] = useState<Outcome>({ kind: "pending" });
  const [hintsShown, setHintsShown] = useState(0);
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => () => { if (tickRef.current) clearInterval(tickRef.current); }, []);

  async function loadNext(name: string) {
    if (!name) return;
    setError(null);
    setLoading(true);
    setOutcome({ kind: "pending" });
    setHintsShown(0);
    if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null; }
    try {
      const ex = await api<Exercise>(`/exercise/${encodeURIComponent(name)}`);
      setExercise(ex);
      const initial = Math.max(3, Math.round(ex.clock_remaining_s));
      setSecondsLeft(initial);
      tickRef.current = setInterval(() => {
        setSecondsLeft((s) => {
          if (s <= 1) {
            if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null; }
            setOutcome((cur) => cur.kind === "pending" ? { kind: "timeout" } : cur);
            return 0;
          }
          return s - 1;
        });
      }, 1000);
    } catch (e) {
      const msg = e instanceof ApiError && e.status === 503
        ? "The blunder analyzer has not produced any drills yet for this player. Run the analyzer DAG to populate exercises."
        : e instanceof ApiError && e.status === 404
        ? `No exercises available for '${name}'.`
        : e instanceof Error ? e.message : String(e);
      setExercise(null);
      setError(msg);
    } finally {
      setLoading(false);
    }
  }

  async function handleMove(uci: string, san: string, nextFen: string) {
    if (!exercise || outcome.kind !== "pending") return;
    if (tickRef.current) { clearInterval(tickRef.current); tickRef.current = null; }

    // Best move from analyzer is in UCI form; if it matches → correct (and stronger than what they actually played).
    if (exercise.best_move && uci === exercise.best_move) {
      setOutcome({ kind: "correct", san, matchedBest: true });
      return;
    }

    // Otherwise evaluate the user's resulting position vs the original blunder line.
    try {
      const userEval = await api<EvalResult>("/eval", { method: "POST", body: JSON.stringify({ fen: nextFen }) });
      const sign = exercise.side_to_move === "white" ? 1 : -1;
      const userScore = sign * (userEval.cp ?? 0);
      const blunderScore = sign * (exercise.eval_cp ?? 0);
      const delta = userScore - blunderScore;
      // Better than what was played in the real game by ≥ 50cp counts as a "save".
      if (delta >= 50) setOutcome({ kind: "correct", san, matchedBest: false });
      else setOutcome({ kind: "wrong", san, userEval, engineDelta: delta });
    } catch {
      setOutcome({ kind: "wrong", san, userEval: { cp: null, mate: null, best_move: null }, engineDelta: null });
    }
  }

  function nextHint() { setHintsShown((n) => Math.min(3, n + 1)); }

  const status = useMemo(() => {
    if (loading) return { tone: "loading" as const, label: "Loading exercise" };
    if (error)   return { tone: "error"   as const, label: error };
    if (!exercise) return { tone: "idle" as const, label: "Enter a username and start" };
    if (outcome.kind === "timeout") return { tone: "warn" as const, label: "Time's up" };
    if (outcome.kind === "correct") return { tone: "ok" as const, label: outcome.matchedBest ? "Best move" : "Strong save" };
    if (outcome.kind === "wrong") return { tone: "error" as const, label: "Worse than the engine line" };
    return { tone: "loading" as const, label: `${secondsLeft}s remaining` };
  }, [loading, error, exercise, outcome, secondsLeft]);

  const showSolutionArrow = outcome.kind !== "pending" && exercise?.best_move;

  return (
    <>
      <Header subtitle={username ? `Pressure drill · ${username}` : "Pressure drill"} />
      <main className="max-w-5xl mx-auto px-6 py-6 space-y-4">
        <div className="flex items-center gap-3">
          <StatusPill tone={status.tone}>{status.label}</StatusPill>
          <input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") loadNext(username.trim()); }}
            placeholder="Lichess username"
            className="flex-1 max-w-xs bg-surface border border-border rounded-md px-3 py-1.5 text-sm outline-none focus:border-accent"
          />
          <button
            onClick={() => loadNext(username.trim())}
            disabled={!username.trim() || loading}
            className="bg-accent text-bg font-medium px-4 py-1.5 rounded-md hover:opacity-90 disabled:opacity-40 text-sm"
          >
            {exercise ? "Next drill" : "Start"}
          </button>
        </div>

        {!exercise && !error && !loading && (
          <p className="text-muted text-sm max-w-prose">
            Pulls one of your real blunders or mistakes, recreates the exact clock pressure of the moment, and asks you to find the move you missed. Your timer matches the seconds you had on the real clock.
          </p>
        )}

        {exercise && (
          <div className="grid grid-cols-1 md:grid-cols-[auto_1fr] gap-6">
            <div className="space-y-3">
              <Board
                fen={exercise.fen_before}
                orientation={exercise.side_to_move}
                bestMove={showSolutionArrow ? exercise.best_move ?? undefined : undefined}
                movable={outcome.kind === "pending"}
                onUserMove={handleMove}
              />
              <div className="text-xs text-muted text-center font-mono tabular-nums">
                {exercise.opening_eco ? `${exercise.opening_eco} · ` : ""}
                {exercise.opening_name ?? "—"} · {exercise.speed} · move {exercise.move_number}
              </div>
            </div>

            <aside className="space-y-3">
              <Card title="Position">
                <p className="text-sm">
                  {exercise.side_to_move === "white" ? "White" : "Black"} to move.
                  {exercise.classification && (
                    <> In your real game, you played a <span className="text-rose-400 font-medium">{exercise.classification}</span>.</>
                  )}
                </p>
                {exercise.eval_swing_cp !== null && (
                  <p className="text-xs text-muted mt-1">
                    Eval swing was about {(Math.abs(exercise.eval_swing_cp) / 100).toFixed(1)} pawns against you.
                  </p>
                )}
              </Card>

              {outcome.kind === "pending" && (
                <Card title="Hints">
                  <div className="space-y-2 text-sm">
                    {HINT_TIERS.slice(0, hintsShown).map((h, i) =>
                      h ? <p key={i} className="text-muted italic">{h}</p>
                        : <p key={i} className="text-muted italic">Look at the green arrow on the board.</p>
                    )}
                    {hintsShown < 3 && (
                      <button
                        onClick={nextHint}
                        className="text-xs text-accent border border-accent/40 px-2 py-1 rounded-md hover:bg-accent/10"
                      >
                        Show hint {hintsShown + 1} of 3
                      </button>
                    )}
                  </div>
                </Card>
              )}

              <AnimatePresence>
                {outcome.kind !== "pending" && (
                  <motion.div
                    key={outcome.kind}
                    initial={{ opacity: 0, y: 6 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.2 }}
                  >
                    <Card title="Result">
                      {outcome.kind === "timeout" && (
                        <p className="text-sm">
                          You ran out of time — same as you did in the real game.
                          {exercise.best_move && <> The strongest move was <code className="text-accent">{exercise.best_move}</code>.</>}
                        </p>
                      )}
                      {outcome.kind === "correct" && (
                        <p className="text-sm">
                          You played <code>{outcome.san}</code> — {outcome.matchedBest ? "exactly the engine's top choice." : "a clear improvement over the real game."}
                        </p>
                      )}
                      {outcome.kind === "wrong" && (
                        <>
                          <p className="text-sm">
                            You played <code>{outcome.san}</code>.
                            {outcome.engineDelta !== null
                              ? <> That's about {Math.abs(outcome.engineDelta / 100).toFixed(1)} pawns worse than the real game move.</>
                              : <> Engine couldn't evaluate the resulting position.</>}
                          </p>
                          {exercise.best_move && (
                            <p className="text-xs text-muted mt-2">
                              Engine's pick: <code className="text-accent">{exercise.best_move}</code> (green arrow).
                            </p>
                          )}
                        </>
                      )}
                    </Card>
                  </motion.div>
                )}
              </AnimatePresence>

              {outcome.kind !== "pending" && (
                <button
                  onClick={() => loadNext(username.trim())}
                  disabled={loading}
                  className="w-full bg-surface border border-border hover:border-accent rounded-md py-2 text-sm"
                >
                  Next drill
                </button>
              )}
            </aside>
          </div>
        )}
      </main>
    </>
  );
}

function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="bg-surface border border-border rounded-md p-4">
      <h3 className="text-xs uppercase tracking-wider text-muted mb-2">{title}</h3>
      {children}
    </section>
  );
}
