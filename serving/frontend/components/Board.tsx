"use client";

import { useEffect, useRef } from "react";
import { Chessground } from "chessground";
import type { Api } from "chessground/api";
import type { Config } from "chessground/config";
import type { Key } from "chessground/types";
import { Chess, SQUARES } from "chess.js";

// chessground v9 doesn't export its CSS via its package.json; we vendor the files.
import "@/styles/chessground/chessground.base.css";
import "@/styles/chessground/chessground.brown.css";
import "@/styles/chessground/chessground.cburnett.css";

export interface BoardProps {
  fen: string;
  orientation?: "white" | "black";
  lastMove?: [Key, Key];
  bestMove?: string;             // uci e.g. "e2e4"
  movable?: boolean;
  onUserMove?: (uci: string, san: string, nextFen: string) => void;
}

function legalDests(fen: string): Map<Key, Key[]> {
  const chess = new Chess(fen);
  const dests = new Map<Key, Key[]>();
  for (const sq of SQUARES) {
    const moves = chess.moves({ square: sq, verbose: true });
    if (moves.length) dests.set(sq as Key, moves.map((m) => m.to as Key));
  }
  return dests;
}

function arrowFromUci(uci: string) {
  if (uci.length < 4) return null;
  return { orig: uci.slice(0, 2) as Key, dest: uci.slice(2, 4) as Key, brush: "green" };
}

export default function Board({ fen, orientation = "white", lastMove, bestMove, movable = false, onUserMove }: BoardProps) {
  const ref = useRef<HTMLDivElement>(null);
  const apiRef = useRef<Api | null>(null);
  // Latest fen + handler kept in refs so chessground's `after` callback always sees fresh values
  // without re-running the effect (which would tear down/reinit the board on every render).
  const fenRef = useRef(fen);
  const onMoveRef = useRef(onUserMove);
  fenRef.current = fen;
  onMoveRef.current = onUserMove;

  useEffect(() => {
    if (!ref.current) return;
    const config: Config = {
      fen,
      orientation,
      lastMove,
      coordinates: true,
      animation: { enabled: true, duration: 180 },
      movable: {
        free: false,
        color: undefined,
        dests: new Map(),
        events: {
          after: (orig, dest) => {
            const handler = onMoveRef.current;
            if (!handler) return;
            const chess = new Chess(fenRef.current);
            const move = chess.move({ from: orig, to: dest, promotion: "q" });
            if (!move) return;
            handler(move.from + move.to + (move.promotion ?? ""), move.san, chess.fen());
          },
        },
      },
      drawable: { enabled: false },
    };
    apiRef.current = Chessground(ref.current, config);
    return () => {
      apiRef.current?.destroy();
      apiRef.current = null;
    };
  }, []);

  useEffect(() => {
    const api = apiRef.current;
    if (!api) return;
    const dests = movable ? legalDests(fen) : new Map();
    const turnColor: "white" | "black" = fen.split(" ")[1] === "w" ? "white" : "black";
    const shapes = bestMove ? [arrowFromUci(bestMove)].filter(Boolean) as any[] : [];
    api.set({
      fen,
      orientation,
      lastMove,
      turnColor,
      movable: { free: false, color: movable ? turnColor : undefined, dests },
      drawable: { enabled: false, autoShapes: shapes },
    });
  }, [fen, orientation, lastMove?.[0], lastMove?.[1], bestMove, movable]);

  return (
    <div className="aspect-square w-full max-w-[520px] min-w-0 shrink-0 overflow-hidden">
      <div ref={ref} className="w-full h-full" />
    </div>
  );
}
