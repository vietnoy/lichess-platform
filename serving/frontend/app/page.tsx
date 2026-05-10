"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";
import Header from "@/components/Header";

export default function Home() {
  const [name, setName] = useState("");
  const [gameId, setGameId] = useState("");
  const router = useRouter();

  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <main className="flex-1 flex items-start justify-center px-6 pt-20 md:pt-28">
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
          className="w-full max-w-lg space-y-12"
        >
          <header className="space-y-4 text-center">
            <h1 className="text-5xl font-medium tracking-tight">Chess Coach</h1>
            <p className="text-muted text-base leading-relaxed max-w-md mx-auto">
              Analyze any Lichess game or player in seconds. Stockfish-powered move review,
              pattern detection across hundreds of games, and an AI coach trained on your real history.
            </p>
          </header>

          <form
            onSubmit={(e) => {
              e.preventDefault();
              const v = name.trim();
              if (v) router.push(`/player/${encodeURIComponent(v)}`);
            }}
            className="space-y-2"
          >
            <label className="text-xs uppercase tracking-wider text-muted">Player</label>
            <div className="flex gap-2">
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Lichess username"
                className="flex-1 bg-surface border border-border rounded-md px-3 py-2 outline-none focus:border-accent"
              />
              <button
                type="submit"
                className="bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90 active:opacity-80 transition-opacity"
                style={{ transitionDuration: "180ms", transitionTimingFunction: "cubic-bezier(0.22, 1, 0.36, 1)" }}
              >
                Open
              </button>
            </div>
            <p className="text-xs text-muted pl-1">
              Try{" "}
              <button
                type="button"
                onClick={() => router.push("/player/temporalmente")}
                className="text-accent hover:underline"
              >
                temporalmente
              </button>
            </p>
          </form>

          <form
            onSubmit={(e) => {
              e.preventDefault();
              const v = gameId.trim();
              if (v) router.push(`/game/${encodeURIComponent(v)}`);
            }}
            className="space-y-2"
          >
            <label className="text-xs uppercase tracking-wider text-muted">Game</label>
            <div className="flex gap-2">
              <input
                value={gameId}
                onChange={(e) => setGameId(e.target.value)}
                placeholder="Game ID (e.g. RPJr6MMX)"
                className="flex-1 bg-surface border border-border rounded-md px-3 py-2 outline-none focus:border-accent"
              />
              <button
                type="submit"
                className="bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90 active:opacity-80 transition-opacity"
                style={{ transitionDuration: "180ms", transitionTimingFunction: "cubic-bezier(0.22, 1, 0.36, 1)" }}
              >
                Explore
              </button>
            </div>
            <p className="text-xs text-muted pl-1">
              Find a game ID on{" "}
              <a
                href="https://lichess.org/games"
                target="_blank"
                rel="noopener noreferrer"
                className="text-accent hover:underline"
              >
                lichess.org/games
              </a>
            </p>
          </form>
        </motion.div>
      </main>
    </div>
  );
}
