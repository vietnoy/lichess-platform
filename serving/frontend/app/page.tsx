"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";

export default function Home() {
  const [name, setName] = useState("");
  const [gameId, setGameId] = useState("");
  const router = useRouter();

  return (
    <main className="min-h-screen flex items-center justify-center px-6">
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
        className="w-full max-w-md space-y-10"
      >
        <header className="space-y-2">
          <h1 className="text-3xl font-medium tracking-tight">Chess Coach</h1>
          <p className="text-muted text-sm leading-relaxed">
            Personal insights drawn from your real Lichess games.
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
              className="bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90 active:opacity-80"
            >
              Open
            </button>
          </div>
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
              className="bg-surface border border-border font-medium px-4 py-2 rounded-md hover:border-accent"
            >
              Explore
            </button>
          </div>
        </form>
      </motion.div>
    </main>
  );
}
