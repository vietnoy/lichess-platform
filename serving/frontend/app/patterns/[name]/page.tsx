"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { motion } from "framer-motion";
import {
  ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, Cell,
} from "recharts";

import Header from "@/components/Header";
import StatusPill from "@/components/StatusPill";
import { api, ApiError } from "@/lib/api";

interface PhaseRow      { phase: string; blunders: number; mistakes: number; inaccuracies: number; }
interface BucketRow     { bucket: string; blunders: number; mistakes: number; }
interface ClockRow      { pressure: string; blunders: number; mistakes: number; }
interface OpeningRow    { opening_eco: string; opening_name: string; blunders: number; mistakes: number; games: number; }
interface WorstGameRow  { game_id: string; blunders: number; mistakes: number; date: string; opponent: string; opening_name: string; }

interface Patterns {
  totals: { games_analyzed: number; blunders: number; mistakes: number; inaccuracies: number };
  by_phase: PhaseRow[];
  by_move_bucket: BucketRow[];
  by_clock: ClockRow[];
  by_opening: OpeningRow[];
  worst_games: WorstGameRow[];
}

const TT = { background: "rgb(22 22 26)", border: "1px solid rgb(38 38 44)", borderRadius: 6, fontSize: 12 };

function Card({ title, children, className = "" }: { title: string; children: React.ReactNode; className?: string }) {
  return (
    <motion.section
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
      className={`bg-surface border border-border rounded-md p-4 ${className}`}
    >
      <h3 className="text-xs uppercase tracking-wider text-muted mb-3">{title}</h3>
      {children}
    </motion.section>
  );
}

function Metric({ label, value, tone }: { label: string; value: string | number; tone?: "rose" | "amber" | "muted" }) {
  const color = tone === "rose" ? "text-rose-400" : tone === "amber" ? "text-amber-400" : "text-text";
  return (
    <div className="space-y-1">
      <div className="text-xs uppercase tracking-wider text-muted">{label}</div>
      <div className={`text-2xl font-medium tabular-nums ${color}`}>{value}</div>
    </div>
  );
}

export default function PatternsPage({ params }: { params: { name: string } }) {
  const username = decodeURIComponent(params.name);
  const [data, setData] = useState<Patterns | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [is404, setIs404] = useState(false);

  useEffect(() => {
    let alive = true;
    setError(null);
    setIs404(false);
    api<Patterns>(`/players/${encodeURIComponent(username)}/patterns`)
      .then((p) => { if (alive) setData(p); })
      .catch((e) => {
        if (!alive) return;
        if (e instanceof ApiError && e.status === 404) {
          setIs404(true);
        } else {
          setError(String(e.message ?? e));
        }
      });
    return () => { alive = false; };
  }, [username]);

  return (
    <>
      <Header subtitle={`Patterns · ${username}`} />
      <main className="max-w-6xl mx-auto px-6 py-6 space-y-4">
        <div className="flex items-center gap-3">
          {!data && !error && !is404 && <StatusPill tone="loading">Aggregating blunders</StatusPill>}
          {error && <StatusPill tone="error">{error}</StatusPill>}
          {is404 && (
            <StatusPill tone="warn">
              No analyzed games yet for this player. Run the analyzer DAG to populate patterns.
            </StatusPill>
          )}
          {data && <StatusPill tone="ok">{data.totals.games_analyzed.toLocaleString()} games analyzed</StatusPill>}
          <Link
            href={`/player/${encodeURIComponent(username)}`}
            className="text-xs px-3 py-1 rounded-md border border-border hover:border-accent text-muted hover:text-text"
          >
            ← back to dashboard
          </Link>
        </div>

        {data && (
          <>
            <Card title="Mistakes overview">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                <Metric label="Games analyzed" value={data.totals.games_analyzed.toLocaleString()} />
                <Metric label="Blunders"      value={data.totals.blunders.toLocaleString()}      tone="rose" />
                <Metric label="Mistakes"      value={data.totals.mistakes.toLocaleString()}      tone="amber" />
                <Metric label="Inaccuracies"  value={data.totals.inaccuracies.toLocaleString()}  tone="muted" />
              </div>
            </Card>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card title="Mistakes by phase">
                {data.by_phase.length === 0 ? <Empty/> : (
                  <ResponsiveContainer width="100%" height={240}>
                    <BarChart data={data.by_phase}>
                      <XAxis dataKey="phase" stroke="#888" fontSize={12} tickFormatter={cap} />
                      <YAxis stroke="#888" fontSize={12} />
                      <Tooltip contentStyle={TT} />
                      <Bar dataKey="blunders"    stackId="a" fill="#f43f5e" />
                      <Bar dataKey="mistakes"    stackId="a" fill="#f59e0b" />
                      <Bar dataKey="inaccuracies" stackId="a" fill="#737373" />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </Card>

              <Card title="Mistakes by clock pressure">
                {data.by_clock.length === 0 ? <Empty/> : (
                  <ResponsiveContainer width="100%" height={240}>
                    <BarChart data={data.by_clock}>
                      <XAxis dataKey="pressure" stroke="#888" fontSize={12} tickFormatter={cap} />
                      <YAxis stroke="#888" fontSize={12} />
                      <Tooltip contentStyle={TT} />
                      <Bar dataKey="blunders" stackId="a" fill="#f43f5e" />
                      <Bar dataKey="mistakes" stackId="a" fill="#f59e0b" />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </Card>
            </div>

            <Card title="Mistakes by move number">
              {data.by_move_bucket.length === 0 ? <Empty/> : (
                <ResponsiveContainer width="100%" height={260}>
                  <BarChart data={data.by_move_bucket}>
                    <XAxis dataKey="bucket" stroke="#888" fontSize={12} />
                    <YAxis stroke="#888" fontSize={12} />
                    <Tooltip contentStyle={TT} />
                    <Bar dataKey="blunders" stackId="a" fill="#f43f5e" />
                    <Bar dataKey="mistakes" stackId="a" fill="#f59e0b" />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </Card>

            <Card title="Worst openings (most blunders)">
              {data.by_opening.length === 0 ? <Empty/> : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-muted text-xs uppercase tracking-wider text-left">
                        <th className="py-2 font-normal">Opening</th>
                        <th className="py-2 font-normal text-right">Games</th>
                        <th className="py-2 font-normal text-right">Blunders</th>
                        <th className="py-2 font-normal text-right">Mistakes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.by_opening.map((o) => (
                        <tr key={`${o.opening_eco}-${o.opening_name}`} className="border-t border-border">
                          <td className="py-2">
                            <span className="text-muted font-mono">{o.opening_eco}</span>{" "}
                            <span>{o.opening_name}</span>
                          </td>
                          <td className="py-2 text-right tabular-nums text-muted">{o.games}</td>
                          <td className="py-2 text-right tabular-nums text-rose-400">{o.blunders}</td>
                          <td className="py-2 text-right tabular-nums text-amber-400">{o.mistakes}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>

            <Card title="Worst recent games">
              {data.worst_games.length === 0 ? <Empty/> : (
                <ul className="space-y-2 text-sm">
                  {data.worst_games.map((g) => (
                    <li key={g.game_id}>
                      <Link
                        href={`/game/${encodeURIComponent(g.game_id)}`}
                        className="flex items-center justify-between gap-3 px-2 py-2 rounded hover:bg-border/30"
                      >
                        <span className="flex items-center gap-3 min-w-0">
                          <span className="text-muted tabular-nums shrink-0">{g.date}</span>
                          <span className="font-mono shrink-0">vs {g.opponent}</span>
                          <span className="text-muted truncate">{g.opening_name}</span>
                        </span>
                        <span className="flex items-center gap-3 shrink-0 tabular-nums">
                          <span className="text-rose-400">{g.blunders} blunders</span>
                          <span className="text-amber-400">{g.mistakes} mistakes</span>
                        </span>
                      </Link>
                    </li>
                  ))}
                </ul>
              )}
            </Card>
          </>
        )}
      </main>
    </>
  );
}

function Empty() {
  return <p className="text-muted text-sm">No data in this slice yet.</p>;
}

function cap(s: string): string { return s.charAt(0).toUpperCase() + s.slice(1); }
