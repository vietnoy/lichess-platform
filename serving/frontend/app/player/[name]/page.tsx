"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";
import {
  ResponsiveContainer, PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis, Tooltip, Legend,
} from "recharts";

import Header from "@/components/Header";
import StatusPill from "@/components/StatusPill";
import { api } from "@/lib/api";

interface OverviewBySpeed { speed: string; total_games: number; wins: number; losses: number; draws: number; avg_rating: number; }
interface ByColor { color: "White" | "Black"; games: number; win_pct: number; }
interface OpeningRow { opening_eco: string; opening_name: string; games: number; win_pct: number; }
interface ClockRow { phase: string; avg_clock_s: number; }
interface VsRatingRow { opponent: string; games: number; win_pct: number; }
interface RecentRow { game_id: string; opponent: string; my_rating: number; opp_rating: number; opening_eco: string; opening_name: string; speed: string; result: "Win" | "Loss" | "Draw"; date: string; }

interface Profile {
  username: string;
  totals: { games: number; wins: number; losses: number; draws: number; win_pct: number; avg_rating: number; };
  by_speed: OverviewBySpeed[];
  by_color: ByColor[];
  openings: OpeningRow[];
  clock_by_phase: ClockRow[];
  vs_rating: VsRatingRow[];
  recent_games: RecentRow[];
}

const RESULT_COLORS = { Win: "#10b981", Loss: "#f43f5e", Draw: "#737373" };
const TOOLTIP_STYLE = { background: "rgb(22 22 26)", border: "1px solid rgb(38 38 44)", borderRadius: 6, fontSize: 12 };

function Card({ title, children, className = "" }: { title?: string; children: React.ReactNode; className?: string }) {
  return (
    <motion.section
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
      className={`bg-surface border border-border rounded-md p-4 ${className}`}
    >
      {title && <h3 className="text-xs uppercase tracking-wider text-muted mb-3">{title}</h3>}
      {children}
    </motion.section>
  );
}

function Metric({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="space-y-1">
      <div className="text-xs uppercase tracking-wider text-muted">{label}</div>
      <div className="text-2xl font-medium tabular-nums">{value}</div>
    </div>
  );
}

export default function PlayerPage({ params }: { params: { name: string } }) {
  const username = decodeURIComponent(params.name);
  const router = useRouter();
  const [profile, setProfile] = useState<Profile | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setError(null);
    api<Profile>(`/players/${encodeURIComponent(username)}/profile`)
      .then((p) => { if (alive) setProfile(p); })
      .catch((e) => { if (alive) setError(String(e.message ?? e)); });
    return () => { alive = false; };
  }, [username]);

  return (
    <>
      <Header subtitle={`Player · ${username}`} />
      <main className="max-w-6xl mx-auto px-6 py-6 space-y-4">
        <div className="flex items-center gap-3">
          {!profile && !error && <StatusPill tone="loading">Loading profile</StatusPill>}
          {error && <StatusPill tone="error">{error}</StatusPill>}
          {profile && <StatusPill tone="ok">Loaded · {profile.totals.games.toLocaleString()} games</StatusPill>}
        </div>

        {profile && (
          <>
            <Card>
              <div className="grid grid-cols-2 md:grid-cols-6 gap-6">
                <Metric label="Total games" value={profile.totals.games.toLocaleString()} />
                <Metric label="Win rate" value={`${profile.totals.win_pct}%`} />
                <Metric label="Wins" value={profile.totals.wins.toLocaleString()} />
                <Metric label="Losses" value={profile.totals.losses.toLocaleString()} />
                <Metric label="Draws" value={profile.totals.draws.toLocaleString()} />
                <Metric label="Avg rating" value={profile.totals.avg_rating} />
              </div>
            </Card>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card title="Result distribution">
                <ResponsiveContainer width="100%" height={240}>
                  <PieChart>
                    <Pie
                      data={[
                        { name: "Wins", value: profile.totals.wins, color: RESULT_COLORS.Win },
                        { name: "Losses", value: profile.totals.losses, color: RESULT_COLORS.Loss },
                        { name: "Draws", value: profile.totals.draws, color: RESULT_COLORS.Draw },
                      ]}
                      dataKey="value"
                      innerRadius={50}
                      outerRadius={85}
                      stroke="rgb(22 22 26)"
                    >
                      <Cell fill={RESULT_COLORS.Win} />
                      <Cell fill={RESULT_COLORS.Loss} />
                      <Cell fill={RESULT_COLORS.Draw} />
                    </Pie>
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                    <Legend wrapperStyle={{ fontSize: 12, color: "#888" }} />
                  </PieChart>
                </ResponsiveContainer>
              </Card>

              <Card title="Win rate by color">
                <ResponsiveContainer width="100%" height={240}>
                  <BarChart data={profile.by_color}>
                    <XAxis dataKey="color" stroke="#888" fontSize={12} />
                    <YAxis stroke="#888" fontSize={12} domain={[0, 100]} />
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                    <Bar dataKey="win_pct" radius={[4, 4, 0, 0]}>
                      {profile.by_color.map((r) => (
                        <Cell
                          key={r.color}
                          fill={r.color === "White" ? "#f5f5f5" : "#0f172a"}
                          stroke={r.color === "Black" ? "#475569" : undefined}
                          strokeWidth={r.color === "Black" ? 1 : 0}
                        />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            </div>

            <Card title="Top openings (win %)">
              {profile.openings.length === 0 ? (
                <p className="text-muted text-sm">Not enough games per opening yet.</p>
              ) : (
                (() => {
                  // Build unique labels: ECO codes can repeat (e.g. multiple A40 sub-variations).
                  // YAxis dataKey must be unique per row or Recharts collapses them.
                  const data = profile.openings.map((o) => ({
                    ...o,
                    label: `${o.opening_eco} · ${o.opening_name.length > 28 ? o.opening_name.slice(0, 27) + "…" : o.opening_name} (${o.games}g)`,
                  }));
                  return (
                    <ResponsiveContainer width="100%" height={Math.max(240, data.length * 32)}>
                      <BarChart data={data} layout="vertical" margin={{ left: 8, right: 24 }}>
                        <XAxis type="number" stroke="#888" fontSize={12} domain={[0, 100]} />
                        <YAxis
                          dataKey="label"
                          type="category"
                          stroke="#aaa"
                          fontSize={11}
                          width={280}
                          interval={0}
                          tick={{ textAnchor: "end" }}
                        />
                        <Tooltip
                          contentStyle={TOOLTIP_STYLE}
                          formatter={(v: number) => [`${v}%`, "win rate"]}
                        />
                        <Bar dataKey="win_pct" radius={[0, 4, 4, 0]}>
                          {data.map((r) => (
                            <Cell key={r.label} fill={r.win_pct >= 55 ? "#10b981" : r.win_pct >= 45 ? "#f59e0b" : "#f43f5e"} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  );
                })()
              )}
            </Card>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card title="Clock remaining by phase (avg)">
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={profile.clock_by_phase}>
                    <XAxis dataKey="phase" stroke="#888" fontSize={12} />
                    <YAxis stroke="#888" fontSize={12} unit="s" />
                    <Tooltip contentStyle={TOOLTIP_STYLE} />
                    <Bar dataKey="avg_clock_s" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </Card>

              <Card title="Performance vs opponent strength">
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={profile.vs_rating.map((r) => ({ ...r, label: `${r.opponent} (n=${r.games})` }))}>
                    <XAxis dataKey="label" stroke="#888" fontSize={11} />
                    <YAxis stroke="#888" fontSize={12} domain={[0, 100]} />
                    <Tooltip
                      contentStyle={TOOLTIP_STYLE}
                      formatter={(v: number) => [`${v}%`, "win rate"]}
                    />
                    <Bar dataKey="win_pct" radius={[4, 4, 0, 0]}>
                      {profile.vs_rating.map((r) => (
                        <Cell key={r.opponent} fill={r.win_pct >= 55 ? "#10b981" : r.win_pct >= 45 ? "#f59e0b" : "#f43f5e"} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            </div>

            <Card title="Recent games">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-muted text-xs uppercase tracking-wider">
                      <th className="text-left py-2 font-normal">Result</th>
                      <th className="text-left py-2 font-normal">Opponent</th>
                      <th className="text-left py-2 font-normal">Rating</th>
                      <th className="text-left py-2 font-normal">Opening</th>
                      <th className="text-left py-2 font-normal">Speed</th>
                      <th className="text-left py-2 font-normal">Date</th>
                    </tr>
                  </thead>
                  <tbody>
                    {profile.recent_games.map((g) => (
                      <tr
                        key={g.game_id}
                        onClick={() => router.push(`/game/${encodeURIComponent(g.game_id)}`)}
                        className="border-t border-border hover:bg-border/30 cursor-pointer transition-colors"
                        style={{ transitionDuration: "180ms" }}
                      >
                        <td className="py-2">
                          <span style={{ color: RESULT_COLORS[g.result] }} className="font-medium">{g.result}</span>
                        </td>
                        <td className="py-2 font-mono">{g.opponent}</td>
                        <td className="py-2 tabular-nums text-muted">{g.my_rating} vs {g.opp_rating}</td>
                        <td className="py-2 text-muted max-w-[20rem]">
                          <span className="block truncate" title={g.opening_name ?? ""}>
                            {g.opening_name ?? "—"}
                          </span>
                        </td>
                        <td className="py-2 text-muted capitalize">{g.speed}</td>
                        <td className="py-2 text-muted tabular-nums">{g.date}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          </>
        )}
      </main>
    </>
  );
}
