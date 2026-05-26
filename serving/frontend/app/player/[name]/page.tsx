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
interface WeaknessSummary {
  player_id: string;
  days: number;
  critical_positions: number;
  games_with_critical_positions: number;
  blunders: number;
  mistakes: number;
  inaccuracies: number;
  avg_eval_swing_cp: number | null;
  time_pressure_positions: number;
  top_phase: string | null;
  top_time_pressure: string | null;
  top_classification: string | null;
}
interface OpeningWeaknessRow {
  opening_eco: string;
  opening_name: string;
  color: string;
  games: number;
  wins: number;
  losses: number;
  draws: number;
  win_rate_pct: number;
  critical_positions: number;
  blunders: number;
  mistakes: number;
  inaccuracies: number;
  avg_eval_swing_cp: number | null;
}
interface PhaseWeaknessRow {
  phase: string;
  games_with_positions: number;
  critical_positions: number;
  blunders: number;
  mistakes: number;
  inaccuracies: number;
  time_pressure_positions: number;
  avg_eval_swing_cp: number | null;
  max_eval_swing_cp: number | null;
}
interface OpeningStatsResponse { player_id: string; opening_stats: OpeningWeaknessRow[]; }
interface PhaseStatsResponse { player_id: string; phase_stats: PhaseWeaknessRow[]; }

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
const TOOLTIP_STYLE = { background: "rgb(255 255 255)", border: "1px solid rgb(220 220 228)", borderRadius: 6, fontSize: 12, color: "rgb(18 18 22)" };

function labelize(value: string | null | undefined) {
  if (!value) return "None yet";
  return value.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function pct(value: number | null | undefined) {
  return typeof value === "number" ? `${value}%` : "0%";
}

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
  const [weakness, setWeakness] = useState<WeaknessSummary | null>(null);
  const [openingStats, setOpeningStats] = useState<OpeningWeaknessRow[]>([]);
  const [phaseStats, setPhaseStats] = useState<PhaseWeaknessRow[]>([]);
  const [coachLoaded, setCoachLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [coachError, setCoachError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setError(null);
    setCoachError(null);
    setProfile(null);
    setWeakness(null);
    setOpeningStats([]);
    setPhaseStats([]);
    setCoachLoaded(false);

    api<Profile>(`/players/${encodeURIComponent(username)}/profile`)
      .then((p) => { if (alive) setProfile(p); })
      .catch((e) => { if (alive) setError(String(e.message ?? e)); });

    Promise.all([
      api<WeaknessSummary>(`/players/${encodeURIComponent(username)}/weakness-summary?days=60`),
      api<OpeningStatsResponse>(`/players/${encodeURIComponent(username)}/opening-stats?days=60&top_n=8`),
      api<PhaseStatsResponse>(`/players/${encodeURIComponent(username)}/phase-stats?days=60`),
    ])
      .then(([summary, openings, phases]) => {
        if (!alive) return;
        setWeakness(summary);
        setOpeningStats(openings.opening_stats ?? []);
        setPhaseStats(phases.phase_stats ?? []);
      })
      .catch((e) => { if (alive) setCoachError(String(e.message ?? e)); })
      .finally(() => { if (alive) setCoachLoaded(true); });

    return () => { alive = false; };
  }, [username]);

  const playerLoaded = Boolean(profile || weakness || coachLoaded);

  return (
    <>
      <Header subtitle={`Player · ${username}`} />
      <main className="max-w-6xl mx-auto px-6 py-6 space-y-4">
        <div className="flex items-center gap-3">
          {!playerLoaded && !error && !coachError && <StatusPill tone="loading">Loading player data</StatusPill>}
          {error && <StatusPill tone="error">{error}</StatusPill>}
          {coachError && <StatusPill tone="error">{coachError}</StatusPill>}
          {profile && <StatusPill tone="ok">Loaded · {profile.totals.games.toLocaleString()} games</StatusPill>}
          {playerLoaded && (
            <a
              href={`/patterns/${encodeURIComponent(username)}`}
              className="ml-auto text-xs px-3 py-1 rounded-md border border-border hover:border-accent text-muted hover:text-text"
            >
              Mistake patterns →
            </a>
          )}
        </div>

        <Card title="Coach snapshot · last 60 days">
          {weakness ? (
            <div className="grid grid-cols-2 md:grid-cols-6 gap-6">
              <Metric label="Critical spots" value={weakness.critical_positions.toLocaleString()} />
              <Metric label="Games affected" value={weakness.games_with_critical_positions.toLocaleString()} />
              <Metric label="Blunders" value={weakness.blunders.toLocaleString()} />
              <Metric label="Mistakes" value={weakness.mistakes.toLocaleString()} />
              <Metric label="Top phase" value={labelize(weakness.top_phase)} />
              <Metric label="Avg swing" value={weakness.avg_eval_swing_cp == null ? "n/a" : `${weakness.avg_eval_swing_cp} cp`} />
            </div>
          ) : (
            <div className="grid grid-cols-2 md:grid-cols-6 gap-6 min-h-[58px]">
              {["Critical spots", "Games affected", "Blunders", "Mistakes", "Top phase", "Avg swing"].map((label) => (
                <Metric key={label} label={label} value={coachLoaded ? "n/a" : "…"} />
              ))}
            </div>
          )}
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <Card title="Weakness by phase">
            {phaseStats.length === 0 ? (
              <div className="h-[260px] flex items-center text-muted text-sm">
                {coachLoaded ? "No analyzed phase data yet." : "Loading phase weaknesses"}
              </div>
            ) : (
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={phaseStats.map((r) => ({ ...r, phaseLabel: labelize(r.phase) }))}>
                  <XAxis dataKey="phaseLabel" stroke="#888" fontSize={12} />
                  <YAxis stroke="#888" fontSize={12} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Legend wrapperStyle={{ fontSize: 12, color: "#888" }} />
                  <Bar dataKey="blunders" stackId="a" fill="#f43f5e" radius={[0, 0, 0, 0]} />
                  <Bar dataKey="mistakes" stackId="a" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </Card>

          <Card title="Opening trouble spots">
            {openingStats.length === 0 ? (
              <div className="min-h-[260px] flex items-center text-muted text-sm">
                {coachLoaded ? "No analyzed opening weaknesses yet." : "Loading opening weaknesses"}
              </div>
            ) : (
              <div className="overflow-x-auto min-h-[260px]">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-muted text-xs uppercase tracking-wider">
                      <th className="text-left py-2 font-normal">Opening</th>
                      <th className="text-right py-2 font-normal">Games</th>
                      <th className="text-right py-2 font-normal">Win</th>
                      <th className="text-right py-2 font-normal">Critical</th>
                      <th className="text-right py-2 font-normal">Blunders</th>
                    </tr>
                  </thead>
                  <tbody>
                    {openingStats.map((o) => (
                      <tr key={`${o.opening_eco}-${o.opening_name}-${o.color}`} className="border-t border-border">
                        <td className="py-2 pr-3">
                          <div className="font-mono text-xs text-muted">{o.opening_eco} · {labelize(o.color)}</div>
                          <div className="max-w-[18rem] truncate" title={o.opening_name}>{o.opening_name}</div>
                        </td>
                        <td className="py-2 text-right tabular-nums">{o.games.toLocaleString()}</td>
                        <td className="py-2 text-right tabular-nums">{pct(o.win_rate_pct)}</td>
                        <td className="py-2 text-right tabular-nums">{o.critical_positions.toLocaleString()}</td>
                        <td className="py-2 text-right tabular-nums text-rose-500">{o.blunders.toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Card>
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
                      stroke="rgb(255 255 255)"
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
              <Card title="Time pressure by phase">
                {phaseStats.length === 0 ? (
                  <p className="text-muted text-sm">No analyzed time-pressure data yet.</p>
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={phaseStats.map((r) => ({ ...r, phaseLabel: labelize(r.phase) }))}>
                      <XAxis dataKey="phaseLabel" stroke="#888" fontSize={12} />
                      <YAxis stroke="#888" fontSize={12} />
                      <Tooltip contentStyle={TOOLTIP_STYLE} />
                      <Bar dataKey="time_pressure_positions" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
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
