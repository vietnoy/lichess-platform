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
interface PlayerInsight {
  type: string;
  score: number;
  title: string;
  evidence: string;
  action: string;
  data?: Record<string, unknown>;
}
interface PlayerInsightsResponse { player_id: string; days: number; insights: PlayerInsight[]; }

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
  if (!value) return "Chưa đủ dữ liệu";
  const labels: Record<string, string> = {
    opening: "Khai cuộc",
    middlegame: "Trung cuộc",
    endgame: "Tàn cuộc",
    blunder: "Sai lầm nghiêm trọng",
    mistake: "Sai lầm",
    inaccuracy: "Nước thiếu chính xác",
    normal: "Không áp lực",
    under_10s: "Dưới 10 giây",
    under_30s: "Dưới 30 giây",
    white: "Cầm Trắng",
    black: "Cầm Đen",
  };
  return labels[value] ?? value.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
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

function Diagnosis({ weakness, openingStats, phaseStats }: {
  weakness: WeaknessSummary | null;
  openingStats: OpeningWeaknessRow[];
  phaseStats: PhaseWeaknessRow[];
}) {
  const worstOpening = openingStats[0];
  const topPhase = weakness?.top_phase ?? phaseStats[0]?.phase ?? null;

  return (
    <Card title="Chẩn đoán chính">
      {weakness ? (
        <div className="space-y-3">
          <p className="text-lg leading-relaxed">
            Điểm yếu lớn nhất hiện tại là <span className="text-accent font-medium">{labelize(topPhase)}</span>.
            Trong 60 ngày gần đây, hệ thống tìm thấy{" "}
            <span className="font-medium tabular-nums">{weakness.critical_positions.toLocaleString()}</span>{" "}
            thế cờ quan trọng từ các ván đã phân tích.
          </p>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm text-muted">
            <div>
              <span className="text-text font-medium tabular-nums">{weakness.blunders.toLocaleString()}</span>{" "}
              sai lầm nghiêm trọng cần luyện lại.
            </div>
            <div>
              <span className="text-text font-medium tabular-nums">{weakness.avg_eval_swing_cp ?? "n/a"}</span>{" "}
              centipawn là mức mất lợi thế trung bình.
            </div>
            <div>
              {worstOpening ? (
                <>
                  Khai cuộc cần chú ý:{" "}
                  <span className="text-text font-medium">{worstOpening.opening_eco} · {worstOpening.opening_name}</span>.
                </>
              ) : (
                "Chưa đủ dữ liệu khai cuộc để kết luận."
              )}
            </div>
          </div>
        </div>
      ) : (
        <div className="min-h-[92px] flex items-center text-muted text-sm">
          Đang đọc dữ liệu phân tích của người chơi...
        </div>
      )}
    </Card>
  );
}

function InsightBoard({ insights, loaded }: { insights: PlayerInsight[]; loaded: boolean }) {
  return (
    <Card title="Insight được ưu tiên">
      {insights.length > 0 ? (
        <div className="grid md:grid-cols-3 gap-3">
          {insights.slice(0, 3).map((item) => (
            <div key={`${item.type}-${item.title}`} className="border border-border rounded-md p-3 space-y-2">
              <div className="flex items-center justify-between gap-2">
                <div className="text-xs text-muted">{item.type.replace(/_/g, " ")}</div>
                <div className="text-xs font-mono text-accent">{item.score}</div>
              </div>
              <h3 className="font-medium leading-snug">{item.title}</h3>
              <p className="text-sm text-muted leading-relaxed">{item.evidence}</p>
              <p className="text-sm leading-relaxed">{item.action}</p>
            </div>
          ))}
        </div>
      ) : (
        <div className="min-h-[92px] flex items-center text-muted text-sm">
          {loaded ? "Chưa đủ signal để tạo ranked insights." : "Đang tính ranked insights từ profile, opening và critical positions..."}
        </div>
      )}
    </Card>
  );
}

export default function PlayerPage({ params }: { params: { name: string } }) {
  const username = decodeURIComponent(params.name);
  const router = useRouter();
  const [profile, setProfile] = useState<Profile | null>(null);
  const [weakness, setWeakness] = useState<WeaknessSummary | null>(null);
  const [openingStats, setOpeningStats] = useState<OpeningWeaknessRow[]>([]);
  const [phaseStats, setPhaseStats] = useState<PhaseWeaknessRow[]>([]);
  const [insights, setInsights] = useState<PlayerInsight[]>([]);
  const [coachLoaded, setCoachLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [coachError, setCoachError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    let alive = true;
    setError(null);
    setCoachError(null);
    setProfile(null);
    setWeakness(null);
    setOpeningStats([]);
    setPhaseStats([]);
    setInsights([]);
    setCoachLoaded(false);

    api<Profile>(`/players/${encodeURIComponent(username)}/profile`, { signal: controller.signal })
      .then((p) => { if (alive) setProfile(p); })
      .catch((e) => { if (alive) setError(String(e.message ?? e)); });

    Promise.all([
      api<WeaknessSummary>(`/players/${encodeURIComponent(username)}/weakness-summary?days=60`, { signal: controller.signal }),
      api<OpeningStatsResponse>(`/players/${encodeURIComponent(username)}/opening-stats?days=60&top_n=8`, { signal: controller.signal }),
      api<PhaseStatsResponse>(`/players/${encodeURIComponent(username)}/phase-stats?days=60`, { signal: controller.signal }),
      api<PlayerInsightsResponse>(`/players/${encodeURIComponent(username)}/insights?days=60`, { signal: controller.signal }),
    ])
      .then(([summary, openings, phases, insightResponse]) => {
        if (!alive) return;
        setWeakness(summary);
        setOpeningStats(openings.opening_stats ?? []);
        setPhaseStats(phases.phase_stats ?? []);
        setInsights(insightResponse.insights ?? []);
      })
      .catch((e) => { if (alive) setCoachError(String(e.message ?? e)); })
      .finally(() => { if (alive) setCoachLoaded(true); });

    return () => {
      alive = false;
      controller.abort();
    };
  }, [username]);

  const playerLoaded = Boolean(profile || weakness || coachLoaded);

  return (
    <>
      <Header subtitle={`Người chơi · ${username}`} />
      <main className="max-w-6xl mx-auto px-6 py-6 space-y-4">
        <div className="flex items-center gap-3">
          {!playerLoaded && !error && !coachError && <StatusPill tone="loading">Đang tải dữ liệu người chơi</StatusPill>}
          {error && <StatusPill tone="error">{error}</StatusPill>}
          {coachError && <StatusPill tone="error">{coachError}</StatusPill>}
          {profile && <StatusPill tone="ok">Đã tải · {profile.totals.games.toLocaleString()} ván</StatusPill>}
          {playerLoaded && (
            <a
              href={`/patterns/${encodeURIComponent(username)}`}
              className="ml-auto text-xs px-3 py-1 rounded-md border border-border hover:border-accent text-muted hover:text-text"
            >
              Mẫu lỗi lặp lại →
            </a>
          )}
        </div>

        <Diagnosis weakness={weakness} openingStats={openingStats} phaseStats={phaseStats} />

        <InsightBoard insights={insights} loaded={coachLoaded} />

        <Card title="Chỉ số huấn luyện · 60 ngày gần đây">
          {weakness ? (
            <div className="grid grid-cols-2 md:grid-cols-6 gap-6">
              <Metric label="Thế cờ quan trọng" value={weakness.critical_positions.toLocaleString()} />
              <Metric label="Ván có lỗi" value={weakness.games_with_critical_positions.toLocaleString()} />
              <Metric label="Lỗi nghiêm trọng" value={weakness.blunders.toLocaleString()} />
              <Metric label="Sai lầm" value={weakness.mistakes.toLocaleString()} />
              <Metric label="Giai đoạn yếu" value={labelize(weakness.top_phase)} />
              <Metric label="Mất lợi thế TB" value={weakness.avg_eval_swing_cp == null ? "n/a" : `${weakness.avg_eval_swing_cp} cp`} />
            </div>
          ) : (
            <div className="grid grid-cols-2 md:grid-cols-6 gap-6 min-h-[58px]">
              {["Thế cờ quan trọng", "Ván có lỗi", "Lỗi nghiêm trọng", "Sai lầm", "Giai đoạn yếu", "Mất lợi thế TB"].map((label) => (
                <Metric key={label} label={label} value={coachLoaded ? "n/a" : "…"} />
              ))}
            </div>
          )}
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <Card title="Điểm yếu theo giai đoạn">
            {phaseStats.length === 0 ? (
              <div className="h-[260px] flex items-center text-muted text-sm">
                {coachLoaded ? "Chưa đủ dữ liệu theo giai đoạn." : "Đang tải điểm yếu theo giai đoạn"}
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

          <Card title="Khai cuộc cần cải thiện">
            {openingStats.length === 0 ? (
              <div className="min-h-[260px] flex items-center text-muted text-sm">
                {coachLoaded ? "Chưa đủ dữ liệu khai cuộc." : "Đang tải điểm yếu khai cuộc"}
              </div>
            ) : (
              <div className="overflow-x-auto min-h-[260px]">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-muted text-xs uppercase tracking-wider">
                      <th className="text-left py-2 font-normal">Khai cuộc</th>
                      <th className="text-right py-2 font-normal">Ván</th>
                      <th className="text-right py-2 font-normal">Thắng</th>
                      <th className="text-right py-2 font-normal">Thế quan trọng</th>
                      <th className="text-right py-2 font-normal">Lỗi nặng</th>
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
                <Metric label="Tổng ván" value={profile.totals.games.toLocaleString()} />
                <Metric label="Tỷ lệ thắng" value={`${profile.totals.win_pct}%`} />
                <Metric label="Thắng" value={profile.totals.wins.toLocaleString()} />
                <Metric label="Thua" value={profile.totals.losses.toLocaleString()} />
                <Metric label="Hòa" value={profile.totals.draws.toLocaleString()} />
                <Metric label="Rating TB" value={profile.totals.avg_rating} />
              </div>
            </Card>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card title="Phân bố kết quả">
                <ResponsiveContainer width="100%" height={240}>
                  <PieChart>
                    <Pie
                      data={[
                        { name: "Thắng", value: profile.totals.wins, color: RESULT_COLORS.Win },
                        { name: "Thua", value: profile.totals.losses, color: RESULT_COLORS.Loss },
                        { name: "Hòa", value: profile.totals.draws, color: RESULT_COLORS.Draw },
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

              <Card title="Tỷ lệ thắng theo màu quân">
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

            <Card title="Khai cuộc thường chơi">
              {profile.openings.length === 0 ? (
                <p className="text-muted text-sm">Chưa đủ dữ liệu theo khai cuộc.</p>
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
                          formatter={(v: number) => [`${v}%`, "tỷ lệ thắng"]}
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
              <Card title="Áp lực thời gian theo giai đoạn">
                {phaseStats.length === 0 ? (
                  <p className="text-muted text-sm">Chưa có dữ liệu áp lực thời gian.</p>
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

              <Card title="Kết quả theo sức mạnh đối thủ">
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={profile.vs_rating.map((r) => ({ ...r, label: `${r.opponent} (n=${r.games})` }))}>
                    <XAxis dataKey="label" stroke="#888" fontSize={11} />
                    <YAxis stroke="#888" fontSize={12} domain={[0, 100]} />
                    <Tooltip
                      contentStyle={TOOLTIP_STYLE}
                      formatter={(v: number) => [`${v}%`, "tỷ lệ thắng"]}
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

            <Card title="Ván gần đây">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-muted text-xs uppercase tracking-wider">
                      <th className="text-left py-2 font-normal">Kết quả</th>
                      <th className="text-left py-2 font-normal">Đối thủ</th>
                      <th className="text-left py-2 font-normal">Rating</th>
                      <th className="text-left py-2 font-normal">Khai cuộc</th>
                      <th className="text-left py-2 font-normal">Thể loại</th>
                      <th className="text-left py-2 font-normal">Ngày</th>
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
