"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { LucideIcon } from "lucide-react";
import { Activity, BarChart3, Crosshair, Gauge, PieChart as PieIcon, Swords, Trophy } from "lucide-react";
import Header from "@/components/Header";
import StatusPill from "@/components/StatusPill";
import { ApiError, api } from "@/lib/api";

interface SpeedRow {
  speed: string;
  games: number;
  player_game_rows: number;
  avg_rating: number | null;
}

interface OpeningRow {
  opening_eco: string | null;
  opening_name: string | null;
  games: number;
  win_rate_pct: number | null;
  critical_positions: number;
}

interface PhaseRow {
  phase: string;
  critical_positions: number;
  blunders: number;
  mistakes: number;
  inaccuracies: number;
}

interface PlatformOverview {
  date: string | null;
  start_date: string | null;
  end_date: string | null;
  range: string;
  totals: {
    games: number;
    player_game_rows: number;
    players: number;
  };
  speed_mix: SpeedRow[];
  top_openings: OpeningRow[];
  phase_mistakes: PhaseRow[];
}

type RangeMode = "14" | "30" | "60" | "all" | "custom";

const COLORS = ["#f59e0b", "#10b981", "#3b82f6", "#ef4444", "#8b5cf6", "#64748b"];
const TOOLTIP_STYLE = {
  background: "rgb(255 255 255)",
  border: "1px solid rgb(220 220 228)",
  borderRadius: 6,
  fontSize: 12,
  color: "rgb(18 18 22)",
};

const PHASE_LABELS: Record<string, string> = {
  opening: "Opening",
  middlegame: "Middlegame",
  endgame: "Endgame",
  unknown: "Unknown",
};

function number(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return new Intl.NumberFormat("vi-VN").format(value);
}

function compact(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return new Intl.NumberFormat("vi-VN", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function percent(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return `${new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 1 }).format(value)}%`;
}

function share(value: number, total: number | undefined) {
  if (!total) return "-";
  return percent((value * 100) / total);
}

function openingLabel(row: OpeningRow) {
  return `${row.opening_eco ?? "-"} · ${row.opening_name ?? "Unknown"}`;
}

function platformOverviewPath(rangeMode: RangeMode, customDate: string) {
  if (rangeMode === "all") return "/platform/overview?all_time=true";
  if (rangeMode === "custom") return customDate ? `/platform/overview?date=${encodeURIComponent(customDate)}` : null;
  return `/platform/overview?days=${rangeMode}`;
}

function rangeLabel(overview: PlatformOverview | null, rangeMode: RangeMode) {
  if (!overview) return "-";
  if (rangeMode === "all" || overview.range === "all") return "All time";
  if (overview.range === "date") return overview.date ?? "-";
  if (overview.start_date && overview.end_date) return `${overview.start_date} → ${overview.end_date}`;
  return overview.date ?? "-";
}

function SectionHeader({
  icon: Icon,
  title,
  desc,
}: {
  icon: LucideIcon;
  title: string;
  desc: string;
}) {
  return (
    <div className="flex items-start justify-between gap-4">
      <div className="space-y-1">
        <div className="flex items-center gap-2">
          <Icon size={18} className="text-accent" />
          <h2 className="font-medium">{title}</h2>
        </div>
        <p className="text-xs text-muted leading-relaxed max-w-2xl">{desc}</p>
      </div>
    </div>
  );
}

function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <section className={`border border-border bg-surface rounded-md p-4 ${className}`}>{children}</section>;
}

function ChartEmpty({ title, desc }: { title: string; desc: string }) {
  return (
    <div className="h-full min-h-[220px] border border-dashed border-border rounded-md flex items-center justify-center p-6 text-center">
      <div className="space-y-2 max-w-sm">
        <div className="text-sm font-medium">{title}</div>
        <p className="text-xs text-muted leading-relaxed">{desc}</p>
      </div>
    </div>
  );
}

export default function PlatformPage() {
  const [overview, setOverview] = useState<PlatformOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [rangeMode, setRangeMode] = useState<RangeMode>("30");
  const [customDate, setCustomDate] = useState("");

  const overviewPath = useMemo(() => platformOverviewPath(rangeMode, customDate), [rangeMode, customDate]);

  useEffect(() => {
    if (!overviewPath) {
      setLoading(false);
      return;
    }
    let alive = true;
    setLoading(true);
    api<PlatformOverview>(overviewPath)
      .then((data) => {
        if (!alive) return;
        setOverview(data);
        setError(null);
      })
      .catch((e) => {
        if (!alive) return;
        setError(e instanceof ApiError ? e.message : String(e));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, [overviewPath]);

  const speedData = useMemo(() => overview?.speed_mix ?? [], [overview]);
  const phaseData = useMemo(
    () => (overview?.phase_mistakes ?? []).map((row) => ({ ...row, phaseLabel: PHASE_LABELS[row.phase] ?? row.phase })),
    [overview],
  );
  const openingData = useMemo(
    () => (overview?.top_openings ?? []).map((row) => ({ ...row, label: openingLabel(row) })),
    [overview],
  );
  const leadingSpeed = speedData[0];
  const leadingPhase = phaseData[0];
  const leadingOpening = openingData[0];
  const sharpestOpening = useMemo(() => {
    return [...openingData]
      .filter((row) => row.win_rate_pct !== null)
      .sort((a, b) => Math.abs((b.win_rate_pct ?? 50) - 50) - Math.abs((a.win_rate_pct ?? 50) - 50))[0];
  }, [openingData]);

  return (
    <div className="min-h-screen">
      <Header subtitle="Meta dashboard" />
      <main className="max-w-7xl mx-auto px-6 py-8 space-y-6">
        <section className="grid xl:grid-cols-[1.15fr_0.85fr] gap-4 items-stretch">
          <div className="border border-border bg-surface rounded-md p-5 space-y-4">
            <StatusPill tone={loading ? "loading" : error ? "error" : "ok"}>
              {loading ? "Đang tải meta" : error ? "Cần kiểm tra" : `Window · ${rangeLabel(overview, rangeMode)}`}
            </StatusPill>
            <div className="space-y-3">
              <h1 className="text-3xl md:text-4xl font-medium tracking-tight">Platform Meta</h1>
              <p className="text-muted leading-relaxed max-w-3xl">
                Một dashboard để nhìn toàn cảnh cộng đồng đang chơi gì, opening nào nổi bật, phase nào sinh nhiều lỗi,
                và hệ thống nên biến các pattern đó thành training decision như thế nào.
              </p>
            </div>
            <div className="flex flex-col md:flex-row md:items-center gap-3">
              <div className="flex flex-wrap gap-2">
                {[
                  ["14", "14 ngày"],
                  ["30", "30 ngày"],
                  ["60", "60 ngày"],
                  ["all", "All time"],
                ].map(([value, label]) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => setRangeMode(value as RangeMode)}
                    className={`h-9 px-3 rounded-md border text-sm transition ${
                      rangeMode === value
                        ? "border-accent bg-accent text-white"
                        : "border-border bg-background hover:bg-border/40"
                    }`}
                  >
                    {label}
                  </button>
                ))}
              </div>
              <label className="flex items-center gap-2 text-sm text-muted">
                Một ngày
                <input
                  type="date"
                  value={customDate}
                  onChange={(event) => {
                    setCustomDate(event.target.value);
                    setRangeMode("custom");
                  }}
                  className="h-9 rounded-md border border-border bg-background px-3 text-sm text-text"
                />
              </label>
            </div>
            {error && <div className="border border-red-200 bg-red-50 text-red-700 rounded-md px-4 py-3 text-sm">{error}</div>}
          </div>

          <div className="grid grid-cols-3 gap-3">
            <Card>
              <div className="flex items-center gap-2 text-xs text-muted"><Trophy size={14} /> Games</div>
              <div className="text-2xl font-medium mt-2">{compact(overview?.totals.games)}</div>
              <div className="text-xs text-muted mt-1">raw match volume</div>
            </Card>
            <Card>
              <div className="flex items-center gap-2 text-xs text-muted"><Swords size={14} /> Players</div>
              <div className="text-2xl font-medium mt-2">{compact(overview?.totals.players)}</div>
              <div className="text-xs text-muted mt-1">unique accounts</div>
            </Card>
            <Card>
              <div className="flex items-center gap-2 text-xs text-muted"><Gauge size={14} /> Rows</div>
              <div className="text-2xl font-medium mt-2">{compact(overview?.totals.player_game_rows)}</div>
              <div className="text-xs text-muted mt-1">player-game facts</div>
            </Card>
          </div>
        </section>

        <section className="grid lg:grid-cols-4 gap-3">
          <Card className="space-y-2">
            <div className="text-xs text-muted">Main behavior</div>
            <p className="text-sm leading-relaxed">
              {leadingSpeed ? (
                <>
                  <span className="font-medium capitalize">{leadingSpeed.speed}</span> chiếm{" "}
                  <span className="font-mono">{share(leadingSpeed.games, overview?.totals.games)}</span>. Training nên
                  ưu tiên pattern recognition và speed decision.
                </>
              ) : (
                "Chưa đủ dữ liệu."
              )}
            </p>
          </Card>
          <Card className="space-y-2">
            <div className="text-xs text-muted">Training hotspot</div>
            <p className="text-sm leading-relaxed">
              {leadingPhase ? (
                <>
                  <span className="font-medium">{leadingPhase.phaseLabel}</span> có nhiều critical positions nhất. Drill
                  nên lấy position từ phase này.
                </>
              ) : (
                "Chưa có critical positions."
              )}
            </p>
          </Card>
          <Card className="space-y-2">
            <div className="text-xs text-muted">Opening trend</div>
            <p className="text-sm leading-relaxed">
              {leadingOpening ? (
                <>
                  <span className="font-medium">{leadingOpening.label}</span> là opening phổ biến nhất trong sample mới.
                </>
              ) : (
                "Chưa có opening meta."
              )}
            </p>
          </Card>
          <Card className="space-y-2">
            <div className="text-xs text-muted">Decision signal</div>
            <p className="text-sm leading-relaxed">
              {sharpestOpening ? (
                <>
                  <span className="font-medium">{sharpestOpening.opening_eco}</span> có win rate lệch khỏi 50% rõ nhất.
                  Đây là candidate để phân tích repertoire.
                </>
              ) : (
                "Chưa đủ win rate signal."
              )}
            </p>
          </Card>
        </section>

        <section className="grid xl:grid-cols-[0.9fr_1.1fr] gap-4">
          <Card className="space-y-4">
            <SectionHeader
              icon={PieIcon}
              title="Time control share"
              desc="Trả lời: phần lớn người chơi đang chơi ở nhịp nào? Điều này quyết định sản phẩm nên ưu tiên fast tactical insight hay deep review."
            />
            <div className="grid md:grid-cols-[240px_1fr] gap-4 items-center">
              {speedData.length > 0 ? (
                <ResponsiveContainer width="100%" height={240}>
                  <PieChart>
                    <Pie data={speedData} dataKey="games" nameKey="speed" innerRadius={58} outerRadius={92} paddingAngle={2}>
                      {speedData.map((row, index) => <Cell key={row.speed} fill={COLORS[index % COLORS.length]} />)}
                    </Pie>
                    <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(value: number) => [number(value), "games"]} />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <ChartEmpty title="Chưa có speed mix" desc="Khi nightly process nạp player-game facts, chart này sẽ hiện phân bổ bullet, blitz, rapid và các mode khác." />
              )}
              <div className="space-y-2">
                {speedData.map((row, index) => (
                  <div key={row.speed} className="flex items-center justify-between gap-3 text-sm">
                    <div className="flex items-center gap-2 min-w-0">
                      <span className="w-2.5 h-2.5 rounded-sm shrink-0" style={{ background: COLORS[index % COLORS.length] }} />
                      <span className="capitalize truncate">{row.speed}</span>
                    </div>
                    <span className="font-mono text-muted whitespace-nowrap">{share(row.games, overview?.totals.games)}</span>
                  </div>
                ))}
              </div>
            </div>
          </Card>

          <Card className="space-y-4">
            <SectionHeader
              icon={BarChart3}
              title="Volume vs average Elo"
              desc="Trả lời: mode nào vừa đông người chơi vừa có level trung bình cao? Đây là nơi meta có nhiều ý nghĩa nhất."
            />
            {speedData.length > 0 ? (
              <ResponsiveContainer width="100%" height={288}>
                <BarChart data={speedData} margin={{ left: 8, right: 20 }}>
                  <CartesianGrid stroke="rgb(220 220 228)" vertical={false} />
                  <XAxis dataKey="speed" stroke="#777" fontSize={12} />
                  <YAxis yAxisId="left" stroke="#777" fontSize={12} tickFormatter={compact} />
                  <YAxis yAxisId="right" orientation="right" stroke="#777" fontSize={12} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Bar yAxisId="left" dataKey="games" name="Games" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                  <Bar yAxisId="right" dataKey="avg_rating" name="Avg Elo" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <ChartEmpty title="Chưa có rating distribution" desc="Chart này cần player-game facts từ prod table để so sánh volume và average Elo theo speed." />
            )}
          </Card>
        </section>

        <section className="grid xl:grid-cols-[1.05fr_0.95fr] gap-4">
          <Card className="space-y-4">
            <SectionHeader
              icon={Crosshair}
              title="Opening popularity map"
              desc="Trả lời: người chơi gặp opening nào nhiều nhất? Đây là map để quyết định nên review repertoire nào trước."
            />
            {openingData.length > 0 ? (
              <ResponsiveContainer width="100%" height={360}>
                <BarChart data={openingData.slice(0, 8).reverse()} layout="vertical" margin={{ left: 12, right: 20 }}>
                  <CartesianGrid stroke="rgb(220 220 228)" horizontal={false} />
                  <XAxis type="number" stroke="#777" fontSize={12} tickFormatter={compact} />
                  <YAxis
                    dataKey="label"
                    type="category"
                    width={230}
                    stroke="#777"
                    fontSize={11}
                    tickFormatter={(value: string) => (value.length > 34 ? `${value.slice(0, 33)}...` : value)}
                  />
                  <Tooltip contentStyle={TOOLTIP_STYLE} formatter={(value: number) => [number(value), "games"]} />
                  <Bar dataKey="games" name="Games" fill="#10b981" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <ChartEmpty title="Opening meta chưa sẵn sàng" desc="Khi analyzer-derived tables có dữ liệu cho partition mới, chart này sẽ xếp hạng opening theo popularity và training signal." />
            )}
          </Card>

          <Card className="space-y-4">
            <SectionHeader
              icon={Swords}
              title="Mistake stack by phase"
              desc="Trả lời: lỗi ở phase nào là blunder, mistake hay inaccuracy? Chart này nối meta với personalized drill."
            />
            {phaseData.length > 0 ? (
              <ResponsiveContainer width="100%" height={360}>
                <BarChart data={phaseData} margin={{ left: 8, right: 20 }}>
                  <CartesianGrid stroke="rgb(220 220 228)" vertical={false} />
                  <XAxis dataKey="phaseLabel" stroke="#777" fontSize={12} />
                  <YAxis stroke="#777" fontSize={12} />
                  <Tooltip contentStyle={TOOLTIP_STYLE} />
                  <Legend wrapperStyle={{ fontSize: 12 }} />
                  <Bar dataKey="blunders" stackId="a" name="Blunders" fill="#ef4444" />
                  <Bar dataKey="mistakes" stackId="a" name="Mistakes" fill="#f59e0b" />
                  <Bar dataKey="inaccuracies" stackId="a" name="Inaccuracies" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            ) : (
              <ChartEmpty title="Chưa có mistake stack" desc="Chart này cần critical positions từ move evaluations để phân tích lỗi theo opening, middlegame và endgame." />
            )}
          </Card>
        </section>

        <section className="space-y-3">
          <SectionHeader
            icon={Activity}
            title="Opening leaderboard"
            desc="Bảng ranking để scan nhanh opening nào phổ biến, win rate đang lệch, và có critical positions đáng chú ý."
          />
          <div className="overflow-x-auto border border-border rounded-md bg-surface">
            <table className="w-full text-sm">
              <thead className="text-left text-xs text-muted border-b border-border">
                <tr>
                  <th className="px-4 py-3 font-medium">#</th>
                  <th className="px-4 py-3 font-medium">Opening</th>
                  <th className="px-4 py-3 font-medium text-right">Games</th>
                  <th className="px-4 py-3 font-medium text-right">Share</th>
                  <th className="px-4 py-3 font-medium text-right">Win rate</th>
                  <th className="px-4 py-3 font-medium text-right">Critical</th>
                </tr>
              </thead>
              <tbody>
                {openingData.map((row, index) => (
                  <tr key={row.label} className="border-b border-border/60 last:border-0">
                    <td className="px-4 py-3 text-muted font-mono">{index + 1}</td>
                    <td className="px-4 py-3 font-medium max-w-md truncate" title={row.label}>{row.label}</td>
                    <td className="px-4 py-3 text-right font-mono">{number(row.games)}</td>
                    <td className="px-4 py-3 text-right font-mono">{share(row.games, overview?.totals.games)}</td>
                    <td className="px-4 py-3 text-right font-mono">{percent(row.win_rate_pct)}</td>
                    <td className="px-4 py-3 text-right font-mono">{number(row.critical_positions)}</td>
                  </tr>
                ))}
                {!loading && openingData.length === 0 && (
                  <tr><td className="px-4 py-5 text-muted" colSpan={6}>Chưa có opening meta.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="border border-border rounded-md p-4 space-y-3">
          <h2 className="font-medium">What this means for training</h2>
          <div className="grid md:grid-cols-3 gap-3 text-sm">
            <p className="leading-relaxed">Time control mix quyết định training tempo: blitz/bullet cần fast pattern recognition, rapid/classical cần deeper review.</p>
            <p className="leading-relaxed">Mistake stack quyết định drill source: phase nào nhiều blunder nhất thì lấy nhiều critical positions từ phase đó.</p>
            <p className="leading-relaxed">Opening leaderboard quyết định repertoire focus: opening phổ biến nhưng win rate lệch là nơi coach nên giải thích trước.</p>
          </div>
        </section>
      </main>
    </div>
  );
}
