"use client";

import { useEffect, useMemo, useState } from "react";
import { BarChart3, Gauge, Swords, Trophy } from "lucide-react";
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
  totals: {
    games: number;
    player_game_rows: number;
    players: number;
  };
  speed_mix: SpeedRow[];
  top_openings: OpeningRow[];
  phase_mistakes: PhaseRow[];
}

const PHASE_LABELS: Record<string, string> = {
  opening: "Khai cuộc",
  middlegame: "Trung cuộc",
  endgame: "Tàn cuộc",
  unknown: "Không rõ",
};

function number(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return new Intl.NumberFormat("vi-VN").format(value);
}

function percent(value: number | null | undefined) {
  if (value === null || value === undefined) return "-";
  return `${new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 1 }).format(value)}%`;
}

function Bar({ value, max }: { value: number; max: number }) {
  const width = max > 0 ? Math.max(4, Math.round((value / max) * 100)) : 0;
  return (
    <div className="h-2 bg-border rounded overflow-hidden">
      <div className="h-full bg-accent rounded" style={{ width: `${width}%` }} />
    </div>
  );
}

export default function PlatformPage() {
  const [overview, setOverview] = useState<PlatformOverview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    api<PlatformOverview>("/platform/overview")
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
  }, []);

  const maxSpeedGames = useMemo(
    () => Math.max(0, ...(overview?.speed_mix ?? []).map((row) => row.games)),
    [overview],
  );
  const maxOpeningGames = useMemo(
    () => Math.max(0, ...(overview?.top_openings ?? []).map((row) => row.games)),
    [overview],
  );
  const maxPhasePositions = useMemo(
    () => Math.max(0, ...(overview?.phase_mistakes ?? []).map((row) => row.critical_positions)),
    [overview],
  );

  return (
    <div className="min-h-screen">
      <Header subtitle="Meta nền tảng" />
      <main className="max-w-6xl mx-auto px-6 py-8 space-y-8">
        <section className="space-y-5">
          <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
            <div className="space-y-3">
              <StatusPill tone={loading ? "loading" : error ? "error" : "ok"}>
                {loading ? "Đang tải meta" : error ? "Cần kiểm tra" : `Dữ liệu ${overview?.date ?? "-"}`}
              </StatusPill>
              <h1 className="text-3xl md:text-4xl font-medium tracking-tight">Meta cờ vua trên nền tảng</h1>
              <p className="text-muted max-w-2xl leading-relaxed">
                Thay vì chỉ xem một người chơi, trang này gom dữ liệu production để nhìn xu hướng chung:
                tốc độ ván phổ biến, khai cuộc được chơi nhiều và giai đoạn dễ mắc lỗi.
              </p>
            </div>
            <div className="grid grid-cols-3 gap-3 md:min-w-[420px]">
              <div className="border border-border bg-surface rounded-md p-4">
                <div className="flex items-center gap-2 text-xs text-muted"><Trophy size={14} /> Ván</div>
                <div className="text-2xl font-medium mt-1">{number(overview?.totals.games)}</div>
              </div>
              <div className="border border-border bg-surface rounded-md p-4">
                <div className="flex items-center gap-2 text-xs text-muted"><Swords size={14} /> Người chơi</div>
                <div className="text-2xl font-medium mt-1">{number(overview?.totals.players)}</div>
              </div>
              <div className="border border-border bg-surface rounded-md p-4">
                <div className="flex items-center gap-2 text-xs text-muted"><Gauge size={14} /> Lượt hồ sơ</div>
                <div className="text-2xl font-medium mt-1">{number(overview?.totals.player_game_rows)}</div>
              </div>
            </div>
          </div>
          {error && (
            <div className="border border-red-200 bg-red-50 text-red-700 rounded-md px-4 py-3 text-sm">
              Không tải được meta nền tảng: {error}
            </div>
          )}
        </section>

        <section className="grid lg:grid-cols-2 gap-4">
          <div className="border border-border bg-surface rounded-md p-4 space-y-4">
            <div className="flex items-center gap-2">
              <BarChart3 size={18} className="text-accent" />
              <h2 className="font-medium">Tốc độ ván phổ biến</h2>
            </div>
            <div className="space-y-3">
              {(overview?.speed_mix ?? []).map((row) => (
                <div key={row.speed} className="space-y-1.5">
                  <div className="flex justify-between gap-3 text-sm">
                    <span className="capitalize">{row.speed}</span>
                    <span className="font-mono text-muted">{number(row.games)} ván · Elo TB {number(row.avg_rating)}</span>
                  </div>
                  <Bar value={row.games} max={maxSpeedGames} />
                </div>
              ))}
              {!loading && overview?.speed_mix.length === 0 && <p className="text-sm text-muted">Chưa có dữ liệu.</p>}
            </div>
          </div>

          <div className="border border-border bg-surface rounded-md p-4 space-y-4">
            <div className="flex items-center gap-2">
              <Swords size={18} className="text-accent" />
              <h2 className="font-medium">Lỗi theo giai đoạn</h2>
            </div>
            <div className="space-y-3">
              {(overview?.phase_mistakes ?? []).map((row) => (
                <div key={row.phase} className="space-y-1.5">
                  <div className="flex justify-between gap-3 text-sm">
                    <span>{PHASE_LABELS[row.phase] ?? row.phase}</span>
                    <span className="font-mono text-muted">
                      {number(row.critical_positions)} điểm · {number(row.blunders)} blunder
                    </span>
                  </div>
                  <Bar value={row.critical_positions} max={maxPhasePositions} />
                </div>
              ))}
              {!loading && overview?.phase_mistakes.length === 0 && <p className="text-sm text-muted">Chưa có dữ liệu.</p>}
            </div>
          </div>
        </section>

        <section className="space-y-3">
          <h2 className="text-xl font-medium">Khai cuộc đang nổi</h2>
          <div className="overflow-x-auto border border-border rounded-md bg-surface">
            <table className="w-full text-sm">
              <thead className="text-left text-xs text-muted border-b border-border">
                <tr>
                  <th className="px-4 py-3 font-medium">Khai cuộc</th>
                  <th className="px-4 py-3 font-medium text-right">Lượt chơi</th>
                  <th className="px-4 py-3 font-medium text-right">Tỷ lệ thắng</th>
                  <th className="px-4 py-3 font-medium text-right">Điểm lỗi</th>
                  <th className="px-4 py-3 font-medium">Quy mô</th>
                </tr>
              </thead>
              <tbody>
                {(overview?.top_openings ?? []).map((row) => (
                  <tr key={`${row.opening_eco}-${row.opening_name}`} className="border-b border-border/60 last:border-0">
                    <td className="px-4 py-3">
                      <div className="font-medium">{row.opening_eco ?? "-"} · {row.opening_name ?? "Unknown"}</div>
                    </td>
                    <td className="px-4 py-3 text-right font-mono">{number(row.games)}</td>
                    <td className="px-4 py-3 text-right font-mono">{percent(row.win_rate_pct)}</td>
                    <td className="px-4 py-3 text-right font-mono">{number(row.critical_positions)}</td>
                    <td className="px-4 py-3 min-w-[160px]"><Bar value={row.games} max={maxOpeningGames} /></td>
                  </tr>
                ))}
                {!loading && overview?.top_openings.length === 0 && (
                  <tr>
                    <td className="px-4 py-5 text-muted" colSpan={5}>Chưa có dữ liệu khai cuộc.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}
