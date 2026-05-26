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

function share(value: number, total: number | undefined) {
  if (!total) return "-";
  return percent((value * 100) / total);
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
  const leadingSpeed = overview?.speed_mix[0];
  const leadingOpening = overview?.top_openings[0];
  const leadingPhase = overview?.phase_mistakes[0];

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
                Dashboard này biến một lượng lớn game data thành insight: người chơi đang chơi gì,
                meta opening nào xuất hiện nhiều, lỗi thường xảy ra ở phase nào, và hệ thống nên tạo
                coach/drill theo hướng nào.
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

        <section className="grid lg:grid-cols-3 gap-3">
          <div className="border border-border bg-surface rounded-md p-4 space-y-2">
            <div className="text-xs text-muted">Insight 1 · Player behavior</div>
            <p className="text-sm leading-relaxed">
              {leadingSpeed ? (
                <>
                  <span className="font-medium capitalize">{leadingSpeed.speed}</span> đang chiếm{" "}
                  <span className="font-mono">{share(leadingSpeed.games, overview?.totals.games)}</span> số ván.
                  Product nên ưu tiên phân tích nhịp chơi nhanh, time pressure và lỗi tactical ngắn hạn.
                </>
              ) : "Chưa đủ dữ liệu để kết luận time control chính."}
            </p>
          </div>
          <div className="border border-border bg-surface rounded-md p-4 space-y-2">
            <div className="text-xs text-muted">Insight 2 · Training focus</div>
            <p className="text-sm leading-relaxed">
              {leadingPhase ? (
                <>
                  Critical positions tập trung nhiều nhất ở{" "}
                  <span className="font-medium">{PHASE_LABELS[leadingPhase.phase] ?? leadingPhase.phase}</span>.
                  Drill generator nên lấy nhiều bài ở phase này để tăng xác suất sửa đúng lỗi thật.
                </>
              ) : "Analyzer chưa có đủ critical positions cho partition mới nhất."}
            </p>
          </div>
          <div className="border border-border bg-surface rounded-md p-4 space-y-2">
            <div className="text-xs text-muted">Insight 3 · Opening meta</div>
            <p className="text-sm leading-relaxed">
              {leadingOpening ? (
                <>
                  Opening được chơi nhiều nhất là{" "}
                  <span className="font-medium">{leadingOpening.opening_eco ?? "-"} · {leadingOpening.opening_name}</span>.
                  Đây là candidate tốt để so sánh win rate, mistake pattern và gợi ý repertoire.
                </>
              ) : "Chưa có dữ liệu opening meta."}
            </p>
          </div>
        </section>

        <section className="grid lg:grid-cols-2 gap-4">
          <div className="border border-border bg-surface rounded-md p-4 space-y-4">
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <BarChart3 size={18} className="text-accent" />
                <h2 className="font-medium">Time control mix</h2>
              </div>
              <p className="text-xs text-muted">
                Cho biết người dùng đang chơi loại ván nào để quyết định feature focus: speed, clock pressure, hay deep analysis.
              </p>
            </div>
            <div className="space-y-3">
              {(overview?.speed_mix ?? []).map((row) => (
                <div key={row.speed} className="space-y-1.5">
                  <div className="flex justify-between gap-3 text-sm">
                    <span className="capitalize">{row.speed}</span>
                    <span className="font-mono text-muted">
                      {number(row.games)} ván · {share(row.games, overview?.totals.games)} · avg Elo {number(row.avg_rating)}
                    </span>
                  </div>
                  <Bar value={row.games} max={maxSpeedGames} />
                </div>
              ))}
              {!loading && overview?.speed_mix.length === 0 && <p className="text-sm text-muted">Chưa có dữ liệu.</p>}
            </div>
          </div>

          <div className="border border-border bg-surface rounded-md p-4 space-y-4">
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <Swords size={18} className="text-accent" />
                <h2 className="font-medium">Mistake distribution by phase</h2>
              </div>
              <p className="text-xs text-muted">
                Cho biết lỗi nghiêm trọng đến từ opening, middlegame hay endgame để AI Coach chọn bài học đúng trọng tâm.
              </p>
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
          <div className="space-y-1">
            <h2 className="text-xl font-medium">Opening meta</h2>
            <p className="text-sm text-muted max-w-3xl leading-relaxed">
              Bảng này không chỉ xếp hạng opening theo popularity. Nó giúp trả lời: người chơi gặp cấu trúc nào nhiều nhất,
              opening nào có win rate đáng chú ý, và opening nào nên được đưa vào coach/drill/repertoire recommendation.
            </p>
          </div>
          <div className="overflow-x-auto border border-border rounded-md bg-surface">
            <table className="w-full text-sm">
              <thead className="text-left text-xs text-muted border-b border-border">
                <tr>
                  <th className="px-4 py-3 font-medium">Khai cuộc</th>
                  <th className="px-4 py-3 font-medium text-right">Games</th>
                  <th className="px-4 py-3 font-medium text-right">Win rate</th>
                  <th className="px-4 py-3 font-medium text-right">Critical positions</th>
                  <th className="px-4 py-3 font-medium">Popularity</th>
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

        <section className="grid md:grid-cols-3 gap-3">
          <div className="border border-border rounded-md p-4 space-y-2">
            <div className="text-xs text-muted">Data platform purpose</div>
            <p className="text-sm leading-relaxed">
              Ingestion và processing không chỉ để lưu data. Nó tạo fact tables và aggregate tables đủ nhanh để product hỏi được câu hỏi lớn.
            </p>
          </div>
          <div className="border border-border rounded-md p-4 space-y-2">
            <div className="text-xs text-muted">Decision layer</div>
            <p className="text-sm leading-relaxed">
              Meta dashboard quyết định nên ưu tiên phân tích speed nào, opening nào, phase nào và loại drill nào.
            </p>
          </div>
          <div className="border border-border rounded-md p-4 space-y-2">
            <div className="text-xs text-muted">User insight</div>
            <p className="text-sm leading-relaxed">
              AI Coach và Drill dùng cùng dữ liệu này để biến pattern toàn nền tảng và lỗi cá nhân thành lời khuyên hành động.
            </p>
          </div>
        </section>
      </main>
    </div>
  );
}
