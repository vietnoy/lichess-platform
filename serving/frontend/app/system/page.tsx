"use client";

import { useEffect, useMemo, useState } from "react";
import { Activity, Database, GitBranch, Layers3, Server, Workflow } from "lucide-react";
import Header from "@/components/Header";
import StatusPill from "@/components/StatusPill";
import { ApiError, api } from "@/lib/api";

interface SystemTable {
  name: string;
  full_name: string;
  description?: string;
  latest_partition_rows: number;
  latest_date: string | null;
}

interface SystemSummary {
  tables: SystemTable[];
  totals: {
    latest_partition_rows: number;
    tables: number;
    latest_date?: string | null;
  };
}

const LAYERS = [
  { name: "Kafka", detail: "Nhận luồng ván đấu theo thời gian thực.", icon: Activity },
  { name: "MinIO", detail: "Lưu raw data theo ngày để có thể xử lý lại.", icon: Database },
  { name: "Spark", detail: "Chuẩn hóa, dedupe và ghi dữ liệu phân vùng.", icon: Workflow },
  { name: "Iceberg + Polaris", detail: "Quản lý lakehouse table và metadata.", icon: GitBranch },
  { name: "StarRocks", detail: "Phục vụ truy vấn nhanh cho dashboard/API.", icon: Server },
  { name: "FastAPI + Next.js", detail: "Biến dữ liệu thành insight cho người chơi.", icon: Layers3 },
];

const TABLE_LABELS: Record<string, string> = {
  chess_move_events: "Nước đi raw",
  player_games: "Hồ sơ ván theo người chơi",
  move_evaluations_ondemand: "Đánh giá Stockfish",
  critical_positions: "Vị trí then chốt",
  player_weakness_summary: "Tổng hợp điểm yếu",
  player_opening_stats: "Thống kê khai cuộc",
  player_phase_stats: "Thống kê giai đoạn",
};

function compactNumber(value: number) {
  return new Intl.NumberFormat("vi-VN", {
    notation: value >= 1_000_000 ? "compact" : "standard",
    maximumFractionDigits: 1,
  }).format(value);
}

function fullNumber(value: number) {
  return new Intl.NumberFormat("vi-VN").format(value);
}

export default function SystemPage() {
  const [summary, setSummary] = useState<SystemSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const controller = new AbortController();
    let alive = true;
    setLoading(true);
    api<SystemSummary>("/system/summary", { signal: controller.signal })
      .then((data) => {
        if (!alive) return;
        setSummary(data);
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
      controller.abort();
    };
  }, []);

  const sortedTables = useMemo(() => {
    return [...(summary?.tables ?? [])].sort((a, b) => b.latest_partition_rows - a.latest_partition_rows);
  }, [summary]);

  return (
    <div className="min-h-screen">
      <Header subtitle="Hệ thống dữ liệu" />
      <main className="max-w-6xl mx-auto px-6 py-8 space-y-8">
        <section className="space-y-5">
          <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
            <div className="space-y-3">
              <StatusPill tone={loading ? "loading" : error ? "error" : "ok"}>
                {loading ? "Đang kiểm tra" : error ? "Cần kiểm tra" : "Đang hoạt động"}
              </StatusPill>
              <h1 className="text-3xl md:text-4xl font-medium tracking-tight">Hệ thống dữ liệu</h1>
              <p className="text-muted max-w-2xl leading-relaxed">
                Trang này tóm tắt backbone của sản phẩm: dữ liệu đi từ ingestion đến lakehouse,
                được phục vụ qua StarRocks rồi biến thành insight cho hồ sơ người chơi, bài tập và AI coach.
              </p>
            </div>
            <div className="grid grid-cols-3 gap-3 md:min-w-[420px]">
              <div className="border border-border bg-surface rounded-md p-4">
                <div className="text-xs text-muted">Dòng ngày mới nhất</div>
                <div className="text-2xl font-medium mt-1">
                  {summary ? compactNumber(summary.totals.latest_partition_rows) : "-"}
                </div>
              </div>
              <div className="border border-border bg-surface rounded-md p-4">
                <div className="text-xs text-muted">Bảng prod</div>
                <div className="text-2xl font-medium mt-1">{summary?.totals.tables ?? "-"}</div>
              </div>
              <div className="border border-border bg-surface rounded-md p-4">
                <div className="text-xs text-muted">Ngày mới nhất</div>
                <div className="text-lg font-medium mt-1 truncate">{summary?.totals.latest_date ?? "-"}</div>
              </div>
            </div>
          </div>
          {error && (
            <div className="border border-red-200 bg-red-50 text-red-700 rounded-md px-4 py-3 text-sm">
              Không tải được trạng thái hệ thống: {error}
            </div>
          )}
        </section>

        <section className="space-y-3">
          <h2 className="text-xl font-medium">Luồng xử lý</h2>
          <div className="grid md:grid-cols-3 gap-3">
            {LAYERS.map((layer) => {
              const Icon = layer.icon;
              return (
                <div key={layer.name} className="border border-border bg-surface rounded-md p-4 min-h-[126px]">
                  <div className="flex items-center gap-2">
                    <Icon size={18} className="text-accent" />
                    <div className="font-medium">{layer.name}</div>
                  </div>
                  <p className="text-sm text-muted leading-relaxed mt-3">{layer.detail}</p>
                </div>
              );
            })}
          </div>
        </section>

        <section className="space-y-3">
          <div className="flex items-center justify-between gap-4">
            <h2 className="text-xl font-medium">Bảng production</h2>
            <span className="text-xs text-muted">Đếm dòng trong partition mới nhất</span>
          </div>
          <div className="overflow-x-auto border border-border rounded-md bg-surface">
            <table className="w-full text-sm">
              <thead className="text-left text-xs text-muted border-b border-border">
                <tr>
                  <th className="px-4 py-3 font-medium">Bảng</th>
                  <th className="px-4 py-3 font-medium">Vai trò</th>
                  <th className="px-4 py-3 font-medium text-right">Dòng partition mới nhất</th>
                  <th className="px-4 py-3 font-medium">Ngày mới nhất</th>
                </tr>
              </thead>
              <tbody>
                {loading && sortedTables.length === 0
                  ? Array.from({ length: 7 }).map((_, idx) => (
                    <tr key={idx} className="border-b border-border/60 last:border-0">
                      <td className="px-4 py-4" colSpan={4}>
                        <div className="h-4 bg-border/60 rounded animate-pulse" />
                      </td>
                    </tr>
                  ))
                  : sortedTables.map((row) => (
                    <tr key={row.name} className="border-b border-border/60 last:border-0">
                      <td className="px-4 py-3">
                        <div className="font-medium">{TABLE_LABELS[row.name] ?? row.name}</div>
                        <div className="text-xs text-muted font-mono mt-1">{row.full_name}</div>
                      </td>
                      <td className="px-4 py-3 text-muted max-w-md">{row.description ?? "-"}</td>
                      <td className="px-4 py-3 text-right font-mono" title={fullNumber(row.latest_partition_rows)}>
                        {compactNumber(row.latest_partition_rows)}
                      </td>
                      <td className="px-4 py-3 font-mono">{row.latest_date ?? "-"}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </div>
  );
}
