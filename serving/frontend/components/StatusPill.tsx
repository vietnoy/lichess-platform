"use client";

type Tone = "idle" | "loading" | "ok" | "warn" | "error";

const COLOR: Record<Tone, string> = {
  idle: "bg-border text-muted",
  loading: "bg-accent/20 text-accent",
  ok: "bg-emerald-500/15 text-emerald-400",
  warn: "bg-amber-500/15 text-amber-300",
  error: "bg-rose-500/15 text-rose-400",
};

export default function StatusPill({ tone, children }: { tone: Tone; children: React.ReactNode }) {
  return (
    <span className={`inline-flex items-center gap-2 px-2.5 py-1 rounded-md text-xs font-medium ${COLOR[tone]}`}>
      {tone === "loading" && <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse" />}
      {children}
    </span>
  );
}
