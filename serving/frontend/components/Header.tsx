"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { api } from "@/lib/api";

const NAV = [
  { href: "/", label: "Trang chủ" },
  { href: "/coach", label: "AI Coach" },
  { href: "/drill", label: "Luyện tập" },
];

interface Freshness {
  data_through: string | null;
  days_available: number;
}

export default function Header({ subtitle }: { subtitle?: string }) {
  const path = usePathname();
  const [fresh, setFresh] = useState<Freshness | null>(null);

  useEffect(() => {
    let alive = true;
    api<Freshness>("/freshness")
      .then((f) => { if (alive) setFresh(f); })
      .catch(() => { /* freshness is best-effort, no UI noise */ });
    return () => { alive = false; };
  }, []);

  return (
    <header className="border-b border-border bg-bg/80 backdrop-blur sticky top-0 z-10">
      <div className="max-w-6xl mx-auto px-6 h-14 flex items-center justify-between gap-4">
        <Link href="/" className="font-medium tracking-tight hover:text-accent shrink-0">
          Chess Insight
        </Link>
        <div className="flex-1 flex items-center justify-center gap-3 min-w-0">
          {subtitle && <span className="text-xs text-muted hidden md:block truncate">{subtitle}</span>}
          {fresh?.data_through && (
            <span
              className="text-[10px] text-muted/70 hidden lg:block whitespace-nowrap font-mono"
              title={`${fresh.days_available} ngày dữ liệu đã được nạp`}
            >
              dữ liệu đến {fresh.data_through}
            </span>
          )}
        </div>
        <nav className="flex gap-1 text-sm shrink-0">
          {NAV.map((n) => {
            const active = path === n.href || (n.href !== "/" && path.startsWith(n.href));
            return (
              <Link
                key={n.href}
                href={n.href}
                className={`px-3 py-1.5 rounded-md ${
                  active ? "text-accent" : "text-muted hover:text-text"
                }`}
              >
                {n.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
