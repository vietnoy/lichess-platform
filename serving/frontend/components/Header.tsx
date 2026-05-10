"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV = [
  { href: "/", label: "Home" },
  { href: "/coach", label: "Coach" },
  { href: "/drill", label: "Drill" },
];

export default function Header({ subtitle }: { subtitle?: string }) {
  const path = usePathname();
  return (
    <header className="border-b border-border bg-bg/80 backdrop-blur sticky top-0 z-10">
      <div className="max-w-6xl mx-auto px-6 h-14 flex items-center justify-between">
        <Link href="/" className="font-medium tracking-tight hover:text-accent">
          Chess Coach
        </Link>
        {subtitle && <span className="text-xs text-muted hidden md:block">{subtitle}</span>}
        <nav className="flex gap-1 text-sm">
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
