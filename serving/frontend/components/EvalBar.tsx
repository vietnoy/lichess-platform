"use client";

import { motion } from "framer-motion";

export default function EvalBar({ cp, mate }: { cp: number | null; mate: number | null }) {
  // Map evaluation to a 0..1 white-share. Clamp at +-5 pawns; mate flips to extremes.
  let whiteShare: number;
  let label: string;
  if (mate !== null) {
    whiteShare = mate > 0 ? 1 : 0;
    label = `M${Math.abs(mate)}`;
  } else if (cp !== null) {
    const clamped = Math.max(-500, Math.min(500, cp));
    whiteShare = 0.5 + clamped / 1000;
    label = (cp / 100).toFixed(1);
  } else {
    whiteShare = 0.5;
    label = "—";
  }

  // Sign the label so users see who is ahead at a glance.
  const signed =
    mate !== null
      ? `M${Math.abs(mate)}`
      : cp !== null
        ? `${cp >= 0 ? "+" : ""}${(cp / 100).toFixed(1)}`
        : "—";

  return (
    <div className="relative w-10 h-[520px] max-h-[80vh] rounded-md overflow-hidden bg-neutral-200 border border-border">
      <motion.div
        className="absolute bottom-0 left-0 right-0 bg-neutral-900"
        initial={{ height: "50%" }}
        animate={{ height: `${(1 - whiteShare) * 100}%` }}
        transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
      />
      <div className="absolute inset-x-0 top-1.5 text-xs text-center text-neutral-800 font-mono font-medium">
        {whiteShare > 0.5 ? signed : ""}
      </div>
      <div className="absolute inset-x-0 bottom-1.5 text-xs text-center text-neutral-100 font-mono font-medium">
        {whiteShare <= 0.5 ? signed : ""}
      </div>
    </div>
  );
}
