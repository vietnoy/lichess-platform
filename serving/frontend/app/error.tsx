"use client";

import { useEffect } from "react";
import Header from "@/components/Header";

export default function GlobalError({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  return (
    <>
      <Header />
      <main className="max-w-md mx-auto px-6 py-24 space-y-4 text-center">
        <h1 className="text-2xl font-medium tracking-tight">Something went wrong</h1>
        <p className="text-muted text-sm break-words">
          {error.message || "An unexpected error occurred."}
        </p>
        {error.digest && (
          <p className="text-xs text-muted font-mono">ref: {error.digest}</p>
        )}
        <div className="flex justify-center gap-2 pt-2">
          <button
            onClick={reset}
            className="bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90"
          >
            Try again
          </button>
          <a
            href="/"
            className="border border-border text-text px-4 py-2 rounded-md hover:border-accent"
          >
            Home
          </a>
        </div>
      </main>
    </>
  );
}
