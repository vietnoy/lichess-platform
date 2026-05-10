import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Chess Coach",
  description: "Personal insights from your real Lichess games.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen antialiased">{children}</body>
    </html>
  );
}
