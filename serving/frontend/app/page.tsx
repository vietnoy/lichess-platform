"use client";

import { useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";
import { BarChart3, BrainCircuit, Database, Dumbbell, Search, UserRound } from "lucide-react";
import Header from "@/components/Header";
import StatusPill from "@/components/StatusPill";

const DEMO_PLAYER = "spoiltbrat12";
const DEMO_GAME_ID = "6KQfynAb";

const DEMO_LINKS = [
  {
    href: `/player/${DEMO_PLAYER}`,
    title: "Chẩn đoán người chơi",
    description: "Hồ sơ, điểm yếu, khai cuộc và bài tập từ lịch sử thật.",
    icon: UserRound,
  },
  {
    href: "/platform",
    title: "Meta nền tảng",
    description: "Xu hướng chung: tốc độ ván, khai cuộc phổ biến, lỗi theo giai đoạn.",
    icon: BarChart3,
  },
  {
    href: "/system",
    title: "Backbone dữ liệu",
    description: "Kafka, MinIO, Spark, Iceberg/Polaris, StarRocks và API serving.",
    icon: Database,
  },
  {
    href: "/coach",
    title: "AI Coach tiếng Việt",
    description: "Coach dùng tool và dữ liệu thật trước khi đưa ra chẩn đoán.",
    icon: BrainCircuit,
  },
  {
    href: "/drill",
    title: "Luyện tập lỗi sai",
    description: "Bài tập sinh từ critical positions và phân tích Stockfish.",
    icon: Dumbbell,
  },
];

export default function Home() {
  const [name, setName] = useState("");
  const [gameId, setGameId] = useState("");
  const router = useRouter();

  return (
    <div className="min-h-screen flex flex-col">
      <Header />
      <main className="flex-1 px-6 py-8">
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
          className="max-w-6xl mx-auto space-y-8"
        >
          <section className="grid lg:grid-cols-[1.1fr_0.9fr] gap-8 items-start">
            <header className="space-y-5 pt-2">
              <StatusPill tone="ok">Graduation thesis demo</StatusPill>
              <div className="space-y-4">
                <h1 className="text-4xl md:text-5xl font-medium tracking-tight max-w-3xl">
                  Chess Insight
                </h1>
                <p className="text-muted text-base md:text-lg leading-relaxed max-w-2xl">
                  Nền tảng phân tích Lichess thân thiện với người chơi Việt: từ dữ liệu lớn,
                  pipeline production, đến chẩn đoán lỗi sai và AI coach dựa trên lịch sử thật.
                </p>
              </div>
              <div className="flex flex-wrap gap-2 text-xs text-muted">
                <span className="border border-border bg-surface rounded-md px-2.5 py-1">Kafka ingestion</span>
                <span className="border border-border bg-surface rounded-md px-2.5 py-1">Lakehouse Iceberg</span>
                <span className="border border-border bg-surface rounded-md px-2.5 py-1">StarRocks serving</span>
                <span className="border border-border bg-surface rounded-md px-2.5 py-1">Stockfish analysis</span>
                <span className="border border-border bg-surface rounded-md px-2.5 py-1">Vietnamese AI coach</span>
              </div>
            </header>

            <div className="grid sm:grid-cols-2 gap-3">
              {DEMO_LINKS.map((item) => {
                const Icon = item.icon;
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={`border border-border bg-surface rounded-md p-4 hover:border-accent ${
                      item.href === `/player/${DEMO_PLAYER}` ? "sm:col-span-2" : ""
                    }`}
                  >
                    <div className="flex items-center gap-2">
                      <Icon size={18} className="text-accent" />
                      <h2 className="font-medium">{item.title}</h2>
                    </div>
                    <p className="text-sm text-muted leading-relaxed mt-3">{item.description}</p>
                  </Link>
                );
              })}
            </div>
          </section>

          <section className="grid lg:grid-cols-2 gap-4">
            <form
              onSubmit={(e) => {
                e.preventDefault();
                const v = name.trim();
                if (v) router.push(`/player/${encodeURIComponent(v)}`);
              }}
              className="border border-border bg-surface rounded-md p-4 space-y-3"
            >
              <div className="flex items-center gap-2">
                <Search size={17} className="text-accent" />
                <label className="font-medium">Tra cứu người chơi</label>
              </div>
              <div className="flex gap-2">
                <input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="Lichess username"
                  className="flex-1 min-w-0 bg-bg border border-border rounded-md px-3 py-2 outline-none focus:border-accent"
                />
                <button
                  type="submit"
                  className="bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90 active:opacity-80"
                >
                  Mở
                </button>
              </div>
              <p className="text-xs text-muted">
                Demo nhanh:{" "}
                <button
                  type="button"
                  onClick={() => router.push(`/player/${DEMO_PLAYER}`)}
                  className="text-accent hover:underline"
                >
                  {DEMO_PLAYER}
                </button>
              </p>
            </form>

            <form
              onSubmit={(e) => {
                e.preventDefault();
                const v = gameId.trim();
                if (v) router.push(`/game/${encodeURIComponent(v)}`);
              }}
              className="border border-border bg-surface rounded-md p-4 space-y-3"
            >
              <div className="flex items-center gap-2">
                <Search size={17} className="text-accent" />
                <label className="font-medium">Phân tích ván đấu</label>
              </div>
              <div className="flex gap-2">
                <input
                  value={gameId}
                  onChange={(e) => setGameId(e.target.value)}
                  placeholder={`Game ID, ví dụ ${DEMO_GAME_ID}`}
                  className="flex-1 min-w-0 bg-bg border border-border rounded-md px-3 py-2 outline-none focus:border-accent"
                />
                <button
                  type="submit"
                  className="bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90 active:opacity-80"
                >
                  Xem
                </button>
              </div>
              <p className="text-xs text-muted">
                Demo nhanh:{" "}
                <button
                  type="button"
                  onClick={() => router.push(`/game/${DEMO_GAME_ID}?player=${DEMO_PLAYER}`)}
                  className="text-accent hover:underline"
                >
                  {DEMO_GAME_ID}
                </button>
                {" "}· mở timeline, evaluation và What If cho từng nước đi.
              </p>
            </form>
          </section>

          <section className="grid md:grid-cols-3 gap-3">
            <div className="border border-border rounded-md p-4">
              <div className="text-xs text-muted">Product layer</div>
              <div className="font-medium mt-1">Profile, Meta, Coach, Drill</div>
            </div>
            <div className="border border-border rounded-md p-4">
              <div className="text-xs text-muted">Data layer</div>
              <div className="font-medium mt-1">Daily partitions + analyzer-derived tables</div>
            </div>
            <div className="border border-border rounded-md p-4">
              <div className="text-xs text-muted">Serving layer</div>
              <div className="font-medium mt-1">FastAPI + StarRocks APIs</div>
            </div>
          </section>
        </motion.div>
      </main>
    </div>
  );
}
