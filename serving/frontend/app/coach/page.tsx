"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";

import Header from "@/components/Header";
import StatusPill from "@/components/StatusPill";
import { sseStream } from "@/lib/sse";

type ToolStatus = "running" | "done" | "error";

interface ToolCall {
  id: string;
  name: string;
  args: Record<string, any>;
  status: ToolStatus;
  summary?: string;
}

interface ChatMessage {
  role: "user" | "assistant";
  text: string;
  toolCalls: ToolCall[];
  streaming?: boolean;
}

const SESSION_KEY = "chess-coach-session";

function newSessionId(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

function loadSession(): string {
  if (typeof window === "undefined") return "";
  let sid = window.localStorage.getItem(SESSION_KEY);
  if (!sid) {
    sid = newSessionId();
    window.localStorage.setItem(SESSION_KEY, sid);
  }
  return sid;
}

export default function CoachPage() {
  const [sessionId, setSessionId] = useState("");
  const [username, setUsername] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  // Generation counter: each new send/clear bumps it. Late events from an aborted stream are dropped.
  const genRef = useRef(0);

  useEffect(() => { setSessionId(loadSession()); }, []);
  useEffect(() => { scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" }); }, [messages]);

  function clearConversation() {
    genRef.current += 1;
    abortRef.current?.abort();
    abortRef.current = null;
    const sid = newSessionId();
    window.localStorage.setItem(SESSION_KEY, sid);
    setSessionId(sid);
    setMessages([]);
    setError(null);
    setBusy(false);
  }

  async function send() {
    const text = input.trim();
    if (!text || busy) return;
    setInput("");
    setError(null);

    const userMsg: ChatMessage = { role: "user", text, toolCalls: [] };
    const assistantMsg: ChatMessage = { role: "assistant", text: "", toolCalls: [], streaming: true };
    setMessages((m) => [...m, userMsg, assistantMsg]);
    setBusy(true);

    const ctrl = new AbortController();
    abortRef.current = ctrl;
    const myGen = ++genRef.current;
    try {
      const stream = sseStream(
        "/api/coach",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ session_id: sessionId, message: text, username: username || null }),
        },
        ctrl.signal,
      );
      for await (const { event, data } of stream) {
        if (genRef.current !== myGen) return;     // stream was superseded; drop late events
        applyEvent(event, data);
      }
    } catch (e) {
      if (genRef.current !== myGen) return;
      const msg = e instanceof Error ? e.message : String(e);
      if (!ctrl.signal.aborted) setError(msg);
    } finally {
      if (genRef.current === myGen) {
        setBusy(false);
        finalizeStreaming();
      }
    }
  }

  function applyEvent(event: string, data: any) {
    setMessages((prev) => {
      const copy = [...prev];
      const last = copy[copy.length - 1];
      if (!last || last.role !== "assistant") return prev;
      const next = { ...last, toolCalls: [...last.toolCalls] };

      if (event === "token" && typeof data.text === "string") {
        next.text = (next.text || "") + data.text;
      } else if (event === "tool_start" && typeof data.name === "string") {
        next.toolCalls.push({
          id: `${data.name}-${Date.now()}-${next.toolCalls.length}`,
          name: data.name, args: data.args ?? {}, status: "running",
        });
      } else if (event === "tool_result" && typeof data.name === "string") {
        for (let i = next.toolCalls.length - 1; i >= 0; i--) {
          if (next.toolCalls[i].name === data.name && next.toolCalls[i].status === "running") {
            next.toolCalls[i] = { ...next.toolCalls[i], status: "done", summary: data.summary };
            break;
          }
        }
      } else if (event === "error") {
        setError(data.message ?? "Unknown error");
      }
      copy[copy.length - 1] = next;
      return copy;
    });
  }

  function finalizeStreaming() {
    setMessages((prev) => {
      if (!prev.length) return prev;
      const last = prev[prev.length - 1];
      if (last.role !== "assistant" || !last.streaming) return prev;
      return [...prev.slice(0, -1), { ...last, streaming: false }];
    });
  }

  const empty = messages.length === 0;
  const status = useMemo(() => {
    if (error) return { tone: "error" as const, label: error };
    if (busy) return { tone: "loading" as const, label: "Thinking" };
    if (sessionId) return { tone: "ok" as const, label: "Ready" };
    return { tone: "idle" as const, label: "Initializing" };
  }, [busy, error, sessionId]);

  return (
    <>
      <Header subtitle={username ? `Coach for ${username}` : "AI Coach"} />
      <main className="max-w-3xl mx-auto px-6 py-6 flex flex-col h-[calc(100vh-3.5rem)]">
        <div className="flex items-center gap-3 mb-3">
          <StatusPill tone={status.tone}>{status.label}</StatusPill>
          <input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Lichess username (optional)"
            className="flex-1 bg-surface border border-border rounded-md px-3 py-1.5 text-sm outline-none focus:border-accent"
          />
          <button
            onClick={clearConversation}
            className="px-3 py-1.5 rounded-md border border-border text-sm text-muted hover:text-text hover:border-accent"
          >
            New chat
          </button>
        </div>

        <div ref={scrollRef} className="flex-1 overflow-y-auto space-y-4 pr-1">
          {empty && (
            <div className="h-full flex items-center justify-center">
              <div className="text-center space-y-2 max-w-md">
                <p className="text-sm text-muted">
                  Ask anything about a player&apos;s game. The coach will query your real Lichess data and answer with numbers, never guesses.
                </p>
                <p className="text-xs text-muted">
                  Try: <em>&ldquo;What openings is khangdv-hub losing the most?&rdquo;</em>
                </p>
              </div>
            </div>
          )}
          {messages.map((m, i) => <Bubble key={i} msg={m} />)}
        </div>

        <form
          onSubmit={(e) => { e.preventDefault(); send(); }}
          className="mt-3 flex gap-2"
        >
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={busy}
            placeholder={busy ? "Coach is thinking…" : "Ask the coach…"}
            className="flex-1 bg-surface border border-border rounded-md px-3 py-2 outline-none focus:border-accent disabled:opacity-60"
          />
          <button
            type="submit"
            disabled={busy || !input.trim()}
            className="bg-accent text-bg font-medium px-4 py-2 rounded-md hover:opacity-90 disabled:opacity-40"
          >
            Send
          </button>
        </form>
      </main>
    </>
  );
}

function Bubble({ msg }: { msg: ChatMessage }) {
  const isUser = msg.role === "user";
  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
      className={`flex ${isUser ? "justify-end" : "justify-start"}`}
    >
      <div className={`max-w-[85%] space-y-2 ${isUser ? "" : "w-full"}`}>
        {!isUser && msg.toolCalls.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            <AnimatePresence>
              {msg.toolCalls.map((t) => (
                <motion.div
                  key={t.id}
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.16 }}
                >
                  <ToolChip tool={t} />
                </motion.div>
              ))}
            </AnimatePresence>
          </div>
        )}
        <div
          className={
            isUser
              ? "bg-accent/15 text-text rounded-md px-3.5 py-2 text-sm whitespace-pre-wrap"
              : "bg-surface border border-border rounded-md px-3.5 py-2.5 text-sm leading-relaxed whitespace-pre-wrap"
          }
        >
          {msg.text || (msg.streaming && msg.toolCalls.length === 0 ? <span className="text-muted">…</span> : null)}
          {msg.streaming && msg.text && <span className="inline-block w-1.5 h-4 align-middle ml-0.5 bg-accent animate-pulse" />}
        </div>
      </div>
    </motion.div>
  );
}

function ToolChip({ tool }: { tool: ToolCall }) {
  const tone =
    tool.status === "running" ? "bg-accent/15 text-accent" :
    tool.status === "error"   ? "bg-rose-500/15 text-rose-400" :
                                "bg-emerald-500/10 text-emerald-400";
  return (
    <span className={`inline-flex items-center gap-2 px-2 py-1 rounded-md text-[11px] font-mono ${tone}`}>
      {tool.status === "running" && <span className="w-1.5 h-1.5 rounded-full bg-current animate-pulse" />}
      {tool.name}
      {tool.summary && <span className="text-muted">· {tool.summary}</span>}
    </span>
  );
}
