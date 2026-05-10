// Minimal SSE consumer over fetch. Yields { event, data } per server-sent message.
// We can't use EventSource because it doesn't support POST.

export interface SseMessage {
  event: string;
  data: any;
}

export async function* sseStream(
  url: string,
  init: RequestInit,
  signal?: AbortSignal,
): AsyncGenerator<SseMessage, void, void> {
  const res = await fetch(url, { ...init, signal });
  if (!res.ok || !res.body) {
    const text = await res.text().catch(() => "");
    throw new Error(`SSE ${res.status} ${res.statusText} ${text}`);
  }
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let idx: number;
    while ((idx = buffer.indexOf("\n\n")) !== -1) {
      const raw = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 2);
      const msg = parse(raw);
      if (msg) yield msg;
    }
  }
}

function parse(raw: string): SseMessage | null {
  let event = "message";
  let data = "";
  for (const line of raw.split("\n")) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) data += line.slice(5).trim();
  }
  if (!data) return null;
  try { return { event, data: JSON.parse(data) }; } catch { return { event, data }; }
}
