// Minimal SSE consumer over fetch. Yields { event, data } per server-sent message.
// EventSource is unusable here because it doesn't support POST.

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
  // Match \r\n\r\n or \n\n event boundaries (per the SSE spec).
  const BOUNDARY = /\r?\n\r?\n/;
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      // Flush any final character-set state and the last buffered event (if no trailing blank line).
      buffer += decoder.decode();
      const tail = buffer.trim();
      if (tail) {
        const msg = parse(tail);
        if (msg) yield msg;
      }
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    let m: RegExpExecArray | null;
    while ((m = BOUNDARY.exec(buffer))) {
      const raw = buffer.slice(0, m.index);
      buffer = buffer.slice(m.index + m[0].length);
      const msg = parse(raw);
      if (msg) yield msg;
    }
  }
}

function parse(raw: string): SseMessage | null {
  let event = "message";
  // The spec joins multiple `data:` lines with a newline, so we keep the newlines.
  const dataLines: string[] = [];
  for (const line of raw.split(/\r?\n/)) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
  }
  const data = dataLines.join("\n");
  if (!data) return null;
  try { return { event, data: JSON.parse(data) }; } catch { return { event, data }; }
}
