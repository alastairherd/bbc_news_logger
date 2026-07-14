import { InputError, deepSeekRequest, normalizeRequest, parseCompletion } from "./core.mjs";

const ALLOWED_ORIGINS = new Set([
  "https://alastairherd.github.io",
  "http://localhost:4321",
  "http://127.0.0.1:4321",
]);
const requestsByClient = new Map();

function headers(origin) {
  const output = {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
  };
  if (ALLOWED_ORIGINS.has(origin)) {
    output["Access-Control-Allow-Origin"] = origin;
    output["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS";
    output["Access-Control-Allow-Headers"] = "Content-Type";
    output.Vary = "Origin";
  }
  return output;
}

function json(value, status, origin) {
  return new Response(JSON.stringify(value), { status, headers: headers(origin) });
}

function rateLimited(request) {
  const client = request.headers.get("CF-Connecting-IP") ?? "unknown";
  const now = Date.now();
  const entries = (requestsByClient.get(client) ?? []).filter((time) => time > now - 600_000);
  if (entries.length >= 5) return true;
  entries.push(now);
  requestsByClient.set(client, entries);
  return false;
}

async function digest(value) {
  const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return [...new Uint8Array(bytes)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export default {
  async fetch(request, env, context) {
    const url = new URL(request.url);
    const origin = request.headers.get("Origin") ?? "";
    if (request.method === "OPTIONS") {
      return new Response(null, { status: ALLOWED_ORIGINS.has(origin) ? 204 : 403, headers: headers(origin) });
    }
    if (request.method === "GET" && url.pathname === "/api/health") {
      return json({ status: "ok", model: "deepseek-v4-flash", runtime: "cloudflare-workers" }, 200, origin);
    }
    if (request.method !== "POST" || url.pathname !== "/api/research") {
      return json({ detail: "Not found." }, 404, origin);
    }
    if (!ALLOWED_ORIGINS.has(origin)) return json({ detail: "Origin is not allowed." }, 403, origin);
    if (rateLimited(request)) return json({ detail: "Please wait before asking another archive question." }, 429, origin);
    const contentLength = Number(request.headers.get("Content-Length") ?? 0);
    if (contentLength > 40_000) return json({ detail: "Evidence payload is too large." }, 413, origin);

    let normalized;
    try {
      const text = await request.text();
      if (text.length > 40_000) throw new InputError("Evidence payload is too large.");
      normalized = normalizeRequest(JSON.parse(text));
    } catch (error) {
      const detail = error instanceof SyntaxError ? "Request body must be valid JSON." : error.message;
      return json({ detail }, 422, origin);
    }

    const canonical = JSON.stringify(normalized);
    const cacheKey = new Request(`https://archive-answer-cache.invalid/${await digest(canonical)}`);
    const cache = caches.default;
    const cached = await cache.match(cacheKey);
    if (cached) return json({ ...await cached.json(), cached: true }, 200, origin);
    if (!env.DEEPSEEK_API_KEY) return json({ detail: "DeepSeek is not configured." }, 503, origin);

    let provider;
    try {
      provider = await fetch("https://api.deepseek.com/chat/completions", {
        method: "POST",
        headers: { "Authorization": `Bearer ${env.DEEPSEEK_API_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify(deepSeekRequest(normalized.query, normalized.evidence)),
      });
      const payload = await provider.json();
      if (!provider.ok) throw new Error(String(payload?.error?.message ?? `DeepSeek returned ${provider.status}`));
      const answer = parseCompletion(payload, normalized.evidence.length);
      const cacheResponse = new Response(JSON.stringify(answer), {
        headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=86400" },
      });
      context.waitUntil(cache.put(cacheKey, cacheResponse));
      return json({ ...answer, cached: false }, 200, origin);
    } catch (error) {
      return json({ detail: String(error?.message ?? "DeepSeek could not answer from the archive.").slice(0, 500) }, 502, origin);
    }
  },
};
