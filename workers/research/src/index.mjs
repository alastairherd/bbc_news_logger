import { InputError, MODEL, deepSeekRequest, maximumRequestCostUsd, normalizeRequest, parseCompletion } from "./core.mjs";
import { checkCollection, dispatchCollection } from "./schedule.mjs";

export { AnswerBudget } from "./budget.mjs";

const ALLOWED_ORIGINS = new Set([
  "https://alastairherd.github.io",
  "http://localhost:4321",
  "http://127.0.0.1:4321",
]);
const PROMPT_VERSION = "2026-07-14-v2";
const requestsByClient = new Map();
const DEFAULT_DAILY_BUDGET_USD = 0.5;
const BUDGET_EXHAUSTED = "Today's shared answer budget has been used. Answers resume after midnight UTC.";

function answerBudget(env) {
  return env.ANSWER_BUDGET?.get(env.ANSWER_BUDGET.idFromName("global"));
}

function dailyLimitUsd(env) {
  const value = Number(env.DAILY_BUDGET_USD);
  return Number.isFinite(value) && value >= 0 ? value : DEFAULT_DAILY_BUDGET_USD;
}

const utcDay = () => new Date().toISOString().slice(0, 10);

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
      const budget = answerBudget(env);
      const answerBudgetStatus = budget ? { limitUsd: dailyLimitUsd(env), spentUsd: await budget.spent(utcDay()) } : null;
      return json({ status: "ok", model: MODEL, runtime: "cloudflare-workers", reasoningModes: ["off", "high"], answerBudget: answerBudgetStatus }, 200, origin);
    }
    if (request.method !== "POST" || url.pathname !== "/api/research") {
      return json({ detail: "Not found." }, 404, origin);
    }
    if (!ALLOWED_ORIGINS.has(origin)) return json({ detail: "Origin is not allowed." }, 403, origin);
    if (rateLimited(request)) return json({ detail: "Please wait before asking another archive question." }, 429, origin);
    const contentLength = Number(request.headers.get("Content-Length") ?? 0);
    if (contentLength > 80_000) return json({ detail: "Evidence payload is too large." }, 413, origin);

    let normalized;
    try {
      const text = await request.text();
      if (text.length > 80_000) throw new InputError("Evidence payload is too large.");
      normalized = normalizeRequest(JSON.parse(text));
    } catch (error) {
      const detail = error instanceof SyntaxError ? "Request body must be valid JSON." : error.message;
      return json({ detail }, 422, origin);
    }

    const canonical = JSON.stringify({ promptVersion: PROMPT_VERSION, ...normalized });
    const cacheKey = new Request(`https://archive-answer-cache.invalid/${await digest(canonical)}`);
    const cache = caches.default;
    const cached = await cache.match(cacheKey);
    if (cached) return json({ ...await cached.json(), cached: true }, 200, origin);
    if (!env.DEEPSEEK_API_KEY) return json({ detail: "DeepSeek is not configured." }, 503, origin);
    const budget = answerBudget(env);
    // Without the shared ledger there is no spending limit, so the paid call is refused.
    if (!budget) return json({ detail: "The shared answer budget is not configured." }, 503, origin);

    const body = deepSeekRequest(normalized.query, normalized.evidence, normalized.reasoning);
    const day = utcDay();
    const reservedUsd = maximumRequestCostUsd(body);
    const reservation = await budget.reserve(day, reservedUsd, dailyLimitUsd(env));
    if (!reservation.ok) return json({ detail: BUDGET_EXHAUSTED, budgetExhausted: true }, 429, origin);

    let provider;
    try {
      provider = await fetch("https://api.deepseek.com/chat/completions", {
        method: "POST",
        headers: { "Authorization": `Bearer ${env.DEEPSEEK_API_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = await provider.json();
      if (!provider.ok) throw new Error(String(payload?.error?.message ?? `DeepSeek returned ${provider.status}`));
      const answer = parseCompletion(payload, normalized.evidence.length, normalized.reasoning);
      // Replace the worst-case reservation with the billed cost. Failed calls keep their reservation.
      context.waitUntil(budget.settle(day, reservedUsd, answer.usage.costUsd));
      const cacheResponse = new Response(JSON.stringify(answer), {
        headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=86400" },
      });
      context.waitUntil(cache.put(cacheKey, cacheResponse));
      return json({ ...answer, cached: false }, 200, origin);
    } catch (error) {
      return json({ detail: String(error?.message ?? "DeepSeek could not answer from the archive.").slice(0, 500) }, 502, origin);
    }
  },

  async scheduled(_controller, env, context) {
    // Independent, so a failed collection dispatch still lets the staleness check raise an alert.
    context.waitUntil(dispatchCollection(env.DISPATCH_TOKEN));
    context.waitUntil(checkCollection(env.DISPATCH_TOKEN));
  },
};
