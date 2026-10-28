import test from "node:test";
import assert from "node:assert/strict";
import { InputError, deepSeekRequest, isPeakPricing, maximumRequestCostUsd, normalizeRequest, parseCompletion, reserveSpend, settleSpend } from "../src/core.mjs";
import { COLLECTION_DISPATCH_URL, dispatchCollection } from "../src/schedule.mjs";

const row = {
  title: "Talks resume after summit",
  url: "https://www.bbc.co.uk/news/articles/example",
  date: "2026-07-01",
  summary: "Officials resumed negotiations.",
  topic: "world",
  themes: ["diplomacy"],
  story_form: "update",
  event_type: "diplomacy",
  similarity: 0.82,
};

test("normalization accepts only bounded unique BBC evidence", () => {
  const rows = Array.from({ length: 25 }, (_, index) => ({
    ...row,
    url: `https://www.bbc.co.uk/news/articles/example-${index}`,
  }));
  const value = normalizeRequest({ query: "What changed?", evidence: rows });
  assert.equal(value.evidence.length, 20);
  assert.equal(value.evidence[0].id, 1);
  assert.equal(value.evidence.at(-1).id, 20);
  assert.equal(value.reasoning, "off");
  assert.throws(
    () => normalizeRequest({ query: "What changed?", evidence: [{ ...row, url: "https://example.com" }] }),
    InputError,
  );
});

test("DeepSeek request defaults to bounded non-thinking mode", () => {
  const body = deepSeekRequest("What changed?", [{ ...row, id: 1 }]);
  assert.equal(body.model, "deepseek-flash");
  assert.deepEqual(body.thinking, { type: "disabled" });
  assert.equal(body.max_tokens, 900);
  assert.match(body.messages[0].content, /Never turn correlation, allegation, or concern into proven causation/);
});

test("DeepSeek request supports bounded high and maximum reasoning", () => {
  const high = deepSeekRequest("What changed?", [{ ...row, id: 1 }], "high");
  assert.deepEqual(high.thinking, { type: "enabled" });
  assert.equal(high.reasoning_effort, "high");
  assert.equal(high.max_tokens, 2500);
  const maximum = deepSeekRequest("What changed?", [{ ...row, id: 1 }], "max");
  assert.equal(maximum.reasoning_effort, "max");
  assert.equal(maximum.max_tokens, 5000);
});

test("peak pricing covers weekday UTC windows only", () => {
  assert.equal(isPeakPricing(new Date("2026-10-28T00:59:00Z")), false);
  assert.equal(isPeakPricing(new Date("2026-10-28T01:00:00Z")), true);
  assert.equal(isPeakPricing(new Date("2026-10-28T04:00:00Z")), false);
  assert.equal(isPeakPricing(new Date("2026-10-28T09:59:00Z")), true);
  assert.equal(isPeakPricing(new Date("2026-10-31T02:00:00Z")), false);
});

test("completion parsing removes invented citations and calculates cost", () => {
  const completion = {
    choices: [{ message: { content: JSON.stringify({
      answer: "Talks resumed [1].",
      findings: [
        { claim: "Talks resumed.", sources: [1, 99] },
        { claim: "Unsupported.", sources: [99] },
      ],
      limitations: "One report.",
    }) } }],
    usage: { prompt_tokens: 100, prompt_cache_miss_tokens: 100, completion_tokens: 30 },
  };
  const value = parseCompletion(completion, 1, "off", new Date("2026-10-31T12:00:00Z"));
  assert.deepEqual(value.findings, [{ claim: "Talks resumed.", sources: [1] }]);
  assert.ok(Math.abs(value.usage.costUsd - 0.000033) < 1e-12);
  const peak = parseCompletion(completion, 1, "off", new Date("2026-10-28T02:00:00Z"));
  assert.ok(Math.abs(peak.usage.costUsd - 0.000066) < 1e-12);
});

test("scheduled collection dispatches the scrape workflow on main", async () => {
  const calls = [];
  const fakeFetch = async (url, init) => {
    calls.push({ url, init });
    return new Response(null, { status: 204 });
  };
  assert.equal(await dispatchCollection("token-value", fakeFetch), true);
  assert.equal(calls[0].url, COLLECTION_DISPATCH_URL);
  assert.equal(calls[0].init.method, "POST");
  assert.equal(calls[0].init.headers.Authorization, "Bearer token-value");
  assert.deepEqual(JSON.parse(calls[0].init.body), { ref: "main" });
});

test("scheduled collection reports missing tokens and GitHub errors", async () => {
  assert.equal(await dispatchCollection("", async () => assert.fail("must not call GitHub")), false);
  const denied = async () => new Response("Bad credentials", { status: 401 });
  await assert.rejects(dispatchCollection("token-value", denied), /HTTP 401: Bad credentials/);
});

test("the reserved cost bounds the billed cost of the largest request", () => {
  const evidence = Array.from({ length: 20 }, (_, index) => ({ ...row, id: index + 1, summary: "x".repeat(1200) }));
  const request = deepSeekRequest("q".repeat(400), evidence, "max");
  const reserved = maximumRequestCostUsd(request);
  const promptBytes = new TextEncoder().encode(JSON.stringify(request.messages)).length;
  const billed = parseCompletion({
    choices: [{ message: { content: JSON.stringify({ answer: "A [1].", findings: [], limitations: "" }) } }],
    usage: { prompt_tokens: promptBytes, prompt_cache_miss_tokens: promptBytes, completion_tokens: request.max_tokens },
  }, 20, "max", new Date("2026-10-28T02:00:00Z")).usage.costUsd;
  assert.ok(reserved >= billed);
  assert.ok(reserved < 0.05);
});

test("the daily ledger refuses spend past its limit and resets each UTC day", () => {
  const first = reserveSpend(undefined, { day: "2026-10-28", amountUsd: 0.3, limitUsd: 0.5 });
  assert.equal(first.ok, true);
  const second = reserveSpend(first.ledger, { day: "2026-10-28", amountUsd: 0.3, limitUsd: 0.5 });
  assert.equal(second.ok, false);
  assert.equal(second.ledger.spentUsd, 0.3);
  const settled = settleSpend(first.ledger, { day: "2026-10-28", reservedUsd: 0.3, actualUsd: 0.01 });
  assert.ok(Math.abs(settled.spentUsd - 0.01) < 1e-12);
  const nextDay = reserveSpend(settled, { day: "2026-10-29", amountUsd: 0.3, limitUsd: 0.5 });
  assert.deepEqual(nextDay, { ok: true, ledger: { day: "2026-10-29", spentUsd: 0.3 } });
  assert.equal(settleSpend(nextDay.ledger, { day: "2026-10-28", reservedUsd: 0.3, actualUsd: 0.01 }), null);
});
