import test from "node:test";
import assert from "node:assert/strict";
import { InputError, deepSeekRequest, isPeakPricing, maximumRequestCostUsd, normalizeRequest, parseCompletion, reserveSpend, settleSpend } from "../src/core.mjs";
import { ALERT_DISPATCH_URL, COLLECTION_DISPATCH_URL, DATASET_COMMITS_URL, checkCollection, collectionState, dispatchCollection } from "../src/schedule.mjs";

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

test("DeepSeek request supports bounded high reasoning only", () => {
  const high = deepSeekRequest("What changed?", [{ ...row, id: 1 }], "high");
  assert.deepEqual(high.thinking, { type: "enabled" });
  assert.equal(high.reasoning_effort, "high");
  assert.equal(high.max_tokens, 2500);
  // The retired Deep analysis mode falls back to the fast default.
  const retired = deepSeekRequest("What changed?", [{ ...row, id: 1 }], "max");
  assert.deepEqual(retired.thinking, { type: "disabled" });
  assert.equal(retired.max_tokens, 900);
  assert.equal(normalizeRequest({ query: "What changed?", evidence: [row], reasoning: "max" }).reasoning, "off");
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
  const request = deepSeekRequest("q".repeat(400), evidence, "high");
  const reserved = maximumRequestCostUsd(request);
  const promptBytes = new TextEncoder().encode(JSON.stringify(request.messages)).length;
  const billed = parseCompletion({
    choices: [{ message: { content: JSON.stringify({ answer: "A [1].", findings: [], limitations: "" }) } }],
    usage: { prompt_tokens: promptBytes, prompt_cache_miss_tokens: promptBytes, completion_tokens: request.max_tokens },
  }, 20, "high", new Date("2026-10-28T02:00:00Z")).usage.costUsd;
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

const NOW = new Date("2026-10-29T18:07:00Z");
const capture = (date) => ({ title: "Update observations for 2026-10-29", date });
const other = (date) => ({ title: "Add semantic signal shard", date });

test("collection state finds the newest capture commit", () => {
  assert.deepEqual(
    collectionState([other("2026-10-29T18:00:00Z"), capture("2026-10-29T17:07:22Z"), capture("2026-10-29T16:07:22Z")], NOW),
    { stale: false, lastCaptureAt: "2026-10-29T17:07:22.000Z" },
  );
  assert.equal(collectionState([capture("2026-10-29T15:06:00Z")], NOW).stale, true);
  assert.equal(collectionState([capture("2026-10-29T15:08:00Z")], NOW).stale, false);
});

test("collection state bounds the capture age when no capture commit is listed", () => {
  // Every listed commit is newer than three hours, so the last capture may still be recent.
  assert.deepEqual(collectionState([other("2026-10-29T18:00:00Z"), other("2026-10-29T16:00:00Z")], NOW), { stale: false, lastCaptureAt: null });
  assert.deepEqual(collectionState([other("2026-10-29T18:00:00Z"), other("2026-10-29T12:00:00Z")], NOW), { stale: true, lastCaptureAt: null });
  assert.deepEqual(collectionState([], NOW), { stale: false, lastCaptureAt: null });
});

test("collection check dispatches the alert workflow only when captures are stale", async () => {
  const run = async (commits) => {
    const calls = [];
    const fakeFetch = async (url, init) => {
      calls.push({ url, init });
      return url === DATASET_COMMITS_URL ? Response.json(commits) : new Response(null, { status: 204 });
    };
    return { result: await checkCollection("token-value", fakeFetch, NOW), calls };
  };
  const fresh = await run([capture("2026-10-29T17:07:22Z")]);
  assert.equal(fresh.result.alerted, false);
  assert.equal(fresh.calls.length, 1);
  const stale = await run([capture("2026-10-29T13:07:22Z")]);
  assert.deepEqual(stale.result, { stale: true, lastCaptureAt: "2026-10-29T13:07:22.000Z", alerted: true });
  assert.equal(stale.calls[1].url, ALERT_DISPATCH_URL);
  assert.deepEqual(JSON.parse(stale.calls[1].init.body), { ref: "main" });
  await assert.rejects(checkCollection("token-value", async () => new Response("", { status: 503 }), NOW), /HTTP 503/);
});
