import test from "node:test";
import assert from "node:assert/strict";
import { InputError, deepSeekRequest, normalizeRequest, parseCompletion } from "../src/core.mjs";

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
  const value = normalizeRequest({ query: "What changed?", evidence: Array(20).fill(row) });
  assert.equal(value.evidence.length, 1);
  assert.equal(value.evidence[0].id, 1);
  assert.throws(
    () => normalizeRequest({ query: "What changed?", evidence: [{ ...row, url: "https://example.com" }] }),
    InputError,
  );
});

test("DeepSeek request is JSON, non-thinking, and output-bounded", () => {
  const body = deepSeekRequest("What changed?", [{ ...row, id: 1 }]);
  assert.equal(body.model, "deepseek-v4-flash");
  assert.deepEqual(body.thinking, { type: "disabled" });
  assert.equal(body.max_tokens, 900);
});

test("completion parsing removes invented citations and calculates cost", () => {
  const value = parseCompletion({
    choices: [{ message: { content: JSON.stringify({
      answer: "Talks resumed [1].",
      findings: [
        { claim: "Talks resumed.", sources: [1, 99] },
        { claim: "Unsupported.", sources: [99] },
      ],
      limitations: "One report.",
    }) } }],
    usage: { prompt_tokens: 100, prompt_cache_miss_tokens: 100, completion_tokens: 30 },
  }, 1);
  assert.deepEqual(value.findings, [{ claim: "Talks resumed.", sources: [1] }]);
  assert.ok(Math.abs(value.usage.costUsd - 0.0000224) < 1e-12);
});
