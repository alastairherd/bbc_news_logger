import assert from "node:assert/strict";
import test from "node:test";
import { createCitationRenderer } from "../src/lib/citations.ts";

const sources = new Map([
  ["1", { id: "1", title: "Housing & renters", href: "#source-1", dateLabel: "Archived 4 Nov 2026" }],
  ["2", { id: "2", title: "Policy changes", href: "#source-2" }],
  ["3", { id: "3", title: "Housing & renters", href: "#source-1" }],
  ["T1", { id: "T1", title: "Theme: housing", href: "#ontology-T1" }],
]);
const renderer = () => createCitationRenderer((id) => sources.get(id));

test("adjacent, comma-separated and ontology citations keep every supporting source", () => {
  const citations = renderer();
  const text = citations.text("Housing changed [1, 2]\n(T1). Another claim [2].");
  assert.match(text, /\[3 sources\]/);
  assert.match(text, /Another claim <a[^>]+href="#source-2"/);
  const panels = citations.panels();
  for (const href of ["#source-1", "#source-2", "#ontology-T1"]) {
    assert.ok(panels.includes(`href="${href}"`));
  }
  assert.match(panels, /Archived 4 Nov 2026/);
});

test("different claims have separate evidence groups", () => {
  const citations = renderer();
  const text = citations.text("First [1][2]. Second [2] [T1].");
  const ids = [...text.matchAll(/aria-controls="([^"]+)"/g)].map((match) => match[1]);
  assert.equal(ids.length, 2);
  assert.notEqual(ids[0], ids[1]);
  const panels = citations.panels().split('<div class="citation-panel"').slice(1);
  assert.match(panels[0], /href="#source-1"/);
  assert.doesNotMatch(panels[0], /href="#ontology-T1"/);
  assert.match(panels[1], /href="#ontology-T1"/);
});

test("duplicate destinations share an entry without losing reference numbers", () => {
  const citations = renderer();
  const text = citations.text("A claim [1][3][1].");
  assert.match(text, /\[1 source\]/);
  assert.equal((citations.panels().match(/href="#source-1"/g) ?? []).length, 1);
  assert.match(citations.panels(), /References 1, 3/);
});

test("single citations identify their article and archive date", () => {
  const citations = renderer();
  const text = citations.text("A claim [1].");
  assert.match(text, /aria-label="Source 1: Housing &amp; renters \(Archived 4 Nov 2026\)"/);
  assert.match(text, />\[1\]<\/a>/);
  assert.equal(citations.panels(), "");
});

test("unresolved references stay visible without invented links", () => {
  const citations = renderer();
  const text = citations.text("Unknown [99] [E4]; mixed [1, 99].");
  assert.ok(text.includes("Unknown [99] [E4]"));
  assert.equal((text.match(/\[99\]/g) ?? []).length, 2);
  assert.doesNotMatch(text, /href="#source-99"|href="#ontology-E4"/);
});

test("prose and source metadata cannot inject HTML into the answer", () => {
  const citations = createCitationRenderer((id) => ({ id, title: '<img src=x onerror="alert(1)">', href: '#source-" onmouseover="bad' }));
  const text = citations.text("<script>alert(1)</script> [1][2]");
  assert.ok(text.startsWith("&lt;script&gt;"));
  assert.doesNotMatch(citations.panels(), /<img|href="#source-" onmouseover/);
  assert.match(citations.panels(), /&lt;img/);
});

test("independently rendered blocks cannot share disclosure IDs", () => {
  const first = renderer().group([1, 2]);
  const second = renderer().group([1, 2]);
  assert.notEqual(first.match(/aria-controls="([^"]+)"/)[1], second.match(/aria-controls="([^"]+)"/)[1]);
});
