---
version: 1
slug: "web-src-pages-explore-astro"
primary_target: "web/src/pages/explore.astro"
related_targets: ["web/src/lib/titles.ts"]
---

# Explore surface brief

Scope: Explore page inside the established shell. Mode: Operate. Audience: researchers and
journalists searching the archive for a story and its capture history. Job: find stories by
headline, filter by list, sort, open one, and share a link to it. Data: stories.json,
rank-series.json, semantic index via the existing worker. Decisions: story, filters, sort and page
live in the URL (explore/?story=<id>); the Overview links each story's archive record here.

## Direction contract

THESIS: A headline-led archive index beside a story record, set in the Overview's news grammar;
it refuses the dashboard default of a data table beside a dark detail card.

OWN-WORLD: White canvas, #141414 ink, #DDDDDD rules, BBC red for links, LIVE labels and Most Read
tags, ink for Front page tags. Inter, bold headlines, tabular figures, 4px controls with a 3:1
border. No cards, pills or dark panels.

STORY: The visitor searches, scans headline rows with list, best position, capture count and
recency, opens a record, reads its facts, summary, positions and related coverage, and copies a
link.

FIRST VIEWPORT: Page title and one-line description; sticky filter bar (search, list, sort, count);
left three-fifths story rows; right two-fifths sticky story record with title, BBC link, copy link,
facts.

FORM: Extension of the established Overview world. seed: none (roll skipped: direction pinned by the user)
Pin, in the user's words: "I'm looking at a visual overhaul in line with the design pattern that
exists on the standard https://www.bbc.co.uk/news website" and "I would like to use the BBC red,
and that's that".

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
