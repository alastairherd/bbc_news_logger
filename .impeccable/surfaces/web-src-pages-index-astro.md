---
version: 1
slug: "web-src-pages-index-astro"
primary_target: "web/src/pages/index.astro"
related_targets: ["web/src/layouts/Layout.astro","web/src/styles/global.css"]
---

# Overview surface brief

Scope: shared shell (header, section navigation, footer) and the Overview page. Mode: Operate.
Audience: researchers and journalists returning to compare what BBC News promotes with what readers
open. Job: within seconds, see the latest capture's lead stories, Most Read top 10, overlap, and
movement, then reach deeper analysis. Data: rank-series.json, stories.json, semantic-findings.json,
daily.json, manifest.json; no new marts. Decisions: headlines link to the BBC article (external);
both surfaces come from the same latest scrape. No archive-record link until Explore supports
per-story deep links.

## Direction contract

THESIS: The archive's latest capture is set as the front page it recorded, annotated with the
archive's own measurements; it refuses the dashboard default of metric cards over a chart.

OWN-WORLD: White canvas, #141414 ink, #DDDDDD rules, BBC News red #B80000 for ranks, active
section and new markers, comparison blue #006EB8 for cross-surface markers. Inter, bold tight
headlines, tabular figures. Text wordmark, no letter-in-square mark, no red header band.

STORY: The visitor sees what leads and what is read, notices overlap and movement, trusts the
capture time and collection status, then follows a theme, surface gap, or returning story.

FIRST VIEWPORT: White header with wordmark and independence label; section bar with red underline;
slim status bar; "Latest capture" heading with UTC time. Left two-thirds: lead story, four-up grid,
compact rows six to ten. Right third: Most Read with large red numerals.

FORM: Pinned by the user and DESIGN.md: BBC News front-page reading patterns. seed: none (roll skipped: direction pinned by the user)

FINISH: unreviewed and undocumented is unfinished; this build ends with the finish review, the verdict, DESIGN.md, and every shipping raster carrying its provenance
