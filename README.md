# BBC News Analyser

An independent, continuously updated archive for exploring how stories move across the BBC News
front page and Most Read list.

[Open the analyser](https://alastairherd.github.io/bbc_news_logger/) ·
[Browse the dataset](https://huggingface.co/datasets/AlastairH/bbc-news-logger) ·
[Read the methodology](https://alastairherd.github.io/bbc_news_logger/methodology/)

The project records selected BBC News homepage positions every hour, fetches article snapshots,
and adds semantic labels and embeddings. The public interface turns that history into explorable
trends, recurring-story timelines, related coverage, and evidence-linked answers.

This project is not affiliated with or endorsed by the BBC. BBC content remains subject to the
BBC's terms and copyright.

## What you can explore

| View | What it shows |
| --- | --- |
| **Overview** | Recent theme momentum, differences between the front page and Most Read, story-form mix, collection health, and returning stories |
| **Explore** | A filterable, paginated story archive with article links and semantically related coverage |
| **Ask & analyse** | Local semantic search, optional cited synthesis, 120-day signal trends, and recurring-story timelines |
| **Promotion vs attention** | Which promoted stories reach Most Read, which are read without promotion, the delay between the two, by theme and by month |
| **Methodology** | Collection rules, schemas, validation, semantic methods, known gaps, and interpretive limits |

### Ask the archive

“Ask the archive” retrieves up to twenty relevant articles with BGE Small and can ask DeepSeek V4.1
Flash to produce a cited synthesis. Every generated finding links back to numbered BBC evidence so
the source material remains inspectable.

- Search queries and embedding comparisons run locally in the browser.
- The embedding model and index load only after a semantic search begins.
- The shared synthesis path sends the question and bounded evidence rows to a rate-limited
  Cloudflare Worker; it does not send the full archive. The Worker enforces a shared daily
  spending cap (`DAILY_BUDGET_USD`, $0.50 per UTC day by default) by reserving each request's
  worst-case cost in a Durable Object before calling DeepSeek and settling the billed cost after.
- Fast and Reasoned modes control the bounded DeepSeek thinking allowance.
- Source search, trends, and recurring-story exploration remain available if synthesis is offline.

Generated labels, clusters, and answers are discovery aids. They can be incomplete or wrong and
should not be treated as verified claims about the BBC or the events being reported.

## How it works

```text
BBC News homepage
        │ hourly collection and validation
        ▼
GitHub Actions ───────────────► Hugging Face Parquet datasets
        │                                      │
        │ DuckDB marts                         ├──► Astro research interface
        │                                      │
        ├── article snapshots                  ├──► BGE embeddings
        ├── semantic enrichment                └──► labels and story clusters
        │
        └── cited synthesis request ──────────────► Cloudflare Worker → DeepSeek
```

The repository contains application code, schemas, tests, and interface assets. Generated data is
published to Hugging Face rather than committed to Git:

- [Curated dataset](https://huggingface.co/datasets/AlastairH/bbc-news-logger): observations,
  parsed article snapshots, run metadata, embeddings, labels, and clusters.
- [Raw HTML companion](https://huggingface.co/datasets/AlastairH/bbc-news-logger-raw): source HTML
  kept separately so ordinary analysis does not download the largest field.

Append-only Parquet shards make routine updates small. A weekly compare-and-swap compaction folds
older shards into compressed base files without racing new collection writes. Dashboard and Fenic
readers see the base and incremental layers as the same logical tables.

## How it was made

BBC News Analyser was built as a sequence of small, auditable systems rather than as one large
application. Each new capability had to preserve the data already collected and remain cheap to
run unattended.

1. **Stabilise collection first.** The collector was made fail-closed, with canonical story IDs,
   typed records, selector versions, and explicit run metadata. A broken page selector creates a
   visible failed run instead of silently publishing an empty or partial dataset.
2. **Treat history as a migration.** Legacy CSV, ZIP, and Parquet files were converted through a
   reproducible migration with source and destination hashes, row counts, and documented repairs.
   The original files remained untouched, making the new dataset auditable rather than merely
   tidier.
3. **Make expensive work incremental.** Article fetches, embeddings, and labels are keyed by stable
   URLs or content hashes. Paid model responses are checkpointed before the next request, and
   immutable output shards are published in bounded batches. A failed or timed-out run resumes from
   completed work instead of paying for it again.
4. **Put each job on the smallest suitable compute.** GitHub Actions handles scheduled Python and
   model work, Hugging Face stores the versioned Parquet data, DuckDB builds browser-ready marts,
   GitHub Pages serves the static product, and Cloudflare performs the short optional synthesis
   request. Trend parsing and semantic comparisons run in browser workers so the interface remains
   responsive without a permanent application server.
5. **Ground the generative layer in evidence.** Retrieval happens before synthesis. DeepSeek sees a
   bounded set of numbered articles, returns structured output, and must cite those sources. The
   interface keeps retrieval results visible so a generated answer can always be checked against
   the archive.

The development process followed the same evidence-first pattern. I set the research goals,
interpretive limits, and cost boundaries, then used an AI coding agent to inspect the repository
and live outputs, implement scoped changes, run tests and workflows, and measure the deployed
result. I reviewed each iteration against real data and real browser behaviour before starting the
next one.

That measurement loop mattered. For example, profiling a frozen trends interface showed that it
was creating 26,521 theme options and repeatedly scanning 70,685 rows on the browser thread. The
fix was not more infrastructure: the mart was indexed in a Web Worker, the interface was limited to
the most active signals, and interactions were measured again on the deployed site. This cycle of
observe → constrain → implement → verify is the process behind the project as much as any single
technology in the architecture.

## Dataset

The main public dataset exposes six configurations:

| Configuration | Grain |
| --- | --- |
| `observations` | One row for each captured story position in each successful scrape |
| `article_snapshots` | Parsed metadata and text for each daily article URL set |
| `scrape_runs` | Validation and operational metadata for each collection attempt |
| `story_signals` | Versioned DeepSeek labels and summaries for each analysed content hash |
| `article_embeddings` | Normalized 384-dimensional BGE Small vectors for semantic retrieval |
| `recurring_events` | Reproducible recurring-story membership derived from vectors and structured evidence |

Stable `story_id` values come from normalized canonical URLs. Every Parquet file carries a schema
version, and publication uses idempotent record keys so rerunning a workflow does not duplicate a
batch. The dataset cards document the complete schemas and the audited historical migration.

### What the numbers mean

- A front-page position is not a direct measure of editorial importance.
- A Most Read rank is not an audience count.
- Counts in the analyser describe captured positions or fetched article versions, not readership.
- Collection covers selected regions of one BBC News web page, not apps, regional variants,
  personalization, or every BBC property.
- Scheduled jobs and page-structure changes can create visible gaps. Failed validation publishes no
  partial observation batch.
- Collection was switched off from 17 September 2026 (last capture 11:33 UTC) until 27 October 2026
  (resumed 20:02 UTC). That period has no observations, article snapshots, or semantic labels, and
  it cannot be backfilled.
- From 27 August 2026 until 28 October 2026, GitHub's scheduler ran the hourly collection only
  every three to six hours, giving about 2–7 captures a day instead of 24. Counts from that period
  are not comparable with hourly periods without normalizing by the number of successful scrapes.

See the [versioned methodology](https://alastairherd.github.io/bbc_news_logger/methodology/) for the
full data contract, migration repairs, clustering thresholds, and limitations.

## Run locally

Requirements:

- Python 3.10–3.12
- [uv](https://docs.astral.sh/uv/)
- Node.js 22

```bash
git clone https://github.com/alastairherd/bbc_news_logger.git
cd bbc_news_logger

uv sync --extra dev
uv run pytest -q
uv run ruff check .

# Build the dashboard data from the public Hugging Face dataset
uv run bbc-news build-marts --output web/public/data

cd web
npm ci
npm run dev
```

The local Astro server prints the URL to open. The optional cited-answer service is not required for
the rest of the interface.

### Useful commands

```bash
# Parse and validate a live homepage response without publishing
uv run bbc-news scrape

# Publish a validated batch (requires HF_TOKEN)
uv run bbc-news scrape --upload

# Rebuild the static dashboard marts
uv run bbc-news build-marts --output web/public/data

# Compact append-only Hugging Face shards
uv run bbc-news compact-dataset --publish
```

## Automation

| Workflow | Schedule or trigger | Result |
| --- | --- | --- |
| Collect BBC News observations | Hourly at `:07`, dispatched by a Cloudflare cron | Validates both surfaces and publishes observations and run metadata |
| Fetch daily article snapshots | Daily at `02:17 UTC` | Fetches the previous day's distinct URLs with a global rate limit |
| Refresh semantic analysis | Daily at `12:23 UTC` | Embeds and labels new content hashes in DeepSeek's off-peak window, checkpoints progress, and refreshes clusters |
| Write weekly theme summaries | Mondays at `12:47 UTC` | Fenic writes cited summaries of the week's most-covered themes |
| Alert on stalled collection | Dispatched by the Worker after 3 hours without a capture; every 4 hours as a backstop | Opens a `collection-alert` issue, which the next successful capture closes |
| Compact the dataset | Weekly | Atomically folds incremental Parquet shards into compact bases |
| Deploy the analyser | Every three hours and relevant pushes | Rebuilds public marts and deploys GitHub Pages |
| Deploy the research Worker | Relevant Worker changes | Publishes the bounded DeepSeek synthesis endpoint |
| CI | Pull requests and `main` | Runs Ruff, pytest, Astro checks, Worker tests, and Fenic API checks |

Actions use least-privilege permissions, locked dependencies, timeouts, concurrency controls, and
caches. Generated data is never committed back to this repository.

A monthly **Keep scheduled workflows active** workflow records a UTC timestamp in
`.github/maintenance/last-keepalive.txt` on the first day of each month. This maintenance commit
prevents GitHub's automatic disabling of public-repository schedules after 60 days without
repository activity. It uses the built-in `GITHUB_TOKEN` with `contents: write`, needs no additional
secrets, and can also be run manually. The token's commits do not trigger other push workflows.
If branch protection later blocks direct bot pushes, this safeguard will need updating. It does
not backfill missed observations or protect against GitHub outages or workflow failures.

## Fenic

[`services/fenic`](services/fenic/) holds the weekly theme summaries, which run on
[Fenic](https://github.com/typedef-ai/fenic). Each Monday, Fenic's `semantic.map` writes two or
three cited sentences for each of the week's most-covered themes, routed to DeepSeek V4.1 Flash
through Fenic's OpenRouter provider and capped at $0.10 per run and $0.25 per month. They appear in
the This week section of Trends.

The daily embedding, labelling, and clustering jobs do not use Fenic; they live in
`src/bbc_news_logger/jobs/`.

<details>
<summary><strong>Maintainer operations</strong></summary>

### Semantic enrichment

`./scripts/refresh_semantics.sh` processes only new content hashes. Each successful paid response
is written synchronously to a SQLite checkpoint before more work starts, then buffered into
content-addressed Parquet shards for publication.

```bash
# Process up to 1,000 missing article versions
./scripts/refresh_semantics.sh 1000

# Use the regular monthly ledger
./scripts/refresh_semantics.sh 1000 --monthly

# Keep checkpoints and shards locally
./scripts/refresh_semantics.sh 1000 --local-only
```

Paid labelling enforces a `$1.00` process ceiling, `$7.50` historical ceiling, and `$1.00` monthly
incremental ceiling. Ambiguous failures are recorded without automatic paid retries. Unpublished
checkpoint rows remain part of the cumulative spend ledger.

DeepSeek bills double on weekdays from 01:00 to 04:00 and 06:00 to 10:00 UTC. Labelling defers any
remaining work when a peak window starts, unless `--allow-peak` is passed to the script or selected
when running the workflow manually.

The `Refresh semantic analysis` workflow can run the BGE backlog on a GitHub-hosted CPU runner.
Uploaded 256-vector checkpoints make timed-out runs resumable without repeating completed work.

### Deployment configuration

- Dataset publication uses the `HF_TOKEN` GitHub secret.
- Worker deployment uses `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`.
- Hourly collection is started by the research Worker's cron trigger, because GitHub drops many
  scheduled runs. The Worker receives `DISPATCH_TOKEN`, a GitHub token that can dispatch Actions
  workflows in this repository. The GitHub schedule remains as a fallback and skips itself when a
  dispatch ran in the previous 50 minutes.
- The same cron checks the dataset's commit log for the latest `Update observations for …` commit.
  After three hours without one it dispatches `collection_alert.yml`, which opens the alert issue
  with the built-in `GITHUB_TOKEN`, so `DISPATCH_TOKEN` needs no issue permissions.
- Pages reads the synthesis endpoint from the `PUBLIC_RESEARCH_API_URL` repository variable.

</details>

## Repository layout

```text
src/bbc_news_logger/   collector, schemas, publication, migration, marts, and scheduled jobs
tests/                 parser, storage, migration, and mart contract tests
web/                   Astro research interface and browser workers
services/fenic/        Fenic weekly theme summaries
workers/research/      bounded cited-synthesis Cloudflare Worker
datasets/              Hugging Face dataset cards
.github/workflows/     collection, publication, CI, and deployment
```

## References

The low-cost data-app architecture was informed by Spicy Data's
[“A live data app for $0: DuckDB, Astro, and no BI tool”](https://spicydata.ai/blog/zero-dollar-data-app/).
The retrieval-and-synthesis design also draws on the
[Fenic Hacker News agent example](https://github.com/typedef-ai/fenic-examples/tree/main/hn_agent).
