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
| **Methodology** | Collection rules, schemas, validation, semantic methods, known gaps, and interpretive limits |

### Ask the archive

“Ask the archive” retrieves up to twenty relevant articles with BGE Small and can ask DeepSeek V4
Flash to produce a cited synthesis. Every generated finding links back to numbered BBC evidence so
the source material remains inspectable.

- Search queries and embedding comparisons run locally in the browser.
- The embedding model and index load only after a semantic search begins.
- The shared synthesis path sends the question and bounded evidence rows to a rate-limited
  Cloudflare Worker; it does not send the full archive.
- A visitor-supplied DeepSeek key is kept in session storage for that browser tab and sent directly
  to DeepSeek instead of this project.
- Fast, Reasoned, and Deep analysis modes control the bounded DeepSeek thinking allowance.
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

## Dataset

The main public dataset exposes three core configurations:

| Configuration | Grain |
| --- | --- |
| `observations` | One row for each captured story position in each successful scrape |
| `article_snapshots` | Parsed metadata and text for each daily article URL set |
| `scrape_runs` | Validation and operational metadata for each collection attempt |

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
| Collect BBC News observations | Hourly at `:07` | Validates both surfaces and publishes observations and run metadata |
| Fetch daily article snapshots | Daily at `02:17 UTC` | Fetches the previous day's distinct URLs with a global rate limit |
| Refresh semantic analysis | After article collection | Embeds and labels new content hashes, checkpoints progress, and refreshes clusters |
| Compact the dataset | Weekly | Atomically folds incremental Parquet shards into compact bases |
| Deploy the analyser | Every three hours and relevant pushes | Rebuilds public marts and deploys GitHub Pages |
| Deploy the research Worker | Relevant Worker changes | Publishes the bounded DeepSeek synthesis endpoint |
| CI | Pull requests and `main` | Runs Ruff, pytest, Astro checks, Worker tests, and Fenic API checks |

Actions use least-privilege permissions, locked dependencies, timeouts, concurrency controls, and
caches. Generated data is never committed back to this repository.

## Fenic and MCP

[`services/fenic`](services/fenic/) is an optional research interface over the same Hugging Face
tables. It materializes a persistent [Fenic](https://github.com/typedef-ai/fenic) catalog and
exposes bounded schema, profile, search, read, and SQL-analysis tools over MCP.

Fenic is not required for the public website, embeddings, or semantic search. The service is
available for local Docker use or deployment to a separate Python host.

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

The `Refresh semantic analysis` workflow can run the BGE backlog on a GitHub-hosted CPU runner.
Uploaded 256-vector checkpoints make timed-out runs resumable without repeating completed work.

### Deployment configuration

- Dataset publication uses the `HF_TOKEN` GitHub secret.
- Worker deployment uses `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`.
- Pages reads the synthesis endpoint from the `PUBLIC_RESEARCH_API_URL` repository variable.

</details>

## Repository layout

```text
src/bbc_news_logger/   collector, schemas, publication, migration, and marts
tests/                 parser, storage, migration, and mart contract tests
web/                   Astro research interface and browser workers
services/fenic/        optional Fenic catalog and MCP service
workers/research/      bounded cited-synthesis Cloudflare Worker
datasets/              Hugging Face dataset cards
.github/workflows/     collection, publication, CI, and deployment
```

## References

The low-cost data-app architecture was informed by Spicy Data's
[“A live data app for $0: DuckDB, Astro, and no BI tool”](https://spicydata.ai/blog/zero-dollar-data-app/).
The retrieval-and-synthesis design also draws on the
[Fenic Hacker News agent example](https://github.com/typedef-ai/fenic-examples/tree/main/hn_agent).
