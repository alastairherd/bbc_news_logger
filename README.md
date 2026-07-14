# BBC News Analyser

An independent longitudinal dataset and research interface for studying how stories, themes, and
recurring events move between the BBC News front page and Most Read list.

- **Explore:** <https://alastairherd.github.io/bbc_news_logger/>
- **Curated dataset:** <https://huggingface.co/datasets/AlastairH/bbc-news-logger>
- **Raw HTML companion:** <https://huggingface.co/datasets/AlastairH/bbc-news-logger-raw>

This project is not affiliated with or endorsed by the BBC. BBC content remains subject to its
terms and copyright.

## Architecture

```text
BBC News homepage
       │ hourly, validated
       ▼
GitHub Actions ───────────────► Hugging Face Parquet datasets
       │                                   │
       │ 3-hourly marts                    ├──► Astro static research explorer
       │                                   │
       ├── daily article + semantic refresh ─► Fenic catalog + optional MCP service
       │                                   │
       ├── checkpointed BGE backfill ───────► GitHub-hosted CPU runner
       │
       └── bounded cited synthesis ─────────► Cloudflare Worker → DeepSeek
```

The Git repository contains code, schemas, tests, and interface assets only. Data is published as
Zstandard-compressed Parquet, partitioned by UTC date. Raw article HTML is kept in a separate
dataset so normal analysis does not download the largest field.

The historical migration preserved 171,887 position observations and 18,892 article snapshots in
1,223 audited destination files. See `migration/manifest.json` in the curated dataset for source
and destination hashes, row counts, and the source commit.

## Local development

Requires Python 3.10–3.12 and [uv](https://docs.astral.sh/uv/). The dashboard requires Node 22.

```bash
uv sync --extra dev
uv run pytest -q
uv run ruff check .

cd web
npm ci
npm run dev
```

Useful pipeline commands:

```bash
# Parse and validate a live homepage response without publishing it
uv run bbc-news scrape

# Publish a validated batch (requires HF_TOKEN)
uv run bbc-news scrape --upload

# Build the JSON marts used by the static dashboard
uv run bbc-news build-marts --output web/public/data
```

## Data contract

The public dataset has three configurations:

- `observations`: one row for every story position in every successful scrape;
- `article_snapshots`: parsed metadata and text for each daily URL set;
- `scrape_runs`: validation and operational metadata for new runs.

Stable `story_id` values derive from normalized canonical URLs. Each Parquet file embeds a schema
version. Publication is an idempotent upsert on the record key, so rerunning a workflow cannot
duplicate a batch.

The dataset cards and the dashboard's Methodology page document repaired legacy fields,
reconstructed front-page position, selector risk, and interpretive limits.

## Automation

| Workflow | Trigger | Result |
| --- | --- | --- |
| Collect BBC News observations | Hourly at `:07` | Validates both surfaces and upserts observations/run metadata to Hugging Face |
| Fetch daily article snapshots | Daily at `02:17 UTC` | Fetches the previous day's distinct URLs with a global request-rate limiter |
| Refresh semantic analysis | After the daily article job | Embeds and labels only new content hashes, checkpoints results, and refreshes recurring-story clusters |
| Deploy research dashboard | Every three hours and relevant pushes | Rebuilds marts from the public dataset and deploys GitHub Pages |
| Deploy cited research Worker | Relevant Worker changes | Deploys the bounded DeepSeek synthesis endpoint to Cloudflare Workers |
| CI | Pull requests and `main` | Runs Ruff, pytest, Astro checks/build, and Fenic's API checker |

All Actions jobs use least-privilege repository permissions, locked dependencies, timeouts,
concurrency groups, and caches. They do not commit generated data back to Git.

## Fenic integration

[`services/fenic`](services/fenic/) materializes the Hugging Face tables in a persistent Fenic
catalog and exposes bounded schema, profile, search, read, and SQL-analysis tools over MCP. The
semantic enrichment command is explicit and cached; ordinary MCP exploration does not call a
language model.

The Docker service remains available for local or separately hosted MCP use. Fenic is not required
for embeddings or for the static explorer. BGE Small runs directly on a GitHub-hosted CPU runner,
which writes completed batches back to the Hugging Face dataset without rebuilding a Fenic catalog.

Semantic enrichment runs explicitly on a local machine with
`./scripts/refresh_semantics.sh`. It bills only new content hashes and writes each successful
eight-article response to a synchronous SQLite checkpoint before starting more paid work. Up to
four requests run concurrently, and completed responses are buffered into 256-row immutable
Parquet shards before upload. This keeps paid-call recovery granular without exhausting the Hugging
Face repository commit quota. A hard `$1.00` process ceiling, `$7.50` historical ceiling, and
`$1.00` monthly incremental ceiling limit spend. Ambiguous model failures are recorded without
automatic paid retries; a Hub commit-rate response waits for its quota window and retries once.
Unpublished checkpoint rows count towards the same cumulative ledger, so an expired Hub token or
failed upload cannot make a resumed run undercount prior paid work.

The Overview now leads with computed theme momentum, front-page versus Most Read skews, and
recurring stories. Signals loads BGE Small in the browser by default, searches the archive by
meaning, and exposes the underlying trends, story-form mix, and recurring-story timelines. Explore
uses the same compact int8 vector index for related coverage and paginates the story archive in
finite 15-row pages. Coverage is always visible because historical enrichment can take more than
one run.

“Ask the archive” follows the same retrieval-then-synthesis pattern as the Fenic HN agent example.
BGE retrieves the strongest matches locally in the browser, then a lightweight Cloudflare Worker
sends at most ten validated BBC evidence rows to DeepSeek V4 Flash. Answers and findings cite the
numbered results. Fast mode disables thinking; optional Reasoned and Deep analysis modes enable
bounded [DeepSeek thinking](https://api-docs.deepseek.com/guides/thinking_mode). Private reasoning is never stored or exposed. The Worker keeps the key
encrypted, caches answers separately by reasoning depth, rate-limits clients, and bounds input and
output. Source discovery still works if it is unavailable or over budget.

Cloudflare's free Worker tier is a better fit than a hosted Python process because network wait does
not count as CPU time and the service performs only one small external request. The durable spend
boundary remains the limited DeepSeek account/key. An explicit bring-your-own-key fallback keeps a
limited key in session storage for the current tab and sends it directly to DeepSeek, never GitHub.
Hugging Face currently permits only Static Spaces on the free account used here, so no dormant
Gradio deployment is retained.

Worker deployment uses the `CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID` GitHub secrets. Pages
reads its endpoint from the `PUBLIC_RESEARCH_API_URL` repository variable; for this repository it is
`https://bbc-news-archive-research.alastairherd.workers.dev/api/research`.

## Semantic backfill

The Raspberry Pi does not run the embedding model. Start `Refresh semantic analysis` manually
with `limit` set to `0` and `run_deepseek` disabled. The public repository's GitHub-hosted CPU
runner processes the full BGE backlog for free and uploads each 256-vector Parquet checkpoint.
If a run reaches its time limit, starting it again discovers the uploaded hashes and resumes.

For a paid-label backfill from this machine, each invocation processes up to the requested number
of missing article versions and stops before its budget boundary:

```bash
./scripts/refresh_semantics.sh 1000
```

Use `--monthly` for the regular monthly ledger, or `--local-only` to retain checkpoints and shards
without publishing them.

## Repository layout

```text
src/bbc_news_logger/   collector, schemas, publication, migration, marts
tests/                 parser, storage, migration, and mart contract tests
web/                   static Astro research interface
services/fenic/        optional Fenic catalog, enrichment, MCP service, Dockerfile
workers/research/      Cloudflare Worker for bounded cited DeepSeek synthesis
datasets/              Hugging Face dataset cards
.github/workflows/     collection, publication, CI, and Pages deployment
```
