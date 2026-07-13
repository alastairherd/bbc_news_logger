# BBC News Surface Lab

An independent longitudinal dataset and research interface for studying how stories move between
the BBC News front page and Most Read list.

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
       └── daily article fetch             └──► Fenic catalog + optional MCP service
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
| Deploy research dashboard | Every three hours and relevant pushes | Rebuilds marts from the public dataset and deploys GitHub Pages |
| CI | Pull requests and `main` | Runs Ruff, pytest, Astro checks/build, and Fenic's API checker |

All Actions jobs use least-privilege repository permissions, locked dependencies, timeouts,
concurrency groups, and caches. They do not commit generated data back to Git.

## Fenic integration

[`services/fenic`](services/fenic/) materializes the Hugging Face tables in a persistent Fenic
catalog and exposes bounded schema, profile, search, read, and SQL-analysis tools over MCP. The
semantic enrichment command is explicit and cached; ordinary MCP exploration does not call a
language model.

The Docker service is deployment-ready but not automatically hosted. Hugging Face currently
requires a paid runtime for Docker Spaces. The static explorer remains fully functional without
the sidecar. See the service README for local and Docker instructions.

Semantic enrichment runs explicitly on a local machine with
`./scripts/refresh_semantics.sh`. It reads the current semantic table from Hugging Face, bills only
new snapshots or changed content, and uploads the updated table and run manifest back to the
dataset. A hard `$1.00` per-run ceiling, bounded inputs and outputs, sequential requests, and
content-hash caching limit spend. Ordinary exploration never invokes the model, and generated
Parquet is not committed to Git.

## Repository layout

```text
src/bbc_news_logger/   collector, schemas, publication, migration, marts
tests/                 parser, storage, migration, and mart contract tests
web/                   static Astro research interface
services/fenic/        optional Fenic catalog, enrichment, MCP service, Dockerfile
datasets/              Hugging Face dataset cards
.github/workflows/     collection, publication, CI, and Pages deployment
```
