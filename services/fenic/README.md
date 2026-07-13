# Fenic research sidecar

This optional service turns the public Hugging Face dataset into inspectable Fenic catalog tables
and exposes Fenic's bounded system tools over streamable HTTP MCP at `/mcp`.

It is deliberately separate from the static dashboard. If this service is stopped, data collection,
the public dataset, and the GitHub Pages explorer continue working.

## Run locally

```bash
uv sync --extra semantic
uv run --extra semantic python -m services.fenic.bootstrap
uv run --extra semantic uvicorn services.fenic.serve:app --host 0.0.0.0 --port 7860
```

The public dataset does not require a Hugging Face token. Set `HF_TOKEN` to avoid anonymous API
limits. The service's ordinary search, profile, read, and SQL analysis tools do not call a language
model.

Pass `--tables article_snapshots story_signals` to bootstrap only those catalog tables for local
analysis. Semantic refreshes do not need to bootstrap a persistent catalog: they read the required
Hugging Face Parquet directly through Fenic and persist only the signal table.

Semantic enrichment is an explicit local batch operation. The wrapper accepts a maximum number of
article versions and defaults to 200:

```bash
./scripts/refresh_semantics.sh 200
```

The wrapper reads the ignored `CREDS.txt` when present and accepts either `DEEPSEEK_API_KEY` or the
existing `DEEPSEEK_API` name. Authenticate this machine with `hf auth login`, or export `HF_TOKEN`,
before publishing. Use `./scripts/refresh_semantics.sh 200 --local-only` to build and inspect output
under `dist/` without uploading it.

It calls DeepSeek's native OpenAI-compatible API with `deepseek-v4-flash`, validates the JSON, and
then stores the typed result in Fenic. This boundary is intentional because Fenic 0.10 does not have
a first-class DeepSeek model provider. Fenic still owns the catalog, Parquet output, SQL analysis,
and MCP tools.

Each result includes topic, reusable themes, summary, entities, event label/type, and story form.
Thinking is disabled, each article input is capped at 32,000 UTF-8 bytes, and up to eight articles
are sent in one request. Four requests may run concurrently. Content hashes avoid repeat billing.

The process calculates cost from DeepSeek's returned token counters and writes a run manifest to
`dist/semantic-run.json`. Each successful response is committed to a synchronous SQLite WAL before
the next wave starts and is uploaded as an immutable Parquet shard when publishing is enabled.
The budget defaults to `$1.00` and the code rejects any higher process value. Persistent dataset
rows enforce the separate `$7.50` backfill and `$1.00` monthly ledgers.

## Deployment

The Docker image listens on port `7860` and persists its catalog beneath `/data`. A public service
should set `FENIC_DB_PATH=/data` and mount durable storage there.

This Docker image is not used by the free Hugging Face deployment. Build and run it locally or on
a separate Docker host when an HTTP MCP service is needed. The historical BGE backfill uses the
standard Gradio application under `spaces/bge-worker`, which fits the available free CPU runtime.
