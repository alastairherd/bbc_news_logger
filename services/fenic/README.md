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

Semantic enrichment is an explicit batch operation:

```bash
export DEEPSEEK_API_KEY=...
uv run --extra semantic python -m services.fenic.enrich --limit 25 --max-cost-usd 1.00
```

For GitHub Actions, store the same credential in the repository secret
`DEEPSEEK_API_KEY`. Do not put it in the workflow or commit it to the repository.

It calls DeepSeek's native OpenAI-compatible API with `deepseek-v4-flash`, validates the JSON, and
then stores the typed result in Fenic. This boundary is intentional because Fenic 0.10 does not have
a first-class DeepSeek model provider. Fenic still owns the catalog, Parquet output, SQL analysis,
and MCP tools.

Each result includes topic, reusable themes, summary, entities, event label/type, and story form.
Thinking is disabled, input is capped at 32,000 UTF-8 bytes, output is capped at 256 tokens, and
requests run sequentially without automatic retries. Content hashes avoid repeat billing.

The process calculates cost from DeepSeek's returned token counters and writes a run manifest to
`dist/semantic-run.json`. The budget defaults to `$1.00` and the code rejects any higher value, even
if a workflow or environment variable tries to raise it. It reserves a conservative worst-case
amount before each request and stops before the next request could cross the remaining budget.

## Deployment

The Docker image listens on port `7860` and persists its catalog beneath `/data`. A public service
should set `FENIC_DB_PATH=/data` and mount durable storage there.

Hugging Face no longer offers a free CPU runtime for Docker Spaces, so the repository does not
automatically deploy this image. Build and run it on any Docker host, or create the intended
`AlastairH/bbc-news-research-lab` Space after enabling a paid Hugging Face runtime.
