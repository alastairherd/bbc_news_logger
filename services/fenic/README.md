# Fenic research sidecar

This optional service turns the public Hugging Face dataset into inspectable Fenic catalog tables
and exposes Fenic's bounded system tools over streamable HTTP MCP at `/mcp`.

It is deliberately separate from the static dashboard. If this service is stopped, data collection,
the public dataset, and the GitHub Pages explorer continue working.

## Run locally

```bash
uv sync --extra semantic
uv run --extra semantic python services/fenic/bootstrap.py
uv run --extra semantic uvicorn services.fenic.serve:app --host 0.0.0.0 --port 7860
```

The public dataset does not require a Hugging Face token. Set `HF_TOKEN` to avoid anonymous API
limits. The service's ordinary search, profile, read, and SQL analysis tools do not call a language
model.

Semantic enrichment is an explicit batch operation:

```bash
export OPENROUTER_API_KEY=...
uv run --extra semantic python services/fenic/enrich.py --limit 25
```

It uses Fenic's `semantic.map` followed by deterministic typed field extraction and defaults to the free
`qwen/qwen3-next-80b-a3b-instruct:free` route. Override with `OPENROUTER_MODEL`. Results are cached by
Fenic and saved as the `story_signals` catalog table, which is exposed automatically on the next
service start.

Fenic 0.10 constructs its OpenRouter client through OpenAI SDK 2.45, which also insists on an
`OPENAI_API_KEY`. The bootstrap mirrors `OPENROUTER_API_KEY` into that variable in-process; requests
still use Fenic's OpenRouter base URL and authorization header.

Fenic 0.10 also sends `max_completion_tokens`, which current OpenRouter endpoints reject in favour
of `max_tokens`. The bootstrap suppresses that one request field in-process while this exact Fenic
version is pinned. The enrichment command preflights the OpenRouter key and exits with a clear error
when its configured total limit is exhausted.

## Deployment

The Docker image listens on port `7860` and persists its catalog beneath `/data`. A public service
should set `FENIC_DB_PATH=/data` and mount durable storage there.

Hugging Face no longer offers a free CPU runtime for Docker Spaces, so the repository does not
automatically deploy this image. Build and run it on any Docker host, or create the intended
`AlastairH/bbc-news-research-lab` Space after enabling a paid Hugging Face runtime.
