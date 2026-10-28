# Fenic services

This folder holds the parts of the project that run on [Fenic](https://github.com/typedef-ai/fenic):

- **Weekly theme summaries** (`weekly.py`), which use Fenic's `semantic.map` to write cited
  summaries of each week's most-covered themes.
- **The MCP research service** (`bootstrap.py`, `serve.py`), which turns the public Hugging Face
  dataset into Fenic catalog tables and exposes Fenic's bounded system tools over streamable HTTP
  MCP at `/mcp`.

The daily embedding, labelling, and clustering jobs do not use Fenic and live in
`src/bbc_news_logger/jobs/`.

## Weekly theme summaries

Every Monday the `Write weekly theme summaries` workflow takes the seven complete UTC days before
the run, groups that week's labelled articles by theme, and keeps up to eight themes with at least
three articles. Each theme's evidence is the newest 30 articles, numbered, with their headline and
existing one-sentence label summary. Fenic sends one prompt per theme and asks for two or three
neutral sentences that cite the numbered articles; citations outside the evidence are removed.

```bash
# Report the themes and the cost bound without calling a model
uv run --extra semantic python -m services.fenic.weekly --dry-run

# Write and publish summaries (requires OPENROUTER_API_KEY and HF_TOKEN)
uv run --extra semantic python -m services.fenic.weekly --publish
```

Fenic 0.13 has no DeepSeek provider of its own, so the job reaches DeepSeek V4.1 Flash
(`deepseek/deepseek-v4.1-flash-1017`) through Fenic's OpenRouter provider. Before any request the
job computes a worst-case cost from UTF-8 byte counts and capped output, at double OpenRouter's
listed price. It refuses to run above `$0.10` per run or `$0.25` per calendar month, counting the
cost Fenic reports for every published summary row. Results are published to `semantic/weekly/`
and appear in the This week section of the Trends page.

## MCP research service

It is deliberately separate from the static site. If this service is stopped, data collection,
the public dataset, and the GitHub Pages site continue working.

```bash
uv sync --extra semantic
uv run --extra semantic python -m services.fenic.bootstrap
uv run --extra semantic uvicorn services.fenic.serve:app --host 0.0.0.0 --port 7860
```

The public dataset does not require a Hugging Face token. Set `HF_TOKEN` to avoid anonymous API
limits. The service's search, profile, read, and SQL analysis tools do not call a language model.
Pass `--tables article_snapshots story_signals` to bootstrap only those catalog tables.

The Docker image listens on port `7860` and persists its catalog beneath `/data`. A public service
should set `FENIC_DB_PATH=/data` and mount durable storage there.
