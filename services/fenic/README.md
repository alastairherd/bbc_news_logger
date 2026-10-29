# Fenic services

This folder holds the part of the project that runs on [Fenic](https://github.com/typedef-ai/fenic):
the weekly theme summaries (`weekly.py`), which use Fenic's `semantic.map` to write cited
summaries of each week's most-covered themes.

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
(`deepseek/deepseek-v4.1-flash-1017`) through Fenic's OpenRouter provider. One local workaround is
needed: that provider builds its OpenAI SDK client without an `api_key`, so the job sets
`OPENAI_API_KEY` to the OpenRouter key when it is unset. Requests still authenticate with the
OpenRouter header. Before any request the
job computes a worst-case cost from UTF-8 byte counts and capped output, at double OpenRouter's
listed price. It refuses to run above `$0.10` per run or `$0.25` per calendar month, counting the
cost Fenic reports for every published summary row. Results are published to `semantic/weekly/`
and appear in the This week section of the Trends page.
