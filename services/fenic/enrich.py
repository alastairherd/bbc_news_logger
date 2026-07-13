"""Create a typed, cached semantic signal table for recent article snapshots."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import fenic as fc
import requests
from huggingface_hub import HfApi

from services.fenic.bootstrap import create_session

SIGNAL_PROMPT = """Read the news article below and return exactly three lines with no preamble.
topic=<one of politics|world|business|science_and_environment|technology|health|culture|sport|other>
summary=<one neutral sentence grounded only in the article>
entities=<up to eight important people, organizations, or places separated by semicolons>

Article:
{{ article }}"""


def validate_openrouter_key() -> None:
    token = os.getenv("OPENROUTER_API_KEY")
    if not token:
        raise RuntimeError("OPENROUTER_API_KEY is required for semantic enrichment")
    response = requests.get(
        "https://openrouter.ai/api/v1/key",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()
    metadata = response.json()["data"]
    limit = metadata.get("limit")
    usage = metadata.get("usage", 0)
    if limit is not None and usage >= limit:
        raise RuntimeError(
            f"OpenRouter key limit is exhausted (usage={usage}, limit={limit}); "
            "increase the key limit before running enrichment"
        )


def enrich(limit: int, output: Path) -> int:
    session = create_session(semantic=True)
    articles = (
        session.table("article_snapshots")
        .filter(fc.col("fetch_ok"))
        .order_by(fc.desc("fetched_at"))
        .limit(limit)
    )
    mapped = articles.select(
        "snapshot_id",
        "story_id",
        "canonical_url",
        "fetched_at",
        fc.semantic.map(
            SIGNAL_PROMPT,
            article=fc.col("article_text"),
            max_output_tokens=256,
        ).alias("semantic_note"),
    )
    enriched = mapped.with_column(
        "signals",
        fc.text.extract(
            fc.col("semantic_note"),
            "topic=${topic}\nsummary=${summary}\nentities=${named_entities}",
        ),
    ).unnest("signals")
    enriched.write.save_as_table("story_signals", mode="overwrite")
    session.catalog.set_table_description(
        "story_signals",
        "Fenic/OpenRouter topic, summary, and named-entity extraction for recent articles.",
    )
    saved = session.table("story_signals")
    output.parent.mkdir(parents=True, exist_ok=True)
    saved.write.parquet(output, mode="overwrite")
    count = saved.count()
    session.stop()
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--output", type=Path, default=Path("dist/story-signals.parquet"))
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.limit <= 200:
        raise SystemExit("--limit must be between 1 and 200")
    validate_openrouter_key()
    count = enrich(args.limit, args.output)
    if args.publish:
        HfApi(token=os.getenv("HF_TOKEN")).upload_file(
            path_or_fileobj=args.output,
            path_in_repo="semantic/story-signals.parquet",
            repo_id=os.getenv("BBC_NEWS_DATASET", "AlastairH/bbc-news-logger"),
            repo_type="dataset",
            commit_message="Refresh Fenic semantic story signals",
        )
    print({"story_signals": count, "published": args.publish})


if __name__ == "__main__":
    main()
