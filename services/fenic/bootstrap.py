"""Materialize the public dataset into a persistent Fenic catalog."""

from __future__ import annotations

import argparse
import os
from collections.abc import Iterable
from pathlib import Path

import fenic as fc
from huggingface_hub import snapshot_download

DATASET_ID = os.getenv("BBC_NEWS_DATASET", "AlastairH/bbc-news-logger")
APP_NAME = os.getenv("FENIC_APP_NAME", "bbc_news_research_lab")
DB_PATH = Path(os.getenv("FENIC_DB_PATH", ".fenic"))
TABLES = {
    "observations": (
        "data/observations/",
        "Hourly position-level observations from the BBC News front page and Most Read list.",
    ),
    "article_snapshots": (
        "data/article_snapshots/",
        "Parsed article snapshots with stable story keys, metadata, and plain text.",
    ),
    "scrape_runs": (
        "data/scrape_runs/",
        "Operational metadata and validation counts for collection runs.",
    ),
    "story_signals": (
        "semantic/",
        "DeepSeek topic, theme, event, summary, and named-entity signals for articles.",
    ),
}


def create_session() -> fc.Session:
    DB_PATH.mkdir(parents=True, exist_ok=True)
    return fc.Session.get_or_create(
        fc.SessionConfig(
            app_name=APP_NAME,
            db_path=DB_PATH,
        )
    )


def dataset_paths(prefix: str) -> list[str]:
    snapshot = Path(
        snapshot_download(
            repo_id=DATASET_ID,
            repo_type="dataset",
            allow_patterns=f"{prefix}**/*.parquet",
            token=os.getenv("HF_TOKEN"),
            max_workers=8,
        )
    )
    return [str(path) for path in sorted((snapshot / prefix).rglob("*.parquet"))]


def bootstrap(table_names: Iterable[str] | None = None) -> dict[str, int]:
    selected = tuple(table_names or TABLES)
    unknown = set(selected) - TABLES.keys()
    if unknown:
        raise ValueError(f"Unknown Fenic tables: {', '.join(sorted(unknown))}")

    session = create_session()
    counts: dict[str, int] = {}
    for table_name in selected:
        prefix, description = TABLES[table_name]
        paths = dataset_paths(prefix)
        if not paths:
            if table_name in {"scrape_runs", "story_signals"}:
                continue
            raise FileNotFoundError(f"No Parquet files found for {table_name} in {DATASET_ID}")
        frame = session.read.parquet(paths)
        frame.write.save_as_table(table_name, mode="overwrite")
        session.catalog.set_table_description(table_name, description)
        counts[table_name] = session.table(table_name).count()
    session.stop(skip_usage_summary=True)
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", nargs="+", choices=TABLES)
    args = parser.parse_args()
    print(bootstrap(args.tables))
