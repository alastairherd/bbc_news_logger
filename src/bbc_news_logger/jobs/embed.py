"""CLI entry point for checkpointed BGE Small embeddings."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from pathlib import Path

from bbc_news_logger.config import DEFAULT_DATASET_ID
from bbc_news_logger.semantics import run_embedding_refresh


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--dataset", default=os.getenv("BBC_NEWS_DATASET", DEFAULT_DATASET_ID))
    parser.add_argument("--output-dir", type=Path, default=Path("dist/embedding-shards"))
    parser.add_argument("--report", type=Path, default=Path("dist/embedding-run.json"))
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    if args.limit < 0:
        raise SystemExit("--limit cannot be negative; use 0 for all remaining articles")
    report = run_embedding_refresh(
        dataset_id=args.dataset,
        limit=args.limit,
        batch_size=args.batch_size,
        publish=args.publish,
        output_dir=args.output_dir,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(asdict(report), indent=2) + "\n")
    print(json.dumps(asdict(report), sort_keys=True))


if __name__ == "__main__":
    main()
