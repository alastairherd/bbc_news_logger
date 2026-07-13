"""CLI entry point for recurring-story clustering."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from bbc_news_logger.clustering import build_remote_event_clusters
from bbc_news_logger.config import DEFAULT_DATASET_ID


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", default=os.getenv("BBC_NEWS_DATASET", DEFAULT_DATASET_ID))
    parser.add_argument("--output", type=Path, default=Path("dist/event-clusters.parquet"))
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    table = build_remote_event_clusters(
        dataset_id=args.dataset, publish=args.publish, output=args.output
    )
    clusters = len(set(table.column("cluster_id").to_pylist())) if table.num_rows else 0
    recurring = len(
        {
            row["cluster_id"]
            for row in table.select(["cluster_id", "cluster_size"]).to_pylist()
            if row["cluster_size"] > 1
        }
    )
    print(json.dumps({"rows": table.num_rows, "clusters": clusters, "recurring": recurring}))


if __name__ == "__main__":
    main()
