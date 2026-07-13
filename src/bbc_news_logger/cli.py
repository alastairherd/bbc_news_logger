"""Command-line interface for collection, migration, publication, and analytics."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from huggingface_hub import HfApi

from .articles import ArticleTarget, fetch_articles
from .config import DEFAULT_DATASET_ID, DEFAULT_RAW_DATASET_ID
from .marts import build_remote_marts
from .migration import build_migration
from .scrape import collect_homepage
from .storage import HuggingFacePublisher, coerce_utc, partition_path


def _git_head() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def command_scrape(args: argparse.Namespace) -> None:
    result = collect_homepage()
    if args.upload:
        publisher = HuggingFacePublisher(
            dataset_id=args.dataset,
            raw_dataset_id=args.raw_dataset,
        )
        publisher.publish_observations(result.observations, result.run)
    print(
        json.dumps(
            {
                "scrape_id": result.run.scrape_id,
                "observations": len(result.observations),
                "most_read": result.run.most_read_count,
                "front_page": result.run.front_page_count,
                "uploaded": args.upload,
            }
        )
    )


def command_fetch_articles(args: argparse.Namespace) -> None:
    day = (
        date.fromisoformat(args.date)
        if args.date
        else datetime.now(timezone.utc).date() - timedelta(days=1)
    )
    publisher = HuggingFacePublisher(dataset_id=args.dataset, raw_dataset_id=args.raw_dataset)
    table = publisher.read_table(args.dataset, partition_path("observations", day))
    if table is None or table.num_rows == 0:
        raise SystemExit(f"No observations found for {day:%Y-%m-%d}")
    first_seen: dict[str, datetime] = {}
    for row in sorted(table.to_pylist(), key=lambda value: value["observed_at"]):
        first_seen.setdefault(row["url"], coerce_utc(row["observed_at"]))
    targets = [ArticleTarget(url=url, first_observed_at=seen) for url, seen in first_seen.items()]
    snapshots = asyncio.run(fetch_articles(targets))
    if args.upload:
        publisher.publish_articles(snapshots, day)
    print(
        json.dumps(
            {
                "date": day.isoformat(),
                "articles": len(snapshots),
                "uploaded": args.upload,
            }
        )
    )


def command_migrate(args: argparse.Namespace) -> None:
    output = Path(args.output)
    manifest = build_migration(Path(args.data_dir), output, args.source_commit or _git_head())
    if args.publish:
        api = HfApi(token=os.getenv("HF_TOKEN"))
        api.upload_folder(
            repo_id=args.dataset,
            repo_type="dataset",
            folder_path=output / "main",
            path_in_repo=".",
            commit_message="Migrate historical BBC News data",
        )
        api.upload_folder(
            repo_id=args.raw_dataset,
            repo_type="dataset",
            folder_path=output / "raw",
            path_in_repo=".",
            commit_message="Migrate historical raw BBC article HTML",
        )
        api.upload_file(
            repo_id=args.dataset,
            repo_type="dataset",
            path_or_fileobj=output / "manifest.json",
            path_in_repo="migration/manifest.json",
            commit_message="Add migration manifest",
        )
    print(json.dumps(manifest["totals"], sort_keys=True))


def command_build_marts(args: argparse.Namespace) -> None:
    manifest = build_remote_marts(Path(args.output), args.dataset)
    print(json.dumps(manifest, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bbc-news")
    parser.set_defaults(func=lambda _: parser.print_help())
    subcommands = parser.add_subparsers(dest="command")

    scrape = subcommands.add_parser("scrape", help="Collect the BBC News homepage")
    scrape.add_argument("--upload", action="store_true")
    scrape.add_argument("--dataset", default=DEFAULT_DATASET_ID)
    scrape.add_argument("--raw-dataset", default=DEFAULT_RAW_DATASET_ID)
    scrape.set_defaults(func=command_scrape)

    articles = subcommands.add_parser(
        "fetch-articles",
        help="Fetch article snapshots for a UTC date",
    )
    articles.add_argument("--date")
    articles.add_argument("--upload", action="store_true")
    articles.add_argument("--dataset", default=DEFAULT_DATASET_ID)
    articles.add_argument("--raw-dataset", default=DEFAULT_RAW_DATASET_ID)
    articles.set_defaults(func=command_fetch_articles)

    migrate = subcommands.add_parser("migrate", help="Convert legacy data into dataset partitions")
    migrate.add_argument("--data-dir", default="data")
    migrate.add_argument(
        "--output",
        default=str(Path(tempfile.gettempdir()) / "bbc-news-migration"),
    )
    migrate.add_argument("--source-commit")
    migrate.add_argument("--publish", action="store_true")
    migrate.add_argument("--dataset", default=DEFAULT_DATASET_ID)
    migrate.add_argument("--raw-dataset", default=DEFAULT_RAW_DATASET_ID)
    migrate.set_defaults(func=command_migrate)

    marts = subcommands.add_parser("build-marts", help="Build static dashboard marts")
    marts.add_argument("--output", default="web/public/data")
    marts.add_argument("--dataset", default=DEFAULT_DATASET_ID)
    marts.set_defaults(func=command_build_marts)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
