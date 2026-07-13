"""Arrow schemas, partitioning, and Hugging Face publication."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

from .config import DEFAULT_DATASET_ID, DEFAULT_RAW_DATASET_ID, SCHEMA_VERSION
from .models import ArticleSnapshot, Observation, ScrapeRun

OBSERVATION_SCHEMA = pa.schema(
    [
        pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("surface", pa.string(), nullable=False),
        pa.field("position", pa.int8(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("url", pa.string(), nullable=False),
        pa.field("story_id", pa.string(), nullable=False),
        pa.field("scrape_id", pa.string(), nullable=False),
    ],
    metadata={b"schema_version": str(SCHEMA_VERSION).encode()},
)

SCRAPE_RUN_SCHEMA = pa.schema(
    [
        pa.field("scrape_id", pa.string(), nullable=False),
        pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("completed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("success", pa.bool_(), nullable=False),
        pa.field("http_status", pa.int16()),
        pa.field("most_read_count", pa.int16(), nullable=False),
        pa.field("front_page_count", pa.int16(), nullable=False),
        pa.field("selector_version", pa.string(), nullable=False),
        pa.field("error", pa.string(), nullable=False),
        pa.field("workflow_run_url", pa.string(), nullable=False),
    ],
    metadata={b"schema_version": str(SCHEMA_VERSION).encode()},
)

ARTICLE_SCHEMA = pa.schema(
    [
        pa.field("snapshot_id", pa.string(), nullable=False),
        pa.field("requested_url", pa.string(), nullable=False),
        pa.field("canonical_url", pa.string(), nullable=False),
        pa.field("story_id", pa.string(), nullable=False),
        pa.field("first_observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("fetched_at_is_inferred", pa.bool_(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("authors", pa.list_(pa.string()), nullable=False),
        pa.field("article_text", pa.string(), nullable=False),
        pa.field("content_sha256", pa.string(), nullable=False),
        pa.field("html_sha256", pa.string(), nullable=False),
        pa.field("http_status", pa.int16()),
        pa.field("fetch_ok", pa.bool_(), nullable=False),
    ],
    metadata={b"schema_version": str(SCHEMA_VERSION).encode()},
)

RAW_ARTICLE_SCHEMA = pa.schema(
    [
        pa.field("snapshot_id", pa.string(), nullable=False),
        pa.field("canonical_url", pa.string(), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("article_html", pa.string(), nullable=False),
        pa.field("html_sha256", pa.string(), nullable=False),
    ],
    metadata={b"schema_version": str(SCHEMA_VERSION).encode()},
)


def observations_table(rows: Iterable[Observation]) -> pa.Table:
    return pa.Table.from_pylist([asdict(row) for row in rows], schema=OBSERVATION_SCHEMA)


def scrape_runs_table(rows: Iterable[ScrapeRun]) -> pa.Table:
    return pa.Table.from_pylist([asdict(row) for row in rows], schema=SCRAPE_RUN_SCHEMA)


def articles_table(rows: Iterable[ArticleSnapshot]) -> pa.Table:
    records = []
    for row in rows:
        value = asdict(row)
        value.pop("article_html")
        value["authors"] = list(row.authors)
        records.append(value)
    return pa.Table.from_pylist(records, schema=ARTICLE_SCHEMA)


def raw_articles_table(rows: Iterable[ArticleSnapshot]) -> pa.Table:
    records = [
        {
            "snapshot_id": row.snapshot_id,
            "canonical_url": row.canonical_url,
            "fetched_at": row.fetched_at,
            "article_html": row.article_html,
            "html_sha256": row.html_sha256,
        }
        for row in rows
    ]
    return pa.Table.from_pylist(records, schema=RAW_ARTICLE_SCHEMA)


def partition_path(kind: str, day: date) -> str:
    return f"data/{kind}/year={day:%Y}/month={day:%m}/{day:%Y-%m-%d}.parquet"


def write_parquet(table: pa.Table, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="zstd",
        compression_level=9,
        use_dictionary=True,
        write_statistics=True,
    )
    return path


def merge_unique(existing: pa.Table | None, incoming: pa.Table, keys: tuple[str, ...]) -> pa.Table:
    rows: dict[tuple[object, ...], dict[str, object]] = {}
    if existing is not None:
        for row in existing.to_pylist():
            rows[tuple(row[key] for key in keys)] = row
    for row in incoming.to_pylist():
        rows[tuple(row[key] for key in keys)] = row
    ordered = sorted(rows.values(), key=lambda row: tuple(str(row[key]) for key in keys))
    return pa.Table.from_pylist(ordered, schema=incoming.schema)


class HuggingFacePublisher:
    """Small transactional facade over dataset repository files."""

    def __init__(
        self,
        *,
        dataset_id: str = DEFAULT_DATASET_ID,
        raw_dataset_id: str = DEFAULT_RAW_DATASET_ID,
        token: str | None = None,
    ) -> None:
        self.dataset_id = dataset_id
        self.raw_dataset_id = raw_dataset_id
        self.token = token or os.getenv("HF_TOKEN")
        self.api = HfApi(token=self.token)

    def read_table(self, repo_id: str, path: str) -> pa.Table | None:
        try:
            local = hf_hub_download(
                repo_id=repo_id,
                repo_type="dataset",
                filename=path,
                token=self.token,
            )
        except EntryNotFoundError:
            return None
        return pq.read_table(local)

    def upsert_table(
        self,
        *,
        repo_id: str,
        path: str,
        incoming: pa.Table,
        keys: tuple[str, ...],
        message: str,
    ) -> int:
        existing = self.read_table(repo_id, path)
        merged = merge_unique(existing, incoming, keys)
        with tempfile.TemporaryDirectory(prefix="bbc-news-publish-") as tmp:
            local = write_parquet(merged, Path(tmp) / Path(path).name)
            self.api.upload_file(
                path_or_fileobj=local,
                path_in_repo=path,
                repo_id=repo_id,
                repo_type="dataset",
                commit_message=message,
            )
        return merged.num_rows

    def publish_observations(self, rows: Iterable[Observation], run: ScrapeRun) -> None:
        rows = tuple(rows)
        if not rows:
            raise ValueError("Cannot publish an empty observation batch")
        day = rows[0].observed_at.date()
        self.upsert_table(
            repo_id=self.dataset_id,
            path=partition_path("observations", day),
            incoming=observations_table(rows),
            keys=("scrape_id", "surface", "position"),
            message=f"Update observations for {day:%Y-%m-%d}",
        )
        self.upsert_table(
            repo_id=self.dataset_id,
            path=partition_path("scrape_runs", day),
            incoming=scrape_runs_table([run]),
            keys=("scrape_id",),
            message=f"Update scrape runs for {day:%Y-%m-%d}",
        )

    def publish_articles(self, rows: Iterable[ArticleSnapshot], day: date) -> None:
        rows = tuple(rows)
        if not rows:
            raise ValueError("Cannot publish an empty article batch")
        self.upsert_table(
            repo_id=self.dataset_id,
            path=partition_path("article_snapshots", day),
            incoming=articles_table(rows),
            keys=("snapshot_id",),
            message=f"Update article snapshots for {day:%Y-%m-%d}",
        )
        self.upsert_table(
            repo_id=self.raw_dataset_id,
            path=partition_path("raw_article_snapshots", day),
            incoming=raw_articles_table(rows),
            keys=("snapshot_id",),
            message=f"Update raw article snapshots for {day:%Y-%m-%d}",
        )

    def upload_manifest(self, manifest: dict[str, object]) -> None:
        payload = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
        self.api.upload_file(
            path_or_fileobj=payload,
            path_in_repo="migration/manifest.json",
            repo_id=self.dataset_id,
            repo_type="dataset",
            commit_message="Add migration manifest",
        )


def coerce_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).replace(" UTC", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc)
