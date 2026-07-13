"""Migrate legacy loose files and ZIP archives into versioned Parquet datasets."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from collections import defaultdict
from collections.abc import Iterator
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .models import ArticleSnapshot, Observation, stable_id
from .storage import (
    articles_table,
    observations_table,
    partition_path,
    raw_articles_table,
    write_parquet,
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_sources(data_dir: Path) -> Iterator[tuple[str, bytes, str]]:
    for path in sorted(data_dir.glob("bbc_*.csv")):
        yield path.name, path.read_bytes(), sha256_file(path)
    for archive in sorted((data_dir / "archive").glob("bbc_*/*/*.zip")):
        with zipfile.ZipFile(archive) as bundle:
            for info in bundle.infolist():
                if info.filename.endswith(".csv"):
                    payload = bundle.read(info)
                    digest = hashlib.sha256(payload).hexdigest()
                    yield f"{archive.as_posix()}::{info.filename}", payload, digest


def read_legacy_observations(data_dir: Path) -> tuple[list[Observation], list[dict[str, object]]]:
    observations: dict[tuple[str, str, int], Observation] = {}
    sources: list[dict[str, object]] = []
    for source, payload, digest in _csv_sources(data_dir):
        text = io.StringIO(payload.decode("utf-8-sig"))
        reader = csv.DictReader(text)
        row_count = 0
        promo_positions: dict[datetime, int] = defaultdict(int)
        for row in reader:
            timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S UTC").replace(
                tzinfo=timezone.utc
            )
            surface = "most_read" if "rank" in row else "front_page"
            if surface == "most_read":
                position = int(row.get("rank") or row.get("position") or 0)
            else:
                promo_positions[timestamp] += 1
                position = int(row.get("position") or promo_positions[timestamp])
            scrape_id = stable_id(timestamp.isoformat(), "bbc-news-home")
            observation = Observation.create(
                observed_at=timestamp,
                surface=surface,
                position=position,
                title=row.get("title", ""),
                url=row.get("link", ""),
                scrape_id=scrape_id,
            )
            observations[(scrape_id, surface, position)] = observation
            row_count += 1
        sources.append({"source": source, "sha256": digest, "rows": row_count, "kind": "csv"})
    return list(observations.values()), sources


def _parquet_sources(data_dir: Path) -> Iterator[tuple[str, pa.Table, str, date]]:
    for path in sorted((data_dir / "article-content").glob("*.parquet")):
        day = date.fromisoformat(path.stem)
        yield path.as_posix(), pq.read_table(path), sha256_file(path), day
    for archive in sorted((data_dir / "archive" / "article-content").glob("*/*.zip")):
        with zipfile.ZipFile(archive) as bundle:
            for info in bundle.infolist():
                if not info.filename.endswith(".parquet"):
                    continue
                payload = bundle.read(info)
                day = date.fromisoformat(Path(info.filename).stem)
                yield (
                    f"{archive.as_posix()}::{info.filename}",
                    pq.read_table(pa.BufferReader(payload)),
                    hashlib.sha256(payload).hexdigest(),
                    day,
                )


def read_legacy_articles(
    data_dir: Path,
    first_observed_by_url: dict[str, datetime] | None = None,
) -> tuple[list[ArticleSnapshot], list[dict[str, object]]]:
    snapshots: dict[str, ArticleSnapshot] = {}
    sources: list[dict[str, object]] = []
    for source, table, digest, day in _parquet_sources(data_dir):
        row_count = 0
        inferred_fetch = datetime.combine(day + timedelta(days=1), time(2), timezone.utc)
        for row in table.to_pylist():
            authors = row.get("authors") or []
            if isinstance(authors, str):
                authors = [part for part in authors.split(";") if part]
            url = row.get("url", "")
            first_seen = row.get("first_appeared_at") or inferred_fetch
            if isinstance(first_seen, str):
                try:
                    first_seen = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
                except ValueError:
                    first_seen = (first_observed_by_url or {}).get(url, inferred_fetch)
            first_seen = first_seen.replace(tzinfo=first_seen.tzinfo or timezone.utc)
            snapshot = ArticleSnapshot.create(
                requested_url=url,
                canonical_url=url,
                first_observed_at=first_seen,
                fetched_at=inferred_fetch,
                fetched_at_is_inferred=True,
                title=row.get("title", ""),
                authors=authors,
                article_text=row.get("article_text") or "",
                article_html=row.get("article_html") or "",
                http_status=200 if row.get("fetch_ok") else None,
                fetch_ok=bool(row.get("fetch_ok")),
            )
            snapshots[snapshot.snapshot_id] = snapshot
            row_count += 1
        sources.append({"source": source, "sha256": digest, "rows": row_count, "kind": "parquet"})
    return list(snapshots.values()), sources


def build_migration(data_dir: Path, output_dir: Path, source_commit: str) -> dict[str, object]:
    observations, observation_sources = read_legacy_observations(data_dir)
    first_observed_by_url: dict[str, datetime] = {}
    for row in sorted(observations, key=lambda item: item.observed_at):
        first_observed_by_url.setdefault(row.url, row.observed_at)
    articles, article_sources = read_legacy_articles(data_dir, first_observed_by_url)
    destinations: list[dict[str, object]] = []

    grouped_observations: dict[date, list[Observation]] = defaultdict(list)
    for row in observations:
        grouped_observations[row.observed_at.date()].append(row)
    for day, rows in sorted(grouped_observations.items()):
        relative = partition_path("observations", day)
        target = write_parquet(observations_table(rows), output_dir / "main" / relative)
        destinations.append(
            {"repo": "main", "path": relative, "rows": len(rows), "sha256": sha256_file(target)}
        )

    grouped_articles: dict[date, list[ArticleSnapshot]] = defaultdict(list)
    for row in articles:
        grouped_articles[(row.fetched_at - timedelta(days=1)).date()].append(row)
    for day, rows in sorted(grouped_articles.items()):
        main_relative = partition_path("article_snapshots", day)
        raw_relative = partition_path("raw_article_snapshots", day)
        main_target = write_parquet(articles_table(rows), output_dir / "main" / main_relative)
        raw_target = write_parquet(raw_articles_table(rows), output_dir / "raw" / raw_relative)
        destinations.extend(
            [
                {
                    "repo": "main",
                    "path": main_relative,
                    "rows": len(rows),
                    "sha256": sha256_file(main_target),
                },
                {
                    "repo": "raw",
                    "path": raw_relative,
                    "rows": len(rows),
                    "sha256": sha256_file(raw_target),
                },
            ]
        )

    manifest = {
        "schema_version": 1,
        "source_commit": source_commit,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_files": observation_sources + article_sources,
        "destinations": destinations,
        "totals": {
            "observations": len(observations),
            "article_snapshots": len(articles),
            "source_files": len(observation_sources) + len(article_sources),
        },
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest
