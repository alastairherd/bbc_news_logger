"""Build compact, static dashboard marts from observation Parquet."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import snapshot_download

from .config import DEFAULT_DATASET_ID


def load_remote_observations(
    dataset_id: str = DEFAULT_DATASET_ID,
    token: str | None = None,
) -> pa.Table:
    snapshot = Path(
        snapshot_download(
            repo_id=dataset_id,
            repo_type="dataset",
            allow_patterns="data/observations/**/*.parquet",
            token=token,
            max_workers=8,
        )
    )
    files = sorted((snapshot / "data" / "observations").rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No observation partitions found in {dataset_id}")
    tables = [pq.ParquetFile(path).read() for path in files]
    return pa.concat_tables(tables, promote_options="default")


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    table = cursor.to_arrow_table()
    rows = table.to_pylist()
    for row in rows:
        for key, value in tuple(row.items()):
            if isinstance(value, (datetime,)):
                row[key] = value.astimezone(timezone.utc).isoformat()
    return rows


def build_marts(table: pa.Table, output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("observation_input", table)
    con.execute(
        """
        CREATE TABLE observations AS
        SELECT
            observed_at,
            CAST(observed_at AS DATE) AS observed_date,
            surface,
            position,
            title,
            url,
            story_id,
            scrape_id
        FROM observation_input
        """
    )

    stories = _records(
        con.execute(
            """
            SELECT
                story_id,
                arg_max(title, observed_at) AS title,
                arg_max(url, observed_at) AS url,
                min(observed_at) AS first_seen,
                max(observed_at) AS last_seen,
                count(*) AS observation_count,
                min(position) AS best_position,
                list_sort(list_distinct(list(surface))) AS surfaces
            FROM observations
            GROUP BY story_id
            ORDER BY last_seen DESC, observation_count DESC
            """
        )
    )
    rank_series = _records(
        con.execute(
            """
            SELECT observed_at, surface, position, title, url, story_id
            FROM observations
            WHERE observed_at >= (SELECT max(observed_at) - INTERVAL '30 days' FROM observations)
            ORDER BY observed_at, surface, position
            """
        )
    )
    daily = _records(
        con.execute(
            """
            WITH by_surface AS (
                SELECT
                    observed_date,
                    count(DISTINCT CASE
                        WHEN surface = 'front_page' THEN story_id
                    END) AS front_page_stories,
                    count(DISTINCT CASE
                        WHEN surface = 'most_read' THEN story_id
                    END) AS most_read_stories
                FROM observations
                GROUP BY observed_date
            ), overlap AS (
                SELECT observed_date, count(*) AS overlapping_stories
                FROM (
                    SELECT observed_date, story_id
                    FROM observations
                    GROUP BY observed_date, story_id
                    HAVING count(DISTINCT surface) = 2
                )
                GROUP BY observed_date
            )
            SELECT
                s.observed_date,
                s.front_page_stories,
                s.most_read_stories,
                coalesce(o.overlapping_stories, 0) AS overlapping_stories
            FROM by_surface s
            LEFT JOIN overlap o USING (observed_date)
            ORDER BY observed_date
            """
        )
    )
    lag = _records(
        con.execute(
            """
            WITH first_seen AS (
                SELECT
                    story_id,
                    min(CASE WHEN surface = 'front_page' THEN observed_at END) AS first_front_page,
                    min(CASE WHEN surface = 'most_read' THEN observed_at END) AS first_most_read,
                    arg_max(title, observed_at) AS title,
                    arg_max(url, observed_at) AS url
                FROM observations
                GROUP BY story_id
            )
            SELECT
                story_id,
                title,
                url,
                first_front_page,
                first_most_read,
                date_diff('minute', first_front_page, first_most_read) AS lag_minutes
            FROM first_seen
            WHERE first_front_page IS NOT NULL AND first_most_read IS NOT NULL
            ORDER BY first_most_read DESC
            """
        )
    )

    payloads = {
        "stories.json": stories,
        "rank-series.json": rank_series,
        "daily.json": daily,
        "surface-lag.json": lag,
    }
    for name, payload in payloads.items():
        (output_dir / name).write_text(
            json.dumps(payload, separators=(",", ":"), default=str) + "\n"
        )

    latest = max(row["observed_at"] for row in rank_series) if rank_series else None
    manifest = {
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "latestObservationAt": latest,
        "observationCount": table.num_rows,
        "storyCount": len(stories),
        "files": {name: len(payload) for name, payload in payloads.items()},
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def build_remote_marts(output_dir: Path, dataset_id: str = DEFAULT_DATASET_ID) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="bbc-news-marts-"):
        return build_marts(load_remote_observations(dataset_id), output_dir)
