"""Build compact, static dashboard marts from observation Parquet."""

from __future__ import annotations

import json
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import snapshot_download

from .config import DEFAULT_DATASET_ID

SEMANTIC_PREFIXES = (
    "data/observations",
    "data/article_snapshots",
    "semantic/signals",
    "semantic/events",
)


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


def load_remote_mart_tables(
    dataset_id: str = DEFAULT_DATASET_ID,
    token: str | None = None,
) -> dict[str, pa.Table | None]:
    snapshot = Path(
        snapshot_download(
            repo_id=dataset_id,
            repo_type="dataset",
            allow_patterns=[f"{prefix}/**/*.parquet" for prefix in SEMANTIC_PREFIXES],
            token=token,
            max_workers=8,
        )
    )
    tables: dict[str, pa.Table | None] = {}
    for prefix in SEMANTIC_PREFIXES:
        files = sorted((snapshot / prefix).rglob("*.parquet"))
        tables[prefix] = (
            pa.concat_tables(
                [pq.ParquetFile(path).read() for path in files], promote_options="default"
            )
            if files
            else None
        )
    if tables["data/observations"] is None:
        raise FileNotFoundError(f"No observation partitions found in {dataset_id}")
    return tables


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    table = cursor.to_arrow_table()
    rows = table.to_pylist()
    for row in rows:
        for key, value in tuple(row.items()):
            if isinstance(value, (datetime,)):
                row[key] = value.astimezone(timezone.utc).isoformat()
    return rows


def _semantic_payloads(
    *,
    articles: pa.Table | None,
    signals: pa.Table | None,
    events: pa.Table | None,
    stories: list[dict[str, object]],
) -> tuple[dict[str, list[dict[str, object]]], dict[str, object]]:
    article_by_hash: dict[str, dict[str, object]] = {}
    if articles is not None:
        for row in articles.to_pylist():
            content_hash = str(row.get("content_sha256") or "")
            if not content_hash or not row.get("fetch_ok"):
                continue
            existing = article_by_hash.get(content_hash)
            if existing is None or row["fetched_at"] > existing["fetched_at"]:
                article_by_hash[content_hash] = row
    if signals is None:
        return (
            {"semantic-trends.json": [], "recurring-events.json": []},
            {
                "signalCount": 0,
                "articleVersionCount": len(article_by_hash),
                "coveragePercent": 0.0,
                "recurringEventCount": 0,
            },
        )

    signal_by_hash: dict[str, dict[str, object]] = {}
    for row in signals.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        existing = signal_by_hash.get(content_hash)
        if content_hash and (
            existing is None or row["generated_at"] > existing["generated_at"]
        ):
            signal_by_hash[content_hash] = row

    counts: Counter[tuple[str, str, str]] = Counter()
    for content_hash, signal in signal_by_hash.items():
        article = article_by_hash.get(content_hash)
        if article is None:
            continue
        day = str(article["fetched_at"].date())
        counts[(day, "topic", str(signal["topic"]))] += 1
        counts[(day, "story_form", str(signal["story_form"]))] += 1
        counts[(day, "event_type", str(signal["event_type"]))] += 1
        for theme in signal.get("themes") or []:
            counts[(day, "theme", str(theme))] += 1
    trends = [
        {"observed_date": day, "dimension": dimension, "value": value, "article_count": count}
        for (day, dimension, value), count in sorted(counts.items())
    ]

    story_stats = {str(row["story_id"]): row for row in stories}
    recurring: list[dict[str, object]] = []
    if events is not None:
        grouped: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
        for row in events.to_pylist():
            if int(row["cluster_size"]) > 1:
                grouped[str(row["cluster_id"])].append(row)
        for cluster_id, rows in grouped.items():
            latest_by_story: dict[str, dict[str, object]] = {}
            for row in rows:
                story_id = str(row["story_id"])
                current = latest_by_story.get(story_id)
                if current is None or row["fetched_at"] > current["fetched_at"]:
                    latest_by_story[story_id] = row
            articles_payload = []
            for row in sorted(latest_by_story.values(), key=lambda value: value["fetched_at"]):
                stats = story_stats.get(str(row["story_id"]), {})
                articles_payload.append(
                    {
                        "story_id": row["story_id"],
                        "title": row["title"],
                        "url": row["canonical_url"],
                        "fetched_at": row["fetched_at"].astimezone(timezone.utc).isoformat(),
                        "best_position": stats.get("best_position"),
                        "surfaces": stats.get("surfaces", []),
                    }
                )
            theme_counts = Counter(theme for row in rows for theme in row.get("themes") or [])
            recurring.append(
                {
                    "cluster_id": cluster_id,
                    "label": Counter(
                        str(row["cluster_label"]) for row in rows
                    ).most_common(1)[0][0],
                    "event_type": Counter(
                        str(row["event_type"]) for row in rows
                    ).most_common(1)[0][0],
                    "themes": [value for value, _ in theme_counts.most_common(5)],
                    "first_seen": min(row["fetched_at"] for row in rows)
                    .astimezone(timezone.utc)
                    .isoformat(),
                    "last_seen": max(row["fetched_at"] for row in rows)
                    .astimezone(timezone.utc)
                    .isoformat(),
                    "article_count": len(latest_by_story),
                    "version_count": len(rows),
                    "articles": articles_payload,
                }
            )
        recurring.sort(key=lambda row: (row["last_seen"], row["article_count"]), reverse=True)

    article_count = len(article_by_hash)
    signal_count = len(article_by_hash.keys() & signal_by_hash.keys())
    coverage = round(signal_count / article_count * 100, 1) if article_count else 0.0
    return (
        {"semantic-trends.json": trends, "recurring-events.json": recurring},
        {
            "signalCount": signal_count,
            "articleVersionCount": article_count,
            "coveragePercent": coverage,
            "recurringEventCount": len(recurring),
        },
    )


def build_marts(
    table: pa.Table,
    output_dir: Path,
    *,
    articles: pa.Table | None = None,
    signals: pa.Table | None = None,
    events: pa.Table | None = None,
) -> dict[str, object]:
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

    semantic_payloads, semantic_manifest = _semantic_payloads(
        articles=articles, signals=signals, events=events, stories=stories
    )
    payloads = {
        "stories.json": stories,
        "rank-series.json": rank_series,
        "daily.json": daily,
        "surface-lag.json": lag,
        **semantic_payloads,
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
        "semantics": semantic_manifest,
        "files": {name: len(payload) for name, payload in payloads.items()},
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def build_remote_marts(output_dir: Path, dataset_id: str = DEFAULT_DATASET_ID) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="bbc-news-marts-"):
        tables = load_remote_mart_tables(dataset_id)
        return build_marts(
            tables["data/observations"],  # type: ignore[arg-type]
            output_dir,
            articles=tables["data/article_snapshots"],
            signals=tables["semantic/signals"],
            events=tables["semantic/events"],
        )
