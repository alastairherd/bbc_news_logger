"""Build compact, static dashboard marts from observation Parquet."""

from __future__ import annotations

import json
import math
import tempfile
from array import array
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import snapshot_download

from .compaction import download_patterns, parquet_files
from .config import DEFAULT_DATASET_ID

SEMANTIC_PREFIXES = (
    "data/observations",
    "data/article_snapshots",
    "semantic/signals",
    "semantic/embeddings",
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
            allow_patterns=download_patterns(["data/observations"]),
            token=token,
            max_workers=8,
        )
    )
    files = parquet_files(snapshot, "data/observations")
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
            allow_patterns=download_patterns(SEMANTIC_PREFIXES),
            token=token,
            max_workers=8,
        )
    )
    tables: dict[str, pa.Table | None] = {}
    for prefix in SEMANTIC_PREFIXES:
        files = parquet_files(snapshot, prefix)
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


def _latest_rows(
    table: pa.Table | None, timestamp_field: str
) -> dict[str, dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for row in table.to_pylist() if table is not None else []:
        content_hash = str(row.get("content_sha256") or "")
        existing = rows.get(content_hash)
        if content_hash and (
            existing is None or row[timestamp_field] > existing[timestamp_field]
        ):
            rows[content_hash] = row
    return rows


def _semantic_index(
    article_by_hash: dict[str, dict[str, object]],
    signal_by_hash: dict[str, dict[str, object]],
    embeddings: pa.Table | None,
    story_stats: dict[str, dict[str, object]],
) -> tuple[dict[str, object], bytes]:
    embedding_by_hash = _latest_rows(embeddings, "generated_at")
    latest_by_story: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    for content_hash in article_by_hash.keys() & embedding_by_hash.keys():
        article = article_by_hash[content_hash]
        story_id = str(article["story_id"])
        existing = latest_by_story.get(story_id)
        if existing is None or article["fetched_at"] > existing[0]["fetched_at"]:
            latest_by_story[story_id] = (article, embedding_by_hash[content_hash])

    vector_bytes = array("b")
    documents: list[dict[str, object]] = []
    model = "BAAI/bge-small-en-v1.5"
    dimensions = 384
    for story_id, (article, embedding) in sorted(latest_by_story.items()):
        values = [float(value) for value in embedding.get("embedding") or []]
        if not values:
            continue
        model = str(embedding.get("model") or model)
        dimensions = len(values)
        maximum = max(abs(value) for value in values)
        scale = maximum / 127 if maximum else 1.0
        quantized = [max(-127, min(127, round(value / scale))) for value in values]
        vector_bytes.extend(quantized)
        norm = math.sqrt(sum((value * scale) ** 2 for value in quantized))
        content_hash = str(article["content_sha256"])
        signal = signal_by_hash.get(content_hash, {})
        stats = story_stats.get(story_id, {})
        documents.append(
            {
                "story_id": story_id,
                "content_sha256": content_hash,
                "title": str(article.get("title") or stats.get("title") or "Untitled"),
                "url": str(article.get("canonical_url") or stats.get("url") or ""),
                "fetched_at": article["fetched_at"].astimezone(timezone.utc).isoformat(),
                "surfaces": list(stats.get("surfaces") or []),
                "best_position": stats.get("best_position"),
                "summary": str(signal.get("summary") or ""),
                "topic": str(signal.get("topic") or "unlabelled"),
                "themes": list(signal.get("themes") or []),
                "story_form": str(signal.get("story_form") or "unlabelled"),
                "event_type": str(signal.get("event_type") or "unlabelled"),
                "named_entities": list(signal.get("named_entities") or []),
                "scale": scale,
                "norm": norm,
            }
        )
    return (
        {
            "schemaVersion": 1,
            "model": model,
            "dimensions": dimensions,
            "documentCount": len(documents),
            "vectorFile": "semantic-vectors.i8",
            "binaryFormat": "row-major signed int8; per-row scale and norm in metadata",
            "documents": documents,
        },
        vector_bytes.tobytes(),
    )


def _semantic_findings(
    article_by_hash: dict[str, dict[str, object]],
    signal_by_hash: dict[str, dict[str, object]],
    stories: list[dict[str, object]],
    recurring: list[dict[str, object]],
) -> dict[str, object]:
    covered = [
        (article_by_hash[content_hash], signal)
        for content_hash, signal in signal_by_hash.items()
        if content_hash in article_by_hash
    ]
    if not covered:
        return {
            "window": None,
            "risingThemes": [],
            "fallingThemes": [],
            "storyForms": [],
            "surfaceDifferences": [],
            "returningStories": [],
        }

    latest = max(article["fetched_at"].date() for article, _ in covered)
    recent_start = latest - timedelta(days=13)
    previous_end = recent_start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=13)

    def period(start: object, end: object) -> tuple[Counter[str], int]:
        rows = [
            signal
            for article, signal in covered
            if start <= article["fetched_at"].date() <= end
        ]
        return Counter(str(theme) for row in rows for theme in row.get("themes") or []), len(rows)

    recent, recent_total = period(recent_start, latest)
    previous, previous_total = period(previous_start, previous_end)
    changes = []
    for theme in recent.keys() | previous.keys():
        if recent[theme] + previous[theme] < 2:
            continue
        recent_share = recent[theme] / recent_total * 100 if recent_total else 0.0
        previous_share = previous[theme] / previous_total * 100 if previous_total else 0.0
        changes.append(
            {
                "theme": theme,
                "recentCount": recent[theme],
                "previousCount": previous[theme],
                "recentShare": round(recent_share, 2),
                "previousShare": round(previous_share, 2),
                "changePercentagePoints": round(recent_share - previous_share, 2),
            }
        )

    form_counts = Counter(str(signal.get("story_form") or "other") for _, signal in covered)
    form_total = sum(form_counts.values())
    story_forms = [
        {"storyForm": value, "count": count, "share": round(count / form_total * 100, 1)}
        for value, count in form_counts.most_common()
    ]

    story_stats = {str(row["story_id"]): row for row in stories}
    latest_covered_story: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    for article, signal in covered:
        story_id = str(article["story_id"])
        existing = latest_covered_story.get(story_id)
        if existing is None or article["fetched_at"] > existing[0]["fetched_at"]:
            latest_covered_story[story_id] = (article, signal)
    surface_counts: dict[str, Counter[str]] = {
        "front_page": Counter(),
        "most_read": Counter(),
    }
    surface_totals = Counter()
    for story_id, (_, signal) in latest_covered_story.items():
        for surface in story_stats.get(story_id, {}).get("surfaces") or []:
            if surface not in surface_counts:
                continue
            surface_totals[surface] += 1
            surface_counts[surface].update(str(theme) for theme in signal.get("themes") or [])
    surface_differences = []
    for theme in surface_counts["front_page"].keys() | surface_counts["most_read"].keys():
        front = (
            surface_counts["front_page"][theme] / surface_totals["front_page"] * 100
            if surface_totals["front_page"]
            else 0.0
        )
        read = (
            surface_counts["most_read"][theme] / surface_totals["most_read"] * 100
            if surface_totals["most_read"]
            else 0.0
        )
        if surface_counts["front_page"][theme] + surface_counts["most_read"][theme] < 3:
            continue
        surface_differences.append(
            {
                "theme": theme,
                "frontPageShare": round(front, 2),
                "mostReadShare": round(read, 2),
                "differencePercentagePoints": round(read - front, 2),
            }
        )
    surface_differences.sort(
        key=lambda row: abs(float(row["differencePercentagePoints"])), reverse=True
    )
    changes.sort(key=lambda row: float(row["changePercentagePoints"]), reverse=True)
    prominent_recurring = sorted(
        recurring,
        key=lambda event: (
            int(event["article_count"]),
            int(event["version_count"]),
            str(event["last_seen"]),
        ),
        reverse=True,
    )
    returning = [
        {key: value for key, value in event.items() if key != "articles"}
        for event in prominent_recurring[:8]
    ]
    return {
        "window": {
            "recentStart": recent_start.isoformat(),
            "recentEnd": latest.isoformat(),
            "previousStart": previous_start.isoformat(),
            "previousEnd": previous_end.isoformat(),
            "recentLabelledArticles": recent_total,
            "previousLabelledArticles": previous_total,
        },
        "risingThemes": changes[:8],
        "fallingThemes": list(reversed(changes[-8:])),
        "storyForms": story_forms,
        "surfaceDifferences": surface_differences[:10],
        "returningStories": returning,
    }


def _semantic_payloads(
    *,
    articles: pa.Table | None,
    signals: pa.Table | None,
    embeddings: pa.Table | None,
    events: pa.Table | None,
    stories: list[dict[str, object]],
) -> tuple[dict[str, object], bytes, dict[str, object]]:
    article_by_hash: dict[str, dict[str, object]] = {}
    if articles is not None:
        for row in articles.to_pylist():
            content_hash = str(row.get("content_sha256") or "")
            if not content_hash or not row.get("fetch_ok"):
                continue
            existing = article_by_hash.get(content_hash)
            if existing is None or row["fetched_at"] > existing["fetched_at"]:
                article_by_hash[content_hash] = row
    signal_by_hash: dict[str, dict[str, object]] = {}
    for row in signals.to_pylist() if signals is not None else []:
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
            if len(latest_by_story) < 2:
                continue
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

    findings = _semantic_findings(article_by_hash, signal_by_hash, stories, recurring)
    documents, vector_bytes = _semantic_index(
        article_by_hash, signal_by_hash, embeddings, story_stats
    )
    article_count = len(article_by_hash)
    signal_count = len(article_by_hash.keys() & signal_by_hash.keys())
    coverage = round(signal_count / article_count * 100, 1) if article_count else 0.0
    return (
        {
            "semantic-trends.json": trends,
            "recurring-events.json": recurring,
            "semantic-findings.json": findings,
            "semantic-documents.json": documents,
        },
        vector_bytes,
        {
            "signalCount": signal_count,
            "articleVersionCount": article_count,
            "coveragePercent": coverage,
            "recurringEventCount": len(recurring),
            "searchDocumentCount": documents["documentCount"],
            "embeddingModel": documents["model"],
        },
    )


def build_marts(
    table: pa.Table,
    output_dir: Path,
    *,
    articles: pa.Table | None = None,
    signals: pa.Table | None = None,
    embeddings: pa.Table | None = None,
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

    semantic_payloads, semantic_vectors, semantic_manifest = _semantic_payloads(
        articles=articles,
        signals=signals,
        embeddings=embeddings,
        events=events,
        stories=stories,
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
    (output_dir / "semantic-vectors.i8").write_bytes(semantic_vectors)

    latest = max(row["observed_at"] for row in rank_series) if rank_series else None
    manifest = {
        "schemaVersion": 2,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "latestObservationAt": latest,
        "observationCount": table.num_rows,
        "storyCount": len(stories),
        "semantics": semantic_manifest,
        "files": {
            **{
                name: (
                    int(payload.get("documentCount", len(payload)))
                    if isinstance(payload, dict)
                    else len(payload)
                )
                for name, payload in payloads.items()
            },
            "semantic-vectors.i8": len(semantic_vectors),
        },
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
            embeddings=tables["semantic/embeddings"],
            events=tables["semantic/events"],
        )
