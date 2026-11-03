"""Build compact, static dashboard marts from observation Parquet."""

from __future__ import annotations

import json
import math
import statistics
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
from .deepseek import normalize_story_form
from .ontology import (
    ONTOLOGY_VERSION,
    normalize_event_type,
    normalize_theme,
    normalize_themes,
    reconcile_signal_rows,
)
from .taxonomy_wikidata import (
    CURATED_ENTITY_QIDS,
    PARENT_THEME_DOMAIN,
    heuristic_parent_theme,
)

SEMANTIC_PREFIXES = (
    "data/observations",
    "data/article_snapshots",
    "semantic/signals",
    "semantic/embeddings",
    "semantic/events",
    "semantic/weekly",
    "semantic/taxonomy",
    "semantic/wikidata",
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


def _latest_week(weekly: pa.Table | None) -> dict[str, object]:
    """Publish the newest week's theme summaries, largest themes first."""

    if weekly is None or weekly.num_rows == 0:
        return {"weekStart": None, "weekEnd": None, "themes": []}
    rows = weekly.to_pylist()
    week = max(row["week_start"] for row in rows)
    latest: dict[str, dict[str, object]] = {}
    for row in rows:
        if row["week_start"] != week:
            continue
        theme = normalize_theme(row.get("theme")) or str(row["theme"])
        key = theme.casefold()
        if key not in latest or row["generated_at"] > latest[key]["generated_at"]:
            updated = dict(row)
            updated["theme"] = theme
            latest[key] = updated
    themes = sorted(
        latest.values(), key=lambda row: (-int(row["article_count"]), str(row["theme"]))
    )
    return {
        "weekStart": week.isoformat(),
        "weekEnd": themes[0]["week_end"].isoformat(),
        "model": themes[0]["model"],
        "themes": [
            {
                "theme": row["theme"],
                "articleCount": row["article_count"],
                "summary": row["summary"],
                "evidence": [
                    {"storyId": story_id, "title": title, "url": url}
                    for story_id, title, url in zip(
                        row["evidence_story_ids"],
                        row["evidence_titles"],
                        row["evidence_urls"],
                        strict=True,
                    )
                ],
            }
            for row in themes
        ],
    }


def _records(cursor: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    table = cursor.to_arrow_table()
    rows = table.to_pylist()
    for row in rows:
        for key, value in tuple(row.items()):
            if isinstance(value, (datetime,)):
                row[key] = value.astimezone(timezone.utc).isoformat()
    return rows


def _latest_rows(table: pa.Table | None, timestamp_field: str) -> dict[str, dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for row in table.to_pylist() if table is not None else []:
        content_hash = str(row.get("content_sha256") or "")
        existing = rows.get(content_hash)
        if content_hash and (existing is None or row[timestamp_field] > existing[timestamp_field]):
            rows[content_hash] = row
    return rows


def _semantic_index(
    article_by_hash: dict[str, dict[str, object]],
    signal_by_hash: dict[str, dict[str, object]],
    embeddings: pa.Table | None,
    story_stats: dict[str, dict[str, object]],
    story_cluster_index: dict[str, int] | None = None,
    story_cluster_id: dict[str, str] | None = None,
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
    cluster_lookup = story_cluster_index or {}
    cluster_id_lookup = story_cluster_id or {}
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
        fetched_iso = article["fetched_at"].astimezone(timezone.utc).isoformat()
        first_seen_raw = stats.get("first_seen")
        first_seen_date = str(first_seen_raw)[:10] if first_seen_raw else fetched_iso[:10]
        documents.append(
            {
                "story_id": story_id,
                "content_sha256": content_hash,
                "title": str(article.get("title") or stats.get("title") or "Untitled"),
                "url": str(article.get("canonical_url") or stats.get("url") or ""),
                "fetched_at": fetched_iso,
                "first_seen": first_seen_date,
                "surfaces": list(stats.get("surfaces") or []),
                "best_position": stats.get("best_position"),
                "front_page_captures": int(stats.get("front_page_captures") or 0),
                "most_read_captures": int(stats.get("most_read_captures") or 0),
                "best_front_page": stats.get("best_front_page"),
                "best_most_read": stats.get("best_most_read"),
                "cluster_index": cluster_lookup.get(story_id),
                "cluster_id": cluster_id_lookup.get(story_id),
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
            signal for article, signal in covered if start <= article["fetched_at"].date() <= end
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


def _labelled_articles(
    articles: pa.Table | None, signals: pa.Table | None
) -> tuple[
    dict[str, dict[str, object]],
    dict[str, dict[str, object]],
    dict[str, list[str]],
    dict[str, list[str]],
]:
    """Latest successful article fetch and normalized label for each content hash."""

    article_by_hash: dict[str, dict[str, object]] = {}
    if articles is not None:
        for row in articles.to_pylist():
            content_hash = str(row.get("content_sha256") or "")
            if not content_hash or not row.get("fetch_ok"):
                continue
            existing = article_by_hash.get(content_hash)
            if existing is None or row["fetched_at"] > existing["fetched_at"]:
                article_by_hash[content_hash] = row
    raw_signal_by_hash: dict[str, dict[str, object]] = {}
    for row in signals.to_pylist() if signals is not None else []:
        content_hash = str(row.get("content_sha256") or "")
        existing = raw_signal_by_hash.get(content_hash)
        if content_hash and (existing is None or row["generated_at"] > existing["generated_at"]):
            updated = dict(row)
            # Older labels predate the story-form vocabulary check, so they are normalized on read.
            updated["story_form"] = normalize_story_form(row.get("story_form"))
            raw_signal_by_hash[content_hash] = updated
    signal_by_hash, theme_aliases, entity_aliases = reconcile_signal_rows(raw_signal_by_hash)
    return article_by_hash, signal_by_hash, theme_aliases, entity_aliases


def _story_themes(
    article_by_hash: dict[str, dict[str, object]],
    signal_by_hash: dict[str, dict[str, object]],
) -> dict[str, list[str]]:
    """Themes of each story's most recently fetched labelled version."""

    latest: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    for content_hash in article_by_hash.keys() & signal_by_hash.keys():
        article = article_by_hash[content_hash]
        story_id = str(article["story_id"])
        existing = latest.get(story_id)
        if existing is None or article["fetched_at"] > existing[0]["fetched_at"]:
            latest[story_id] = (article, signal_by_hash[content_hash])
    return {
        story_id: [str(theme) for theme in signal.get("themes") or []]
        for story_id, (_, signal) in latest.items()
    }


ONTOLOGY_MIN_STORIES = 5
ONTOLOGY_MAX_ENTITIES = 400


def _taxonomy_lookup(taxonomy: pa.Table | None) -> dict[str, str]:
    lookup: dict[str, str] = {}
    if taxonomy is None or taxonomy.num_rows == 0:
        return lookup
    rows = sorted(taxonomy.to_pylist(), key=lambda row: row["generated_at"])
    for row in rows:
        theme = str(row.get("theme") or "").strip()
        parent = str(row.get("parent_theme") or "").strip()
        if theme and parent in PARENT_THEME_DOMAIN:
            lookup[theme.casefold()] = parent
    return lookup


def _wikidata_lookup(wikidata: pa.Table | None) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {
        key: {
            "qid": meta["qid"],
            "label": meta["label"],
            "description": meta["description"],
            "entity_type": meta["entity_type"],
            "wikidata_url": f"https://www.wikidata.org/wiki/{meta['qid']}",
        }
        for key, meta in CURATED_ENTITY_QIDS.items()
    }
    if wikidata is None or wikidata.num_rows == 0:
        return lookup
    rows = sorted(wikidata.to_pylist(), key=lambda row: row["generated_at"])
    for row in rows:
        entity = str(row.get("entity") or "").strip()
        if not entity:
            continue
        key = entity.casefold()
        if key in CURATED_ENTITY_QIDS:
            continue
        qid = str(row.get("qid") or "").strip()
        lookup[key] = {
            "qid": qid,
            "label": str(row.get("label") or "").strip(),
            "description": str(row.get("description") or "").strip(),
            "entity_type": str(row.get("entity_type") or "other").strip() or "other",
            "wikidata_url": str(row.get("wikidata_url") or "").strip()
            or (f"https://www.wikidata.org/wiki/{qid}" if qid else ""),
        }
    return lookup


def _semantic_ontology(
    *,
    article_by_hash: dict[str, dict[str, object]],
    signal_by_hash: dict[str, dict[str, object]],
    story_stats: dict[str, dict[str, object]],
    recurring: list[dict[str, object]],
    promotion_themes: list[dict[str, object]],
    promotion_parent_themes: list[dict[str, object]] | None = None,
    theme_aliases: dict[str, list[str]],
    entity_aliases: dict[str, list[str]],
    taxonomy_by_theme: dict[str, str] | None = None,
    wikidata_by_entity: dict[str, dict[str, str]] | None = None,
) -> dict[str, object]:
    """Build the governed semantic ontology catalog of canonical themes, entities, and events."""

    latest_by_story: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    for content_hash in article_by_hash.keys() & signal_by_hash.keys():
        article = article_by_hash[content_hash]
        story_id = str(article["story_id"])
        existing = latest_by_story.get(story_id)
        if existing is None or article["fetched_at"] > existing[0]["fetched_at"]:
            latest_by_story[story_id] = (article, signal_by_hash[content_hash])

    theme_stories: defaultdict[str, set[str]] = defaultdict(set)
    theme_front: defaultdict[str, int] = defaultdict(int)
    theme_read: defaultdict[str, int] = defaultdict(int)
    theme_topics: defaultdict[str, Counter[str]] = defaultdict(Counter)

    entity_stories: defaultdict[str, set[str]] = defaultdict(set)
    entity_front: defaultdict[str, int] = defaultdict(int)
    entity_read: defaultdict[str, int] = defaultdict(int)
    entity_themes: defaultdict[str, Counter[str]] = defaultdict(Counter)

    for story_id, (_article, signal) in latest_by_story.items():
        surfaces = set(story_stats.get(story_id, {}).get("surfaces") or [])
        on_front = "front_page" in surfaces
        on_read = "most_read" in surfaces
        topic = str(signal.get("topic") or "other")
        themes = [str(theme) for theme in signal.get("themes") or [] if str(theme)]
        entities = [str(entity) for entity in signal.get("named_entities") or [] if str(entity)]

        for theme in set(themes):
            theme_stories[theme].add(story_id)
            theme_topics[theme][topic] += 1
            if on_front:
                theme_front[theme] += 1
            if on_read:
                theme_read[theme] += 1

        for entity in set(entities):
            entity_stories[entity].add(story_id)
            if on_front:
                entity_front[entity] += 1
            if on_read:
                entity_read[entity] += 1
            for theme in set(themes):
                entity_themes[entity][theme] += 1

    promo_by_theme = {str(row["theme"]).casefold(): row for row in promotion_themes}
    promo_by_parent = {
        str(row["parent_theme"]).casefold(): row for row in (promotion_parent_themes or [])
    }
    tax_lookup = taxonomy_by_theme or {}
    wd_lookup = wikidata_by_entity or _wikidata_lookup(None)

    parent_stories: defaultdict[str, set[str]] = defaultdict(set)
    parent_front: defaultdict[str, set[str]] = defaultdict(set)
    parent_read: defaultdict[str, set[str]] = defaultdict(set)
    parent_children: defaultdict[str, list[tuple[str, int]]] = defaultdict(list)

    themes_out: list[dict[str, object]] = []
    sorted_themes = sorted(theme_stories.items(), key=lambda item: (-len(item[1]), item[0]))
    for name, story_ids in sorted_themes:
        top_topic = theme_topics[name].most_common(1)[0][0] if theme_topics[name] else "other"
        parent_theme = tax_lookup.get(name.casefold()) or heuristic_parent_theme(name, top_topic)
        parent_stories[parent_theme].update(story_ids)
        for s_id in story_ids:
            surfaces = set(story_stats.get(s_id, {}).get("surfaces") or [])
            if "front_page" in surfaces:
                parent_front[parent_theme].add(s_id)
            if "most_read" in surfaces:
                parent_read[parent_theme].add(s_id)

        if len(story_ids) < ONTOLOGY_MIN_STORIES:
            continue
        parent_children[parent_theme].append((name, len(story_ids)))
        promo = promo_by_theme.get(name.casefold())
        promotion_payload = (
            {
                "settled_stories": int(promo["stories"]),
                "promoted_only": int(promo["promotedOnly"]),
                "read_only": int(promo["readOnly"]),
                "both": int(promo["both"]),
                "reach_rate": promo["reachRate"],
                "unpromoted_share": promo["unpromotedShare"],
                "median_lag_minutes": promo["medianLagMinutes"],
            }
            if promo is not None
            else None
        )
        themes_out.append(
            {
                "name": name,
                "parent_theme": parent_theme,
                "aliases": theme_aliases.get(name, []),
                "stories": len(story_ids),
                "front_page_stories": theme_front[name],
                "most_read_stories": theme_read[name],
                "promotion": promotion_payload,
            }
        )

    parent_themes_out: list[dict[str, object]] = []
    for parent_name, story_ids in sorted(
        parent_stories.items(), key=lambda item: (-len(item[1]), item[0])
    ):
        if len(story_ids) < ONTOLOGY_MIN_STORIES:
            continue
        promo = promo_by_parent.get(parent_name.casefold())
        promotion_payload = (
            {
                "settled_stories": int(promo["stories"]),
                "promoted_only": int(promo["promotedOnly"]),
                "read_only": int(promo["readOnly"]),
                "both": int(promo["both"]),
                "reach_rate": promo["reachRate"],
                "unpromoted_share": promo["unpromotedShare"],
                "median_lag_minutes": promo["medianLagMinutes"],
            }
            if promo is not None
            else None
        )
        children = [
            child
            for child, _ in sorted(
                parent_children[parent_name], key=lambda item: (-item[1], item[0])
            )[:8]
        ]
        parent_themes_out.append(
            {
                "name": parent_name,
                "domain": PARENT_THEME_DOMAIN.get(parent_name, "other"),
                "stories": len(story_ids),
                "front_page_stories": len(parent_front[parent_name]),
                "most_read_stories": len(parent_read[parent_name]),
                "child_themes": children,
                "promotion": promotion_payload,
            }
        )

    entities_out: list[dict[str, object]] = []
    sorted_entities = sorted(entity_stories.items(), key=lambda item: (-len(item[1]), item[0]))
    for name, story_ids in sorted_entities:
        if len(story_ids) < ONTOLOGY_MIN_STORIES:
            continue
        wd = wd_lookup.get(name.casefold(), {})
        qid = wd.get("qid") or None
        entity_entry: dict[str, object] = {
            "name": name,
            "aliases": entity_aliases.get(name, []),
            "stories": len(story_ids),
            "front_page_stories": entity_front[name],
            "most_read_stories": entity_read[name],
            "top_themes": [theme for theme, _ in entity_themes[name].most_common(4)],
        }
        if qid:
            entity_entry["qid"] = qid
            entity_entry["entity_type"] = wd.get("entity_type") or "other"
            if wd.get("description"):
                entity_entry["description"] = wd["description"]
            if wd.get("wikidata_url"):
                entity_entry["wikidata_url"] = wd["wikidata_url"]
        entities_out.append(entity_entry)
        if len(entities_out) >= ONTOLOGY_MAX_ENTITIES:
            break

    events_out = [
        {
            "cluster_id": str(event["cluster_id"]),
            "label": str(event["label"]),
            "event_type": str(event["event_type"]),
            "themes": list(event.get("themes") or []),
            "first_seen": str(event["first_seen"])[:10],
            "last_seen": str(event["last_seen"])[:10],
            "article_count": int(event["article_count"]),
        }
        for event in recurring
    ]

    return {
        "schemaVersion": 2,
        "ontologyVersion": ONTOLOGY_VERSION,
        "contracts": {
            "story_grain": (
                "One distinct BBC story URL (story_id); counts use the latest fetched "
                "labelled article version per story."
            ),
            "surface_capture_grain": (
                "front_page_captures and most_read_captures count distinct scrape runs "
                "on each surface."
            ),
            "promotion_contract": (
                f"Promotion metrics compare only paired captures holding both lists after a "
                f"{PROMOTION_SETTLE_HOURS}-hour settle window and require at least "
                f"{PROMOTION_THEME_MINIMUM} settled stories per theme."
            ),
            "taxonomy_contract": (
                "Canonical themes map many-to-one onto a controlled 46-node mid-level "
                "parent_theme hierarchy."
            ),
            "wikidata_contract": (
                "Canonical entities link to Wikidata Q-IDs, entity types, and descriptions "
                "where unambiguously resolved."
            ),
        },
        "parent_themes": parent_themes_out,
        "themes": themes_out,
        "entities": entities_out,
        "events": events_out,
    }


def _semantic_payloads(
    *,
    article_by_hash: dict[str, dict[str, object]],
    signal_by_hash: dict[str, dict[str, object]],
    embeddings: pa.Table | None,
    events: pa.Table | None,
    stories: list[dict[str, object]],
    promotion_themes: list[dict[str, object]] | None = None,
    promotion_parent_themes: list[dict[str, object]] | None = None,
    theme_aliases: dict[str, list[str]] | None = None,
    entity_aliases: dict[str, list[str]] | None = None,
    taxonomy: pa.Table | None = None,
    wikidata: pa.Table | None = None,
) -> tuple[dict[str, object], bytes, dict[str, object]]:
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
            theme_counts = Counter(
                theme for row in rows for theme in normalize_themes(row.get("themes") or [])
            )
            recurring.append(
                {
                    "cluster_id": cluster_id,
                    "label": Counter(str(row["cluster_label"]) for row in rows).most_common(1)[0][
                        0
                    ],
                    "event_type": Counter(
                        normalize_event_type(row.get("event_type")) for row in rows
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

    story_cluster_index: dict[str, int] = {}
    story_cluster_id: dict[str, str] = {}
    for cluster_idx, cluster in enumerate(recurring):
        cluster_id_str = str(cluster["cluster_id"])
        for article_entry in cluster.get("articles") or []:
            s_id = str(article_entry["story_id"])
            story_cluster_index[s_id] = cluster_idx
            story_cluster_id[s_id] = cluster_id_str

    findings = _semantic_findings(article_by_hash, signal_by_hash, stories, recurring)
    documents, vector_bytes = _semantic_index(
        article_by_hash,
        signal_by_hash,
        embeddings,
        story_stats,
        story_cluster_index,
        story_cluster_id,
    )
    ontology = _semantic_ontology(
        article_by_hash=article_by_hash,
        signal_by_hash=signal_by_hash,
        story_stats=story_stats,
        recurring=recurring,
        promotion_themes=promotion_themes or [],
        promotion_parent_themes=promotion_parent_themes or [],
        theme_aliases=theme_aliases or {},
        entity_aliases=entity_aliases or {},
        taxonomy_by_theme=_taxonomy_lookup(taxonomy),
        wikidata_by_entity=_wikidata_lookup(wikidata),
    )
    article_count = len(article_by_hash)
    signal_count = len(article_by_hash.keys() & signal_by_hash.keys())
    coverage = round(signal_count / article_count * 100, 1) if article_count else 0.0
    wikidata_linked_count = sum(1 for item in ontology["entities"] if item.get("qid"))
    return (
        {
            "semantic-trends.json": trends,
            "recurring-events.json": recurring,
            "semantic-findings.json": findings,
            "semantic-documents.json": documents,
            "semantic-ontology.json": ontology,
        },
        vector_bytes,
        {
            "signalCount": signal_count,
            "articleVersionCount": article_count,
            "coveragePercent": coverage,
            "recurringEventCount": len(recurring),
            "searchDocumentCount": documents["documentCount"],
            "embeddingModel": documents["model"],
            "ontologyVersion": ONTOLOGY_VERSION,
            "ontologyParentThemeCount": len(ontology["parent_themes"]),
            "ontologyThemeCount": len(ontology["themes"]),
            "ontologyEntityCount": len(ontology["entities"]),
            "ontologyWikidataLinkedCount": wikidata_linked_count,
        },
    )


# A story first seen this recently may still reach the other list, so it is not classified yet.
PROMOTION_SETTLE_HOURS = 24
PROMOTION_THEME_MINIMUM = 30
PROMOTION_EXAMPLES = 10
# Signed minutes from first front-page capture to first Most Read capture. Captures are about an
# hour apart at best, so the buckets are no finer than that.
LAG_BUCKETS = (
    ("read_first", "In Most Read before the front page", None, -1),
    ("same_capture", "Same capture", 0, 0),
    ("within_3_hours", "Within 3 hours", 1, 180),
    ("within_12_hours", "3 to 12 hours later", 181, 720),
    ("later", "More than 12 hours later", 721, None),
)


def _share(part: int, whole: int) -> float | None:
    return round(part / whole * 100, 1) if whole else None


def _outcome_counts(rows: list[dict[str, object]]) -> dict[str, object]:
    counts = Counter(str(row["outcome"]) for row in rows)
    promoted_only, read_only, both = counts["promoted_only"], counts["read_only"], counts["both"]
    return {
        "stories": len(rows),
        "promotedOnly": promoted_only,
        "readOnly": read_only,
        "both": both,
        # Of the stories promoted on the front page, the share that also reached Most Read.
        "reachRate": _share(both, both + promoted_only),
        # Of the stories in Most Read, the share never captured on the front page.
        "unpromotedShare": _share(read_only, read_only + both),
    }


def _promotion_attention(
    con: duckdb.DuckDBPyConnection,
    story_themes: dict[str, list[str]],
    taxonomy_by_theme: dict[str, str] | None = None,
) -> dict[str, object]:
    """Compare front-page promotion with Most Read attention, story by story."""

    # A capture that recorded only one list cannot show whether a story was on the other, so
    # only captures holding both lists are compared.
    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW paired AS
        SELECT * FROM observations
        WHERE scrape_id IN (
            SELECT scrape_id FROM observations
            GROUP BY scrape_id
            HAVING count(DISTINCT surface) = 2
        )
        """
    )
    single_surface = con.execute(
        "SELECT count(DISTINCT scrape_id) FROM observations "
        "WHERE scrape_id NOT IN (SELECT scrape_id FROM paired)"
    ).fetchone()[0]
    rows = _records(
        con.execute(
            f"""
            WITH by_story AS (
                SELECT
                    story_id,
                    arg_max(title, observed_at) AS title,
                    arg_max(url, observed_at) AS url,
                    min(observed_at) AS first_seen,
                    min(observed_at) FILTER (WHERE surface = 'front_page') AS first_front_page,
                    min(observed_at) FILTER (WHERE surface = 'most_read') AS first_most_read,
                    count(DISTINCT scrape_id) FILTER (WHERE surface = 'front_page')
                        AS front_page_captures,
                    count(DISTINCT scrape_id) FILTER (WHERE surface = 'most_read')
                        AS most_read_captures,
                    min(position) FILTER (WHERE surface = 'front_page') AS best_front_page,
                    min(position) FILTER (WHERE surface = 'most_read') AS best_most_read
                FROM paired
                GROUP BY story_id
            )
            SELECT
                *,
                CASE
                    WHEN first_most_read IS NULL THEN 'promoted_only'
                    WHEN first_front_page IS NULL THEN 'read_only'
                    ELSE 'both'
                END AS outcome,
                date_diff('minute', first_front_page, first_most_read) AS lag_minutes,
                strftime(first_seen, '%Y-%m') AS month
            FROM by_story
            WHERE first_seen <= (SELECT max(observed_at) FROM paired)
                - INTERVAL {PROMOTION_SETTLE_HOURS} HOUR
            """
        )
    )
    unsettled = con.execute(
        f"""
        SELECT count(*) FROM (
            SELECT story_id FROM paired GROUP BY story_id
            HAVING min(observed_at) > (SELECT max(observed_at) FROM paired)
                - INTERVAL {PROMOTION_SETTLE_HOURS} HOUR
        )
        """
    ).fetchone()[0]
    captures_by_month = dict(
        con.execute(
            "SELECT strftime(observed_at, '%Y-%m'), count(DISTINCT scrape_id) "
            "FROM paired GROUP BY 1"
        ).fetchall()
    )

    both = [row for row in rows if row["outcome"] == "both"]
    lag = []
    for key, label, low, high in LAG_BUCKETS:
        count = sum(
            1
            for row in both
            if (low is None or row["lag_minutes"] >= low)
            and (high is None or row["lag_minutes"] <= high)
        )
        lag.append(
            {"bucket": key, "label": label, "count": count, "share": _share(count, len(both))}
        )

    tax_lookup = taxonomy_by_theme or {}
    by_theme: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    by_parent: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        s_themes = set(story_themes.get(str(row["story_id"]), ()))
        s_parents: set[str] = set()
        for theme in s_themes:
            by_theme[theme].append(row)
            parent = tax_lookup.get(theme.casefold()) or heuristic_parent_theme(theme)
            s_parents.add(parent)
        for parent in s_parents:
            by_parent[parent].append(row)
    themes = []
    for theme, theme_rows in by_theme.items():
        if len(theme_rows) < PROMOTION_THEME_MINIMUM:
            continue
        promoted_first = [
            int(row["lag_minutes"])
            for row in theme_rows
            if row["outcome"] == "both" and int(row["lag_minutes"]) > 0
        ]
        themes.append(
            {
                "theme": theme,
                **_outcome_counts(theme_rows),
                "promotedFirst": len(promoted_first),
                "medianLagMinutes": statistics.median(promoted_first) if promoted_first else None,
            }
        )
    themes.sort(key=lambda row: (-int(row["stories"]), str(row["theme"])))

    parent_themes = []
    for parent, parent_rows in by_parent.items():
        if len(parent_rows) < PROMOTION_THEME_MINIMUM:
            continue
        promoted_first = [
            int(row["lag_minutes"])
            for row in parent_rows
            if row["outcome"] == "both" and int(row["lag_minutes"]) > 0
        ]
        parent_themes.append(
            {
                "parent_theme": parent,
                **_outcome_counts(parent_rows),
                "promotedFirst": len(promoted_first),
                "medianLagMinutes": statistics.median(promoted_first) if promoted_first else None,
            }
        )
    parent_themes.sort(key=lambda row: (-int(row["stories"]), str(row["parent_theme"])))

    by_month: defaultdict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_month[str(row["month"])].append(row)
    months = [
        {
            "month": month,
            "captures": int(captures_by_month.get(month, 0)),
            **_outcome_counts(month_rows),
        }
        for month, month_rows in sorted(by_month.items())
    ]

    def example(row: dict[str, object], surface: str) -> dict[str, object]:
        return {
            "storyId": row["story_id"],
            "title": row["title"],
            "url": row["url"],
            "firstSeen": row["first_seen"],
            "captures": row[f"{surface}_captures"],
            "bestPosition": row[f"best_{surface}"],
        }

    def strongest(outcome: str, surface: str) -> list[dict[str, object]]:
        candidates = [row for row in rows if row["outcome"] == outcome]
        candidates.sort(
            key=lambda row: (
                -int(row[f"{surface}_captures"]),
                int(row[f"best_{surface}"]),
                str(row["first_seen"]),
            )
        )
        return [example(row, surface) for row in candidates[:PROMOTION_EXAMPLES]]

    return {
        "schemaVersion": 1,
        "settleHours": PROMOTION_SETTLE_HOURS,
        "firstSeen": min((str(row["first_seen"]) for row in rows), default=None),
        "lastSeen": max((str(row["first_seen"]) for row in rows), default=None),
        "unsettledStories": int(unsettled),
        "singleSurfaceCaptures": int(single_surface),
        "totals": _outcome_counts(rows),
        "labelledStories": sum(1 for row in rows if str(row["story_id"]) in story_themes),
        "lag": lag,
        "themes": themes,
        "parentThemes": parent_themes,
        "months": months,
        "examples": {
            "readOnly": strongest("read_only", "most_read"),
            "promotedOnly": strongest("promoted_only", "front_page"),
        },
    }


def build_marts(
    table: pa.Table,
    output_dir: Path,
    *,
    articles: pa.Table | None = None,
    signals: pa.Table | None = None,
    embeddings: pa.Table | None = None,
    events: pa.Table | None = None,
    weekly: pa.Table | None = None,
    taxonomy: pa.Table | None = None,
    wikidata: pa.Table | None = None,
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
                count(DISTINCT scrape_id) FILTER (WHERE surface = 'front_page')
                    AS front_page_captures,
                count(DISTINCT scrape_id) FILTER (WHERE surface = 'most_read')
                    AS most_read_captures,
                min(position) FILTER (WHERE surface = 'front_page') AS best_front_page,
                min(position) FILTER (WHERE surface = 'most_read') AS best_most_read,
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
    article_by_hash, signal_by_hash, theme_aliases, entity_aliases = _labelled_articles(
        articles, signals
    )
    tax_lookup = _taxonomy_lookup(taxonomy)
    promotion_attention = _promotion_attention(
        con, _story_themes(article_by_hash, signal_by_hash), tax_lookup
    )
    semantic_payloads, semantic_vectors, semantic_manifest = _semantic_payloads(
        article_by_hash=article_by_hash,
        signal_by_hash=signal_by_hash,
        embeddings=embeddings,
        events=events,
        stories=stories,
        promotion_themes=list(promotion_attention.get("themes") or []),  # type: ignore[arg-type]
        promotion_parent_themes=list(promotion_attention.get("parentThemes") or []),  # type: ignore[arg-type]
        theme_aliases=theme_aliases,
        entity_aliases=entity_aliases,
        taxonomy=taxonomy,
        wikidata=wikidata,
    )
    payloads = {
        "stories.json": stories,
        "rank-series.json": rank_series,
        "daily.json": daily,
        "promotion-attention.json": promotion_attention,
        "weekly-summaries.json": _latest_week(weekly),
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
            weekly=tables["semantic/weekly"],
            taxonomy=tables.get("semantic/taxonomy"),
            wikidata=tables.get("semantic/wikidata"),
        )
