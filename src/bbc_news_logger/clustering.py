"""Deterministic recurring-story clustering from BGE vectors and semantic signals."""

from __future__ import annotations

import hashlib
import os
import re
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
from huggingface_hub import HfApi

from .config import DEFAULT_DATASET_ID
from .semantics import EMBEDDING_PREFIX, SIGNAL_PREFIX, download_dataset_tables
from .storage import write_parquet

EVENT_PATH = "semantic/events/latest.parquet"
EVENT_SCHEMA = pa.schema(
    [
        pa.field("cluster_id", pa.string(), nullable=False),
        pa.field("cluster_label", pa.string(), nullable=False),
        pa.field("cluster_size", pa.int32(), nullable=False),
        pa.field("content_sha256", pa.string(), nullable=False),
        pa.field("story_id", pa.string(), nullable=False),
        pa.field("canonical_url", pa.string(), nullable=False),
        pa.field("title", pa.string(), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("topic", pa.string(), nullable=False),
        pa.field("themes", pa.list_(pa.string()), nullable=False),
        pa.field("event_label", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("named_entities", pa.list_(pa.string()), nullable=False),
        pa.field("similarity_to_anchor", pa.float32(), nullable=False),
    ]
)

TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
GENERIC_ENTITIES = {"bbc", "bbc news", "uk", "united kingdom", "government"}


class _UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def _tokens(value: str) -> set[str]:
    return set(TOKEN_PATTERN.findall(value.casefold()))


def _label_overlap(left: str, right: str) -> float:
    left_tokens, right_tokens = _tokens(left), _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _entities(row: dict[str, Any]) -> set[str]:
    return {
        str(value).strip().casefold()
        for value in row.get("named_entities") or []
        if str(value).strip().casefold() not in GENERIC_ENTITIES
    }


def _latest_by_hash(table: pa.Table, timestamp: str) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in table.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        if not content_hash:
            continue
        existing = rows.get(content_hash)
        if existing is None or row[timestamp] > existing[timestamp]:
            rows[content_hash] = row
    return rows


def cluster_events(
    articles: pa.Table,
    signals: pa.Table,
    embeddings: pa.Table,
    *,
    window_days: int = 45,
    strong_similarity: float = 0.88,
    supported_similarity: float = 0.80,
) -> pa.Table:
    article_rows = _latest_by_hash(articles, "fetched_at")
    signal_rows = _latest_by_hash(signals, "generated_at")
    embedding_rows = _latest_by_hash(embeddings, "generated_at")
    hashes = sorted(
        article_rows.keys() & signal_rows.keys() & embedding_rows.keys(),
        key=lambda value: (article_rows[value]["fetched_at"], value),
    )
    if not hashes:
        return pa.Table.from_pylist([], schema=EVENT_SCHEMA)

    matrix = np.asarray([embedding_rows[value]["embedding"] for value in hashes], dtype=np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    matrix = matrix / np.maximum(norms, np.finfo(np.float32).eps)
    union = _UnionFind(len(hashes))
    window = timedelta(days=window_days)
    window_start = 0
    for index, content_hash in enumerate(hashes):
        fetched_at = article_rows[content_hash]["fetched_at"]
        while (
            window_start < index
            and article_rows[hashes[window_start]]["fetched_at"] < fetched_at - window
        ):
            window_start += 1
        if window_start == index:
            continue
        similarities = matrix[window_start:index] @ matrix[index]
        current_signal = signal_rows[content_hash]
        current_entities = _entities(current_signal)
        for offset in np.flatnonzero(similarities >= supported_similarity):
            candidate_index = window_start + int(offset)
            similarity = float(similarities[offset])
            candidate_signal = signal_rows[hashes[candidate_index]]
            supported = bool(current_entities & _entities(candidate_signal)) or _label_overlap(
                str(current_signal["event_label"]), str(candidate_signal["event_label"])
            ) >= 0.5
            if similarity >= strong_similarity or supported:
                union.union(index, candidate_index)

    members: dict[int, list[int]] = {}
    for index in range(len(hashes)):
        members.setdefault(union.find(index), []).append(index)

    output: list[dict[str, Any]] = []
    for indices in members.values():
        anchor_index = min(indices)
        anchor_hash = hashes[anchor_index]
        cluster_id = "event-" + hashlib.sha256(anchor_hash.encode()).hexdigest()[:16]
        labels = [str(signal_rows[hashes[index]]["event_label"]) for index in indices]
        cluster_label = Counter(labels).most_common(1)[0][0]
        for index in indices:
            content_hash = hashes[index]
            article = article_rows[content_hash]
            signal = signal_rows[content_hash]
            output.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_label": cluster_label,
                    "cluster_size": len(indices),
                    "content_sha256": content_hash,
                    "story_id": str(article["story_id"]),
                    "canonical_url": str(article["canonical_url"]),
                    "title": str(article["title"]),
                    "fetched_at": article["fetched_at"],
                    "topic": str(signal["topic"]),
                    "themes": list(signal["themes"]),
                    "event_label": str(signal["event_label"]),
                    "event_type": str(signal["event_type"]),
                    "named_entities": list(signal["named_entities"]),
                    "similarity_to_anchor": float(matrix[index] @ matrix[anchor_index]),
                }
            )
    output.sort(key=lambda row: (row["cluster_id"], row["fetched_at"], row["content_sha256"]))
    return pa.Table.from_pylist(output, schema=EVENT_SCHEMA)


def build_remote_event_clusters(
    *, dataset_id: str = DEFAULT_DATASET_ID, publish: bool = False, output: Path
) -> pa.Table:
    tables = download_dataset_tables(
        ["data/article_snapshots", SIGNAL_PREFIX, EMBEDDING_PREFIX], dataset_id=dataset_id
    )
    if any(tables[prefix] is None for prefix in tables):
        missing = [prefix for prefix, table in tables.items() if table is None]
        raise FileNotFoundError(f"Cannot cluster events; missing: {', '.join(missing)}")
    result = cluster_events(
        tables["data/article_snapshots"],  # type: ignore[arg-type]
        tables[SIGNAL_PREFIX],  # type: ignore[arg-type]
        tables[EMBEDDING_PREFIX],  # type: ignore[arg-type]
    )
    write_parquet(result, output)
    if publish:
        HfApi(token=os.getenv("HF_TOKEN")).upload_file(
            repo_id=dataset_id,
            repo_type="dataset",
            path_or_fileobj=output,
            path_in_repo=EVENT_PATH,
            commit_message="Refresh recurring story clusters",
        )
    return result
