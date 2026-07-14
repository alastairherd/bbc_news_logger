"""Compact append-only Hugging Face Parquet shards into fast cold-start bases."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi, snapshot_download

from .config import DEFAULT_DATASET_ID

COMPACTABLE_PREFIXES = (
    "data/observations",
    "data/article_snapshots",
    "semantic/signals",
    "semantic/embeddings",
)
LATEST_BY_HASH = {
    "semantic/signals": "generated_at",
    "semantic/embeddings": "generated_at",
}


def compact_path(prefix: str) -> str:
    """Return the canonical base path for a dataset family."""

    return f"compacted/{prefix.replace('/', '--')}.parquet"


def download_patterns(prefixes: Sequence[str]) -> list[str]:
    """Include compact bases and any incremental shards written after compaction."""

    return [
        pattern
        for prefix in prefixes
        for pattern in (compact_path(prefix), f"{prefix}/*.parquet", f"{prefix}/**/*.parquet")
    ]


def parquet_files(snapshot: Path, prefix: str) -> list[Path]:
    files = sorted((snapshot / prefix).rglob("*.parquet"))
    base = snapshot / compact_path(prefix)
    return ([base] if base.exists() else []) + files


def _latest_by_hash(table: pa.Table, timestamp: str) -> pa.Table:
    rows: dict[str, dict[str, Any]] = {}
    for row in table.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        existing = rows.get(content_hash)
        if content_hash and (existing is None or row[timestamp] > existing[timestamp]):
            rows[content_hash] = row
    return pa.Table.from_pylist(list(rows.values()), schema=table.schema)


def compact_table(prefix: str, files: Sequence[Path]) -> pa.Table:
    if not files:
        raise FileNotFoundError(f"No Parquet shards found for {prefix}")
    table = pa.concat_tables(
        [pq.ParquetFile(path).read() for path in files], promote_options="default"
    )
    timestamp = LATEST_BY_HASH.get(prefix)
    return _latest_by_hash(table, timestamp) if timestamp else table


def compact_remote_dataset(
    output: Path,
    *,
    dataset_id: str = DEFAULT_DATASET_ID,
    token: str | None = None,
    publish: bool = False,
) -> dict[str, dict[str, int]]:
    """Build compact bases and optionally replace the source shards atomically."""

    token = token or os.getenv("HF_TOKEN")
    snapshot = Path(
        snapshot_download(
            repo_id=dataset_id,
            repo_type="dataset",
            allow_patterns=download_patterns(COMPACTABLE_PREFIXES),
            token=token,
            max_workers=8,
        )
    )
    output.mkdir(parents=True, exist_ok=True)
    report: dict[str, dict[str, int]] = {}
    local_files: dict[str, Path] = {}
    for prefix in COMPACTABLE_PREFIXES:
        files = parquet_files(snapshot, prefix)
        table = compact_table(prefix, files)
        local = output / Path(compact_path(prefix)).name
        pq.write_table(table, local, compression="zstd")
        local_files[prefix] = local
        report[prefix] = {
            "source_files": len(files),
            "rows": table.num_rows,
            "bytes": local.stat().st_size,
        }

    if publish:
        operations = [
            CommitOperationAdd(
                path_in_repo=compact_path(prefix), path_or_fileobj=local_files[prefix]
            )
            for prefix in COMPACTABLE_PREFIXES
        ]
        operations.extend(
            CommitOperationDelete(path_in_repo=prefix, is_folder=True)
            for prefix in COMPACTABLE_PREFIXES
        )
        HfApi(token=token).create_commit(
            repo_id=dataset_id,
            repo_type="dataset",
            operations=operations,
            commit_message="Compact dashboard dataset shards",
            parent_commit=snapshot.name,
        )
    return report


def temporary_output() -> Path:
    return Path(tempfile.mkdtemp(prefix="bbc-news-compaction-"))
