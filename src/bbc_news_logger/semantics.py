"""Checkpointed semantic storage and BGE embedding workers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import tempfile
import time
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, snapshot_download
from huggingface_hub.errors import HfHubHTTPError

from .config import DEFAULT_DATASET_ID
from .deepseek import DEEPSEEK_MODEL, PROMPT_VERSION, DeepSeekBatchResult
from .storage import write_parquet

EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
EMBEDDING_MODEL_REVISION = "main"
EMBEDDING_INPUT_VERSION = "headline-body-lead-v1"
EMBEDDING_DIMENSIONS = 384
EMBEDDING_TEXT_CHARACTERS = 4_000
HF_UPLOAD_MAX_ATTEMPTS = 2
HF_COMMIT_RATE_LIMIT_DELAY_SECONDS = 3_600

SIGNAL_PREFIX = "semantic/signals"
EMBEDDING_PREFIX = "semantic/embeddings"

SIGNAL_SCHEMA = pa.schema(
    [
        pa.field("content_sha256", pa.string(), nullable=False),
        pa.field("model", pa.string(), nullable=False),
        pa.field("prompt_version", pa.string(), nullable=False),
        pa.field("topic", pa.string(), nullable=False),
        pa.field("themes", pa.list_(pa.string()), nullable=False),
        pa.field("summary", pa.string(), nullable=False),
        pa.field("named_entities", pa.list_(pa.string()), nullable=False),
        pa.field("event_label", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("story_form", pa.string(), nullable=False),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("deepseek_response_id", pa.string(), nullable=False),
        pa.field("prompt_tokens", pa.int64(), nullable=False),
        pa.field("prompt_cache_hit_tokens", pa.int64(), nullable=False),
        pa.field("prompt_cache_miss_tokens", pa.int64(), nullable=False),
        pa.field("completion_tokens", pa.int64(), nullable=False),
        pa.field("request_cost_usd", pa.float64(), nullable=False),
        pa.field("batch_size", pa.int16(), nullable=False),
    ]
)

EMBEDDING_SCHEMA = pa.schema(
    [
        pa.field("content_sha256", pa.string(), nullable=False),
        pa.field("model", pa.string(), nullable=False),
        pa.field("model_revision", pa.string(), nullable=False),
        pa.field("input_version", pa.string(), nullable=False),
        pa.field("embedding", pa.list_(pa.float32(), EMBEDDING_DIMENSIONS), nullable=False),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


class TextEmbedder(Protocol):
    def embed(self, documents: Sequence[str], **kwargs: Any) -> Iterable[Any]: ...


@dataclass(frozen=True)
class EmbeddingReport:
    model: str
    candidates: int
    rows_added: int
    shards_published: int
    remaining: int


def _parquet_files(snapshot: Path, prefix: str) -> list[Path]:
    return sorted((snapshot / prefix).rglob("*.parquet"))


def download_dataset_tables(
    prefixes: Sequence[str],
    *,
    dataset_id: str = DEFAULT_DATASET_ID,
    token: str | None = None,
) -> dict[str, pa.Table | None]:
    """Download selected dataset prefixes once and read their Parquet shards."""

    snapshot = Path(
        snapshot_download(
            repo_id=dataset_id,
            repo_type="dataset",
            allow_patterns=[f"{prefix}/**/*.parquet" for prefix in prefixes],
            token=token or os.getenv("HF_TOKEN"),
            max_workers=8,
        )
    )
    result: dict[str, pa.Table | None] = {}
    for prefix in prefixes:
        files = _parquet_files(snapshot, prefix)
        result[prefix] = (
            pa.concat_tables(
                [pq.ParquetFile(path).read() for path in files], promote_options="default"
            )
            if files
            else None
        )
    return result


def unique_article_rows(table: pa.Table) -> list[dict[str, Any]]:
    """Return the newest successful snapshot for every distinct content version."""

    by_hash: dict[str, dict[str, Any]] = {}
    for row in table.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        if not content_hash or not row.get("fetch_ok"):
            continue
        existing = by_hash.get(content_hash)
        if existing is None or row["fetched_at"] > existing["fetched_at"]:
            by_hash[content_hash] = row
    return sorted(by_hash.values(), key=lambda row: (row["fetched_at"], row["content_sha256"]))


def embedding_text(article: dict[str, Any]) -> str:
    """Create a stable short document suited to BGE Small's context window."""

    title = str(article.get("title") or "").strip()
    body = " ".join(str(article.get("article_text") or "").split())
    if body.startswith(title):
        body = body[len(title) :].lstrip(" .:-")
    return f"{title}\n\n{body[:EMBEDDING_TEXT_CHARACTERS]}".strip()


def completed_hashes(
    table: pa.Table | None, *, model: str, version_field: str, version: str
) -> set[str]:
    if table is None:
        return set()
    return {
        str(row["content_sha256"])
        for row in table.to_pylist()
        if row.get("model") == model and row.get(version_field) == version
    }


def _split_integer(total: int, count: int) -> list[int]:
    quotient, remainder = divmod(total, count)
    return [quotient + (1 if index < remainder else 0) for index in range(count)]


def signal_rows_from_batch(result: DeepSeekBatchResult) -> list[dict[str, Any]]:
    """Allocate request-level usage across rows without inflating aggregate cost."""

    count = len(result.signals)
    usage = result.usage
    prompt = _split_integer(usage.prompt_tokens, count)
    cache_hit = _split_integer(usage.prompt_cache_hit_tokens, count)
    cache_miss = _split_integer(usage.prompt_cache_miss_tokens, count)
    completion = _split_integer(usage.completion_tokens, count)
    cost = float(usage.cost_usd) / count
    generated_at = datetime.now(timezone.utc)
    rows = []
    for index, (content_hash, signals) in enumerate(result.signals):
        rows.append(
            {
                "content_sha256": content_hash,
                "model": DEEPSEEK_MODEL,
                "prompt_version": PROMPT_VERSION,
                **asdict(signals),
                "themes": list(signals.themes),
                "named_entities": list(signals.named_entities),
                "generated_at": generated_at,
                "deepseek_response_id": result.response_id,
                "prompt_tokens": prompt[index],
                "prompt_cache_hit_tokens": cache_hit[index],
                "prompt_cache_miss_tokens": cache_miss[index],
                "completion_tokens": completion[index],
                "request_cost_usd": cost,
                "batch_size": count,
            }
        )
    return rows


def shard_path(prefix: str, rows: Sequence[dict[str, Any]]) -> str:
    identities = "\n".join(
        sorted(
            ":".join(
                str(row.get(field) or "")
                for field in (
                    "content_sha256",
                    "model",
                    "prompt_version",
                    "model_revision",
                    "input_version",
                )
            )
            for row in rows
        )
    )
    digest = hashlib.sha256(identities.encode()).hexdigest()[:16]
    generated = rows[0].get("generated_at") if rows else None
    if not isinstance(generated, datetime):
        generated = datetime.now(timezone.utc)
    generated = generated.astimezone(timezone.utc)
    stamp = generated.strftime("%Y-%m-%dT%H%M%S%fZ")
    return (
        f"{prefix}/year={generated:%Y}/month={generated:%m}/"
        f"{stamp}-{digest}.parquet"
    )


def publish_shard(
    table: pa.Table,
    *,
    prefix: str,
    dataset_id: str = DEFAULT_DATASET_ID,
    token: str | None = None,
    message: str,
) -> str:
    rows = table.select(["content_sha256"]).to_pylist()
    path = shard_path(prefix, rows)
    with tempfile.TemporaryDirectory(prefix="bbc-news-semantic-shard-") as tmp:
        local = write_parquet(table, Path(tmp) / "shard.parquet")
        api = HfApi(token=token or os.getenv("HF_TOKEN"))
        for attempt in range(1, HF_UPLOAD_MAX_ATTEMPTS + 1):
            try:
                api.upload_file(
                    repo_id=dataset_id,
                    repo_type="dataset",
                    path_or_fileobj=local,
                    path_in_repo=path,
                    commit_message=message,
                )
                break
            except HfHubHTTPError as exc:
                response = exc.response
                if (
                    response is None
                    or response.status_code != 429
                    or attempt == HF_UPLOAD_MAX_ATTEMPTS
                ):
                    raise
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = max(1, int(float(retry_after or 0)))
                except ValueError:
                    delay = 60
                if "repository commits" in str(exc).lower():
                    delay = max(delay, HF_COMMIT_RATE_LIMIT_DELAY_SECONDS)
                print(
                    json.dumps(
                        {
                            "event": "hf_upload_retry",
                            "attempt": attempt,
                            "delay_seconds": delay,
                            "path": path,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                time.sleep(delay)
    return path


def take_ready_shard(
    rows: list[dict[str, Any]], *, shard_size: int, force: bool
) -> list[dict[str, Any]]:
    """Remove one publishable shard while retaining a smaller in-memory buffer."""
    if len(rows) < shard_size and not (force and rows):
        return []
    count = min(len(rows), shard_size)
    shard = rows[:count]
    del rows[:count]
    return shard


class SemanticCheckpoint:
    """Synchronous SQLite WAL checkpoint written before another paid request starts."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_results (
                content_sha256 TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                generated_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS failures (
                content_sha256 TEXT PRIMARY KEY,
                error TEXT NOT NULL,
                failed_at TEXT NOT NULL
            )
            """
        )
        self.connection.commit()

    def completed_hashes(self) -> set[str]:
        rows = self.connection.execute("SELECT content_sha256 FROM signal_results")
        return {str(row[0]) for row in rows}

    def failed_hashes(self) -> set[str]:
        rows = self.connection.execute("SELECT content_sha256 FROM failures")
        return {str(row[0]) for row in rows}

    def rows(self) -> list[dict[str, Any]]:
        records = []
        for (payload,) in self.connection.execute(
            "SELECT payload FROM signal_results ORDER BY generated_at, content_sha256"
        ):
            row = json.loads(str(payload))
            row["generated_at"] = datetime.fromisoformat(row["generated_at"])
            records.append(row)
        return records

    def record_rows(self, rows: Sequence[dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR REPLACE INTO signal_results (content_sha256, payload, generated_at)
                VALUES (?, ?, ?)
                """,
                [
                    (
                        row["content_sha256"],
                        json.dumps(row, default=str, separators=(",", ":")),
                        now,
                    )
                    for row in rows
                ],
            )
            self.connection.executemany(
                "DELETE FROM failures WHERE content_sha256 = ?",
                [(row["content_sha256"],) for row in rows],
            )

    def record_failure(self, content_hashes: Sequence[str], error: BaseException) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connection:
            self.connection.executemany(
                """
                INSERT OR REPLACE INTO failures (content_sha256, error, failed_at)
                VALUES (?, ?, ?)
                """,
                [(content_hash, str(error)[:2_000], now) for content_hash in content_hashes],
            )

    def close(self) -> None:
        self.connection.close()


def _default_embedder() -> TextEmbedder:
    try:
        from fastembed import TextEmbedding
    except ImportError as exc:  # pragma: no cover - exercised in deployed semantic environments
        raise RuntimeError("Install the semantic extra to run BGE embeddings") from exc
    return TextEmbedding(
        model_name=EMBEDDING_MODEL,
        providers=["CPUExecutionProvider"],
        threads=max(1, min(4, os.cpu_count() or 1)),
    )


def run_embedding_refresh(
    *,
    dataset_id: str = DEFAULT_DATASET_ID,
    limit: int = 0,
    batch_size: int = 128,
    publish: bool = False,
    embedder: TextEmbedder | None = None,
    output_dir: Path = Path("dist/embedding-shards"),
) -> EmbeddingReport:
    if not 1 <= batch_size <= 512:
        raise ValueError("Embedding batch size must be between 1 and 512")
    tables = download_dataset_tables(
        ["data/article_snapshots", EMBEDDING_PREFIX], dataset_id=dataset_id
    )
    articles_table = tables["data/article_snapshots"]
    if articles_table is None:
        raise FileNotFoundError(f"No article snapshots found in {dataset_id}")
    articles = unique_article_rows(articles_table)
    done = completed_hashes(
        tables[EMBEDDING_PREFIX],
        model=EMBEDDING_MODEL,
        version_field="input_version",
        version=EMBEDDING_INPUT_VERSION,
    )
    candidates = [row for row in articles if row["content_sha256"] not in done]
    selected = candidates[:limit] if limit > 0 else candidates
    print(
        json.dumps(
            {
                "event": "embedding_start",
                "model": EMBEDDING_MODEL,
                "completed": len(done),
                "candidates": len(candidates),
                "selected": len(selected),
                "batch_size": batch_size,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    model = embedder or _default_embedder()
    added = 0
    shards = 0
    output_dir.mkdir(parents=True, exist_ok=True)
    for start in range(0, len(selected), batch_size):
        batch = selected[start : start + batch_size]
        vectors = list(model.embed([embedding_text(row) for row in batch], batch_size=batch_size))
        if len(vectors) != len(batch):
            raise RuntimeError("BGE returned a different number of embeddings than requested")
        generated_at = datetime.now(timezone.utc)
        rows = []
        for row, vector in zip(batch, vectors, strict=True):
            values = [float(value) for value in vector]
            if len(values) != EMBEDDING_DIMENSIONS:
                raise RuntimeError(
                    f"BGE returned {len(values)} dimensions; expected {EMBEDDING_DIMENSIONS}"
                )
            norm = math.sqrt(sum(value * value for value in values))
            if norm == 0:
                raise RuntimeError("BGE returned a zero vector")
            rows.append({
                "content_sha256": row["content_sha256"],
                "model": EMBEDDING_MODEL,
                "model_revision": EMBEDDING_MODEL_REVISION,
                "input_version": EMBEDDING_INPUT_VERSION,
                "embedding": [value / norm for value in values],
                "generated_at": generated_at,
            })
        table = pa.Table.from_pylist(rows, schema=EMBEDDING_SCHEMA)
        local_path = output_dir / Path(shard_path(EMBEDDING_PREFIX, rows)).name
        write_parquet(table, local_path)
        published_path = None
        if publish:
            published_path = publish_shard(
                table,
                prefix=EMBEDDING_PREFIX,
                dataset_id=dataset_id,
                message=f"Add {len(rows)} BGE Small article embeddings",
            )
            shards += 1
        added += len(rows)
        print(
            json.dumps(
                {
                    "event": "embedding_checkpoint",
                    "rows_in_shard": len(rows),
                    "rows_added": added,
                    "selected": len(selected),
                    "remaining_after_run": max(0, len(candidates) - added),
                    "path": published_path or str(local_path),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    return EmbeddingReport(
        model=EMBEDDING_MODEL,
        candidates=len(candidates),
        rows_added=added,
        shards_published=shards,
        remaining=max(0, len(candidates) - len(selected)),
    )
