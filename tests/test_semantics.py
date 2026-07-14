from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from httpx import Request, Response
from huggingface_hub.errors import HfHubHTTPError

from bbc_news_logger.clustering import cluster_events
from bbc_news_logger.semantics import (
    EMBEDDING_DIMENSIONS,
    EMBEDDING_SCHEMA,
    SIGNAL_SCHEMA,
    SemanticCheckpoint,
    embedding_text,
    publish_shard,
    run_embedding_refresh,
    take_ready_shard,
)
from bbc_news_logger.storage import ARTICLE_SCHEMA


def _article(content_hash: str, title: str, fetched_at: datetime) -> dict[str, object]:
    return {
        "snapshot_id": f"snapshot-{content_hash}",
        "requested_url": f"https://www.bbc.co.uk/news/articles/{content_hash}",
        "canonical_url": f"https://www.bbc.co.uk/news/articles/{content_hash}",
        "story_id": f"story-{content_hash}",
        "first_observed_at": fetched_at,
        "fetched_at": fetched_at,
        "fetched_at_is_inferred": False,
        "title": title,
        "authors": [],
        "article_text": f"{title}. This is the article body.",
        "content_sha256": content_hash,
        "html_sha256": f"html-{content_hash}",
        "http_status": 200,
        "fetch_ok": True,
    }


def _signal(
    content_hash: str, label: str, entity: str, generated_at: datetime
) -> dict[str, object]:
    return {
        "content_sha256": content_hash,
        "model": "deepseek-v4-flash",
        "prompt_version": "2026-07-13-v1",
        "topic": "world",
        "themes": ["diplomacy"],
        "summary": "A summary.",
        "named_entities": [entity],
        "event_label": label,
        "event_type": "diplomacy",
        "story_form": "update",
        "generated_at": generated_at,
        "deepseek_response_id": f"response-{content_hash}",
        "prompt_tokens": 100,
        "prompt_cache_hit_tokens": 0,
        "prompt_cache_miss_tokens": 100,
        "completion_tokens": 50,
        "request_cost_usd": 0.001,
        "batch_size": 1,
    }


def _embedding(content_hash: str, axis: int, generated_at: datetime) -> dict[str, object]:
    vector = [0.0] * EMBEDDING_DIMENSIONS
    vector[axis] = 1.0
    return {
        "content_sha256": content_hash,
        "model": "BAAI/bge-small-en-v1.5",
        "model_revision": "main",
        "input_version": "headline-body-lead-v1",
        "embedding": vector,
        "generated_at": generated_at,
    }


def _embedding_vector(
    content_hash: str, first: float, second: float, generated_at: datetime
) -> dict[str, object]:
    row = _embedding(content_hash, 0, generated_at)
    row["embedding"] = [first, second, *([0.0] * (EMBEDDING_DIMENSIONS - 2))]
    return row


def test_embedding_text_is_stable_and_does_not_repeat_title() -> None:
    value = embedding_text({"title": "Headline", "article_text": "Headline. Body text"})
    assert value == "Headline\n\nBody text"


def test_sqlite_checkpoint_survives_reopen(tmp_path) -> None:
    path = tmp_path / "semantic.sqlite3"
    row = _signal("hash-a", "Talks resume", "Example State", datetime.now(timezone.utc))
    checkpoint = SemanticCheckpoint(path)
    checkpoint.record_rows([row])
    checkpoint.close()

    restored = SemanticCheckpoint(path)
    assert restored.completed_hashes() == {"hash-a"}
    assert restored.rows()[0]["event_label"] == "Talks resume"
    restored.close()


def test_publish_shard_waits_for_hugging_face_commit_rate_limit(
    monkeypatch, capsys
) -> None:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    table = pa.Table.from_pylist([_signal("hash-a", "Talks", "State", start)], SIGNAL_SCHEMA)
    request = Request("POST", "https://huggingface.co/api/datasets/example/commit/main")
    response = Response(429, headers={"Retry-After": "68"}, request=request)
    error = HfHubHTTPError(
        "You have exceeded the rate limit for repository commits (128 per hour).",
        response=response,
    )

    class FakeApi:
        attempts = 0

        def upload_file(self, **_kwargs):
            self.attempts += 1
            if self.attempts == 1:
                raise error

    api = FakeApi()
    delays: list[int] = []
    monkeypatch.setattr("bbc_news_logger.semantics.HfApi", lambda **_kwargs: api)
    monkeypatch.setattr("bbc_news_logger.semantics.time.sleep", delays.append)

    path = publish_shard(
        table,
        prefix="semantic/signals",
        dataset_id="example/dataset",
        message="test",
    )

    assert path.endswith(".parquet")
    assert api.attempts == 2
    assert delays == [3_600]
    assert '"event": "hf_upload_retry"' in capsys.readouterr().out


def test_take_ready_shard_buffers_rows_until_target_size() -> None:
    rows = [{"content_sha256": f"hash-{index}"} for index in range(300)]

    first = take_ready_shard(rows, shard_size=256, force=False)

    assert len(first) == 256
    assert len(rows) == 44
    assert take_ready_shard(rows, shard_size=256, force=False) == []

    final = take_ready_shard(rows, shard_size=256, force=True)

    assert len(final) == 44
    assert rows == []


def test_embedding_refresh_normalizes_and_reports_checkpoint(
    monkeypatch, tmp_path, capsys
) -> None:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    article_table = pa.Table.from_pylist(
        [_article("hash-a", "Talks begin", start)], schema=ARTICLE_SCHEMA
    )

    def fake_tables(*_args, **_kwargs):
        return {"data/article_snapshots": article_table, "semantic/embeddings": None}

    class FakeEmbedder:
        def embed(self, _documents, **_kwargs):
            return [[3.0, 4.0, *([0.0] * (EMBEDDING_DIMENSIONS - 2))]]

    monkeypatch.setattr("bbc_news_logger.semantics.download_dataset_tables", fake_tables)
    report = run_embedding_refresh(
        limit=0,
        batch_size=8,
        publish=False,
        embedder=FakeEmbedder(),
        output_dir=tmp_path,
    )

    assert report.rows_added == 1
    table = pq.read_table(next(tmp_path.glob("*.parquet")))
    vector = table.column("embedding")[0].as_py()
    assert vector[:2] == pytest.approx([0.6, 0.8])
    output = capsys.readouterr().out
    assert '"event": "embedding_start"' in output
    assert '"event": "embedding_checkpoint"' in output
    assert '"rows_added": 1' in output


def test_embedding_refresh_does_not_load_model_when_work_is_complete(monkeypatch, tmp_path) -> None:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    article_table = pa.Table.from_pylist(
        [_article("hash-a", "Talks begin", start)], schema=ARTICLE_SCHEMA
    )
    embedding_table = pa.Table.from_pylist(
        [_embedding("hash-a", 0, start)], schema=EMBEDDING_SCHEMA
    )

    def fake_tables(*_args, **_kwargs):
        return {
            "data/article_snapshots": article_table,
            "semantic/embeddings": embedding_table,
        }

    def fail_if_loaded():
        raise AssertionError("The embedding model must not load when there are no candidates")

    monkeypatch.setattr("bbc_news_logger.semantics.download_dataset_tables", fake_tables)
    monkeypatch.setattr("bbc_news_logger.semantics._default_embedder", fail_if_loaded)

    report = run_embedding_refresh(output_dir=tmp_path)

    assert report.candidates == 0
    assert report.rows_added == 0
    assert report.remaining == 0
    assert list(tmp_path.iterdir()) == []


def test_clustering_joins_same_event_but_not_nearby_unrelated_story() -> None:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    articles = pa.Table.from_pylist(
        [
            _article("hash-a", "Talks begin", start),
            _article("hash-b", "Talks continue", start + timedelta(days=1)),
            _article("hash-c", "Separate court case", start + timedelta(days=2)),
        ],
        schema=ARTICLE_SCHEMA,
    )
    signals = pa.Table.from_pylist(
        [
            _signal("hash-a", "Example peace talks", "Example State", start),
            _signal("hash-b", "Example peace talks resume", "Example State", start),
            _signal("hash-c", "Example court ruling", "Example Court", start),
        ],
        schema=SIGNAL_SCHEMA,
    )
    embeddings = pa.Table.from_pylist(
        [
            _embedding("hash-a", 0, start),
            _embedding("hash-b", 0, start),
            _embedding("hash-c", 1, start),
        ],
        schema=EMBEDDING_SCHEMA,
    )

    result = cluster_events(articles, signals, embeddings).to_pylist()
    sizes = {row["content_sha256"]: row["cluster_size"] for row in result}
    assert sizes == {"hash-a": 2, "hash-b": 2, "hash-c": 1}


def test_clustering_does_not_merge_a_transitive_similarity_chain() -> None:
    start = datetime(2026, 7, 1, tzinfo=timezone.utc)
    articles = pa.Table.from_pylist(
        [
            _article("hash-a", "Talks begin", start),
            _article("hash-b", "Talks continue", start + timedelta(days=1)),
            _article("hash-c", "Talks enter a different phase", start + timedelta(days=2)),
        ],
        schema=ARTICLE_SCHEMA,
    )
    signals = pa.Table.from_pylist(
        [
            _signal("hash-a", "Example peace talks", "Example State", start),
            _signal("hash-b", "Example peace talks resume", "Example State", start),
            _signal("hash-c", "Example peace talks update", "Example State", start),
        ],
        schema=SIGNAL_SCHEMA,
    )
    embeddings = pa.Table.from_pylist(
        [
            _embedding_vector("hash-a", 1.0, 0.0, start),
            _embedding_vector("hash-b", 0.9, 0.43589, start),
            _embedding_vector("hash-c", 0.62, 0.78460, start),
        ],
        schema=EMBEDDING_SCHEMA,
    )

    result = cluster_events(articles, signals, embeddings).to_pylist()
    clusters = {row["content_sha256"]: row["cluster_id"] for row in result}

    assert clusters["hash-a"] == clusters["hash-b"]
    assert clusters["hash-c"] != clusters["hash-a"]
