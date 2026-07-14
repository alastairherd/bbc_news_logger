import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa

from bbc_news_logger.marts import build_marts
from bbc_news_logger.models import Observation
from bbc_news_logger.semantics import (
    EMBEDDING_DIMENSIONS,
    EMBEDDING_SCHEMA,
    SIGNAL_SCHEMA,
)
from bbc_news_logger.storage import ARTICLE_SCHEMA, observations_table


def test_build_marts_produces_static_research_payloads(tmp_path) -> None:
    start = datetime(2026, 7, 12, 10, tzinfo=timezone.utc)
    rows = [
        Observation.create(
            observed_at=start,
            surface="front_page",
            position=1,
            title="Story",
            url="https://www.bbc.co.uk/news/articles/example",
        ),
        Observation.create(
            observed_at=start + timedelta(hours=2),
            surface="most_read",
            position=3,
            title="Story",
            url="https://www.bbc.co.uk/news/articles/example",
        ),
    ]

    manifest = build_marts(observations_table(rows), tmp_path)

    assert manifest["observationCount"] == 2
    assert manifest["storyCount"] == 1
    assert set(manifest["files"]) == {
        "stories.json",
        "rank-series.json",
        "daily.json",
        "surface-lag.json",
        "semantic-trends.json",
        "recurring-events.json",
        "semantic-findings.json",
        "semantic-documents.json",
        "semantic-vectors.i8",
    }
    assert manifest["semantics"]["coveragePercent"] == 0.0
    assert manifest["semantics"]["searchDocumentCount"] == 0
    assert (tmp_path / "semantic-vectors.i8").read_bytes() == b""
    lag = json.loads((tmp_path / "surface-lag.json").read_text())
    assert lag[0]["lag_minutes"] == 120


def test_build_marts_quantizes_aligned_browser_search_index(tmp_path) -> None:
    observed = datetime(2026, 7, 12, 10, tzinfo=timezone.utc)
    observation = Observation.create(
        observed_at=observed,
        surface="front_page",
        position=2,
        title="Rent reform reaches Parliament",
        url="https://www.bbc.co.uk/news/articles/example",
    )
    article = {
        "snapshot_id": "snapshot-1",
        "requested_url": observation.url,
        "canonical_url": observation.url,
        "story_id": observation.story_id,
        "first_observed_at": observed,
        "fetched_at": observed,
        "fetched_at_is_inferred": False,
        "title": observation.title,
        "authors": [],
        "article_text": "A bill would change protections for renters.",
        "content_sha256": "hash-1",
        "html_sha256": "html-1",
        "http_status": 200,
        "fetch_ok": True,
    }
    generated = observed + timedelta(minutes=5)
    signal = {
        "content_sha256": "hash-1",
        "model": "deepseek-v4-flash",
        "prompt_version": "test-v1",
        "topic": "politics",
        "themes": ["renters' rights", "housing policy"],
        "summary": "Parliament considers changes to renters' protections.",
        "named_entities": ["Parliament"],
        "event_label": "Rent reform bill",
        "event_type": "policy_change",
        "story_form": "update",
        "generated_at": generated,
        "deepseek_response_id": "response-1",
        "prompt_tokens": 10,
        "prompt_cache_hit_tokens": 0,
        "prompt_cache_miss_tokens": 10,
        "completion_tokens": 5,
        "request_cost_usd": 0.001,
        "batch_size": 1,
    }
    vector = [0.0] * EMBEDDING_DIMENSIONS
    vector[0], vector[1] = 0.6, 0.8
    embedding = {
        "content_sha256": "hash-1",
        "model": "BAAI/bge-small-en-v1.5",
        "model_revision": "main",
        "input_version": "headline-body-lead-v1",
        "embedding": vector,
        "generated_at": generated,
    }

    manifest = build_marts(
        observations_table([observation]),
        tmp_path,
        articles=pa.Table.from_pylist([article], schema=ARTICLE_SCHEMA),
        signals=pa.Table.from_pylist([signal], schema=SIGNAL_SCHEMA),
        embeddings=pa.Table.from_pylist([embedding], schema=EMBEDDING_SCHEMA),
    )

    metadata = json.loads((tmp_path / "semantic-documents.json").read_text())
    assert manifest["semantics"]["searchDocumentCount"] == 1
    assert metadata["documentCount"] == 1
    assert metadata["documents"][0]["story_id"] == observation.story_id
    assert metadata["documents"][0]["summary"] == signal["summary"]
    assert len((tmp_path / "semantic-vectors.i8").read_bytes()) == EMBEDDING_DIMENSIONS
