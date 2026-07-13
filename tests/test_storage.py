from datetime import date, datetime, timezone

import pyarrow.parquet as pq

from bbc_news_logger.models import ArticleSnapshot, Observation
from bbc_news_logger.storage import (
    articles_table,
    merge_unique,
    observations_table,
    partition_path,
    raw_articles_table,
    write_parquet,
)

NOW = datetime(2026, 7, 13, 10, tzinfo=timezone.utc)


def test_observation_schema_and_merge_are_idempotent(tmp_path) -> None:
    row = Observation.create(
        observed_at=NOW,
        surface="most_read",
        position=1,
        title="Story",
        url="https://www.bbc.co.uk/news/articles/example?utm_source=test",
    )
    incoming = observations_table([row])
    merged = merge_unique(incoming, incoming, ("scrape_id", "surface", "position"))
    output = write_parquet(merged, tmp_path / "part.parquet")

    assert merged.num_rows == 1
    assert pq.read_table(output).schema.metadata[b"schema_version"] == b"1"
    assert row.url == "https://www.bbc.co.uk/news/articles/example"
    assert partition_path("observations", date(2026, 7, 13)).endswith(
        "year=2026/month=07/2026-07-13.parquet"
    )


def test_article_html_is_excluded_from_public_table() -> None:
    row = ArticleSnapshot.create(
        requested_url="https://www.bbc.co.uk/news/articles/example",
        canonical_url="https://www.bbc.co.uk/news/articles/example",
        first_observed_at=NOW,
        fetched_at=NOW,
        title="Story",
        authors=["A Writer"],
        article_text="Article text",
        article_html="<p>Article text</p>",
        http_status=200,
        fetch_ok=True,
    )

    assert "article_html" not in articles_table([row]).column_names
    assert raw_articles_table([row]).column_names == [
        "snapshot_id",
        "canonical_url",
        "fetched_at",
        "article_html",
        "html_sha256",
    ]
