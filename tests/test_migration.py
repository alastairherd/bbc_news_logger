import csv

import pyarrow as pa
import pyarrow.parquet as pq

from bbc_news_logger.migration import build_migration


def test_build_migration_splits_public_text_and_raw_html(tmp_path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    with (data / "bbc_most_read_2026-07-12.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "rank", "title", "link"])
        writer.writerow(
            [
                "2026-07-12 10:00:00 UTC",
                1,
                "Story",
                "https://www.bbc.co.uk/news/articles/example",
            ]
        )
    article_dir = data / "article-content"
    article_dir.mkdir()
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "url": "https://www.bbc.co.uk/news/articles/example",
                    # The legacy writer accidentally duplicated URL into this field.
                    "first_appeared_at": "https://www.bbc.co.uk/news/articles/example",
                    "title": "Story",
                    "authors": ["A Writer"],
                    "article_html": "<p>Body</p>",
                    "article_text": "Body",
                    "fetch_ok": True,
                }
            ]
        ),
        article_dir / "2026-07-12.parquet",
    )

    output = tmp_path / "output"
    manifest = build_migration(data, output, "abc123")

    assert manifest["source_commit"] == "abc123"
    assert manifest["totals"] == {
        "observations": 1,
        "article_snapshots": 1,
        "source_files": 2,
    }
    public = next((output / "main/data/article_snapshots").rglob("*.parquet"))
    raw = next((output / "raw/data/raw_article_snapshots").rglob("*.parquet"))
    assert "article_html" not in pq.read_table(public).column_names
    assert pq.read_table(raw)["article_html"].to_pylist() == ["<p>Body</p>"]
