from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from bbc_news_logger.compaction import compact_path, compact_table, download_patterns


def test_download_patterns_include_base_and_incremental_shards() -> None:
    assert download_patterns(["semantic/signals"]) == [
        "compacted/semantic--signals.parquet",
        "semantic/signals/*.parquet",
        "semantic/signals/**/*.parquet",
    ]
    assert compact_path("data/observations") == "compacted/data--observations.parquet"


def test_semantic_compaction_keeps_latest_row_per_content_hash(tmp_path) -> None:
    schema = pa.schema(
        [
            pa.field("content_sha256", pa.string()),
            pa.field("generated_at", pa.timestamp("us", tz="UTC")),
            pa.field("summary", pa.string()),
        ]
    )
    old = pa.Table.from_pylist(
        [
            {
                "content_sha256": "same",
                "generated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "summary": "old",
            }
        ],
        schema=schema,
    )
    new = pa.Table.from_pylist(
        [
            {
                "content_sha256": "same",
                "generated_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
                "summary": "new",
            },
            {
                "content_sha256": "other",
                "generated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                "summary": "other",
            },
        ],
        schema=schema,
    )
    paths = [tmp_path / "old.parquet", tmp_path / "new.parquet"]
    pq.write_table(old, paths[0])
    pq.write_table(new, paths[1])

    rows = compact_table("semantic/signals", paths).to_pylist()

    assert {row["content_sha256"]: row["summary"] for row in rows} == {
        "same": "new",
        "other": "other",
    }
