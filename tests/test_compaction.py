from datetime import datetime, timezone
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from bbc_news_logger.compaction import (
    COMPACTABLE_PREFIXES,
    compact_path,
    compact_remote_dataset,
    compact_table,
    download_patterns,
)


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


def test_publish_skips_deleting_absent_shard_directories(tmp_path) -> None:
    snapshot = tmp_path / "snapshot-revision"
    output = tmp_path / "output"
    snapshot.mkdir()
    (snapshot / "data/observations").mkdir(parents=True)

    table = pa.table({"value": [1]})
    for prefix in COMPACTABLE_PREFIXES:
        base = snapshot / compact_path(prefix)
        base.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, base)

    with (
        patch(
            "bbc_news_logger.compaction.snapshot_download",
            return_value=str(snapshot),
        ),
        patch("bbc_news_logger.compaction.compact_table", return_value=table),
        patch("bbc_news_logger.compaction.HfApi.create_commit") as create_commit,
    ):
        compact_remote_dataset(output, publish=True)

    operations = create_commit.call_args.kwargs["operations"]
    deletes = [
        operation.path_in_repo for operation in operations if hasattr(operation, "is_folder")
    ]
    assert deletes == ["data/observations"]
