---
license: other
language:
  - en
pretty_name: BBC News Raw Article Snapshots
tags:
  - journalism
  - news
  - html
configs:
  - config_name: raw_article_snapshots
    data_files: data/raw_article_snapshots/**/*.parquet
---

# BBC News Raw Article Snapshots

Raw HTML corresponding to the parsed article snapshots in
[`AlastairH/bbc-news-logger`](https://huggingface.co/datasets/AlastairH/bbc-news-logger).
It is separated so routine analytics do not download the largest, least-compressible field.

This independent research archive is not affiliated with or endorsed by the BBC. Content remains
subject to the BBC's terms and copyright. Do not treat this repository as a redistribution license;
users are responsible for the legality of their use.

Files are Zstandard-compressed Parquet, partitioned by UTC collection date. Join to the public
article table with `snapshot_id`. Integrity hashes are included in the public migration manifest.
