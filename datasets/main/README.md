---
license: other
language:
  - en
pretty_name: BBC News Surface Observations
task_categories:
  - time-series-forecasting
tags:
  - journalism
  - news
  - longitudinal
configs:
  - config_name: observations
    data_files: data/observations/**/*.parquet
  - config_name: article_snapshots
    data_files: data/article_snapshots/**/*.parquet
  - config_name: scrape_runs
    data_files: data/scrape_runs/**/*.parquet
  - config_name: story_signals
    data_files: semantic/signals/**/*.parquet
  - config_name: article_embeddings
    data_files: semantic/embeddings/**/*.parquet
  - config_name: recurring_events
    data_files: semantic/events/**/*.parquet
---

# BBC News Surface Observations

An independent longitudinal research dataset recording which stories appear on the BBC News
front page and Most Read list, plus parsed snapshots of linked articles.

This project is not affiliated with or endorsed by the BBC. Headlines, article text, and linked
content remain subject to the BBC's terms and copyright. The collection is published for research,
audit, and journalistic analysis; users are responsible for ensuring their use is lawful.

## Configurations

- `observations`: one row per story position and hourly collection. Stable `story_id` values make
  cross-surface and longitudinal joins straightforward.
- `article_snapshots`: parsed metadata and plain text fetched once per daily URL set. Raw HTML is
  kept in the companion raw dataset.
- `scrape_runs`: operational metadata for new collection runs, including selector version and
  validation counts.
- `story_signals`: DeepSeek-generated topics, themes, story forms, event labels, and entities,
  keyed by article content hash with token and cost provenance.
- `article_embeddings`: normalized 384-dimensional BGE Small vectors, also keyed by content hash.
- `recurring_events`: reproducible recurring-story cluster membership derived from vectors and
  structured event evidence.

All timestamps are UTC. Files use Zstandard-compressed Parquet partitioned by year, month, and UTC
date. Schema version metadata is embedded in every file. Historical article `fetched_at` values are
inferred from the old daily filename and are marked with `fetched_at_is_inferred=true`.

The migration audit at `migration/manifest.json` records source hashes, row counts, destination
hashes, and the source Git commit.

## Known limitations

- This records two BBC web surfaces, not editorial intent, readership, or all BBC output.
- Layout and selector changes can cause gaps. New runs fail closed if either surface is empty.
- The legacy article writer accidentally stored the URL in `first_appeared_at`; migration repairs
  it from the earliest matching observation when available.
- Front-page promos had no explicit rank historically, so their position is reconstructed from row
  order within each scrape.
- Semantic labels and clusters are model-assisted research signals, not verified statements of
  fact. Coverage and model versions must be considered when comparing time periods.
