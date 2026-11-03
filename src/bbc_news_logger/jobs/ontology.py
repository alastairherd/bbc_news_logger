"""CLI entry point for incremental theme hierarchy classification and Wikidata entity linking."""

from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal

import pyarrow as pa

from bbc_news_logger.config import DEFAULT_DATASET_ID
from bbc_news_logger.deepseek import DeepSeekClient
from bbc_news_logger.semantics import SIGNAL_PREFIX, download_dataset_tables
from bbc_news_logger.storage import HuggingFacePublisher
from bbc_news_logger.taxonomy_wikidata import (
    CURATED_ENTITY_QIDS,
    MAX_TAXONOMY_RUN_COST_USD,
    TAXONOMY_PATH,
    TAXONOMY_SCHEMA,
    WIKIDATA_PATH,
    WIKIDATA_SCHEMA,
    classify_themes_with_deepseek,
    extract_active_concepts,
    infer_entity_type_from_description,
    resolve_entities_wikidata,
    resolve_entity_wikidata,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally update theme taxonomy and Wikidata entity links"
    )
    parser.add_argument("--dataset", default=os.getenv("BBC_NEWS_DATASET", DEFAULT_DATASET_ID))
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--skip-taxonomy", action="store_true")
    parser.add_argument("--skip-wikidata", action="store_true")
    parser.add_argument(
        "--max-cost-usd",
        type=Decimal,
        default=MAX_TAXONOMY_RUN_COST_USD,
    )
    args = parser.parse_args()

    token = os.getenv("HF_TOKEN")
    publisher = HuggingFacePublisher(dataset_id=args.dataset, token=token)
    tables = download_dataset_tables(
        ["data/article_snapshots", SIGNAL_PREFIX],
        dataset_id=args.dataset,
        token=token,
    )
    articles = tables["data/article_snapshots"]
    signals = tables[SIGNAL_PREFIX]
    if articles is None or signals is None:
        raise SystemExit("Article snapshots and signals are required for ontology enrichment")

    active_themes, active_entities = extract_active_concepts(articles, signals)

    existing_taxonomy = publisher.read_table(args.dataset, TAXONOMY_PATH)
    known_themes = (
        {str(row["theme"]).casefold() for row in existing_taxonomy.to_pylist()}
        if existing_taxonomy is not None
        else set()
    )
    missing_themes = [
        (theme, top_topic)
        for theme, top_topic, _count in active_themes
        if theme.casefold() not in known_themes
    ]

    existing_wikidata = publisher.read_table(args.dataset, WIKIDATA_PATH)
    known_entities: dict[str, dict[str, object]] = {}
    refreshed_rows: list[dict[str, object]] = []
    if existing_wikidata is not None:
        for row in existing_wikidata.to_pylist():
            entity_name = str(row["entity"])
            folded = entity_name.casefold()
            if folded in CURATED_ENTITY_QIDS:
                curated_row = resolve_entity_wikidata(entity_name)
                known_entities[folded] = curated_row
                if (
                    row.get("qid") != curated_row["qid"]
                    or row.get("entity_type") != curated_row["entity_type"]
                ):
                    refreshed_rows.append(curated_row)
                continue
            qid = str(row.get("qid") or "").strip()
            if not qid:
                continue
            new_type = infer_entity_type_from_description(
                str(row.get("description") or ""), str(row.get("label") or "")
            )
            if new_type != row.get("entity_type"):
                updated_row = dict(row)
                updated_row["entity_type"] = new_type
                refreshed_rows.append(updated_row)
                known_entities[folded] = updated_row
            else:
                known_entities[folded] = row

    missing_entities = [
        entity for entity, _count in active_entities if entity.casefold() not in known_entities
    ]

    taxonomy_added = 0
    taxonomy_cost = Decimal("0")
    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_API") or ""
    if not args.skip_taxonomy and missing_themes and api_key:
        client = DeepSeekClient(api_key)
        new_tax_rows, taxonomy_cost = classify_themes_with_deepseek(
            missing_themes,
            client=client,
            max_cost_usd=args.max_cost_usd,
        )
        taxonomy_added = len(new_tax_rows)
        if new_tax_rows and args.publish:
            incoming_tax = pa.Table.from_pylist(new_tax_rows, schema=TAXONOMY_SCHEMA)
            publisher.upsert_table(
                repo_id=args.dataset,
                path=TAXONOMY_PATH,
                incoming=incoming_tax,
                keys=("theme",),
                message=f"Update theme taxonomy (+{taxonomy_added} themes)",
            )

    wikidata_added = 0
    if not args.skip_wikidata and (missing_entities or refreshed_rows):
        new_wiki_rows = resolve_entities_wikidata(missing_entities) if missing_entities else []
        all_updates = [*refreshed_rows, *new_wiki_rows]
        wikidata_added = len(all_updates)
        if all_updates and args.publish:
            incoming_wiki = pa.Table.from_pylist(all_updates, schema=WIKIDATA_SCHEMA)
            publisher.upsert_table(
                repo_id=args.dataset,
                path=WIKIDATA_PATH,
                incoming=incoming_wiki,
                keys=("entity",),
                message=(
                    f"Update entity Wikidata links "
                    f"(+{len(new_wiki_rows)} new, {len(refreshed_rows)} refreshed)"
                ),
            )

    print(
        json.dumps(
            {
                "active_themes": len(active_themes),
                "missing_themes": len(missing_themes),
                "taxonomy_added": taxonomy_added,
                "taxonomy_cost_usd": float(taxonomy_cost),
                "active_entities": len(active_entities),
                "missing_entities": len(missing_entities),
                "wikidata_added": wikidata_added,
                "published": args.publish,
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
