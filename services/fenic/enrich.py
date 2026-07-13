"""Create a typed, cached semantic signal table for recent article snapshots."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import fenic as fc
import pyarrow as pa
from huggingface_hub import CommitOperationAdd, HfApi

from bbc_news_logger.deepseek import (
    DEEPSEEK_MODEL,
    MAX_RUN_BUDGET_USD,
    PROMPT_VERSION,
    BudgetExceeded,
    DeepSeekClient,
    DeepSeekResult,
    RunBudget,
    maximum_request_cost_usd,
)
from services.fenic.bootstrap import create_session

SIGNAL_DESCRIPTION = (
    "DeepSeek V4 Flash topic, theme, event, summary, and named-entity signals for articles."
)
SIGNAL_SCHEMA = pa.schema(
    [
        pa.field("snapshot_id", pa.string(), nullable=False),
        pa.field("story_id", pa.string(), nullable=False),
        pa.field("canonical_url", pa.string(), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
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
        pa.field("cache_reused", pa.bool_(), nullable=False),
    ]
)


@dataclass(frozen=True)
class EnrichmentReport:
    model: str
    prompt_version: str
    budget_usd: float
    spent_usd: float
    api_requests: int
    cache_reuses: int
    rows_added: int
    rows_total: int
    stopped_for_budget: bool
    published: bool = False


def _existing_rows(session: fc.Session) -> list[dict[str, Any]]:
    if "story_signals" not in session.catalog.list_tables():
        return []
    table = session.table("story_signals")
    required = {field.name for field in SIGNAL_SCHEMA}
    if not required.issubset(table.columns):
        return []
    return table.to_pylist()


def _candidate_rows(
    session: fc.Session, existing: list[dict[str, Any]], limit: int
) -> list[dict[str, Any]]:
    articles = session.table("article_snapshots").filter(fc.col("fetch_ok"))
    completed_ids = {
        str(row["snapshot_id"])
        for row in existing
        if row.get("model") == DEEPSEEK_MODEL and row.get("prompt_version") == PROMPT_VERSION
    }
    if completed_ids:
        completed = session.create_dataframe(
            [{"_completed_snapshot_id": value} for value in completed_ids]
        )
        articles = articles.join(
            completed,
            left_on=fc.col("snapshot_id"),
            right_on=fc.col("_completed_snapshot_id"),
            how="left",
        ).filter(fc.col("_completed_snapshot_id").is_null())
    return (
        articles.order_by(fc.desc("fetched_at"))
        .limit(limit)
        .select(
            "snapshot_id",
            "story_id",
            "canonical_url",
            "fetched_at",
            "content_sha256",
            "article_text",
        )
        .to_pylist()
    )


def _cached_record(article: dict[str, Any], cached: dict[str, Any]) -> dict[str, Any]:
    record = dict(cached)
    for field in ("snapshot_id", "story_id", "canonical_url", "fetched_at", "content_sha256"):
        record[field] = article[field]
    record.update(
        {
            "deepseek_response_id": "",
            "prompt_tokens": 0,
            "prompt_cache_hit_tokens": 0,
            "prompt_cache_miss_tokens": 0,
            "completion_tokens": 0,
            "request_cost_usd": 0.0,
            "cache_reused": True,
        }
    )
    return record


def _new_record(article: dict[str, Any], result: DeepSeekResult) -> dict[str, Any]:
    usage = result.usage
    return {
        "snapshot_id": article["snapshot_id"],
        "story_id": article["story_id"],
        "canonical_url": article["canonical_url"],
        "fetched_at": article["fetched_at"],
        "content_sha256": article["content_sha256"],
        "model": DEEPSEEK_MODEL,
        "prompt_version": PROMPT_VERSION,
        **asdict(result.signals),
        "themes": list(result.signals.themes),
        "named_entities": list(result.signals.named_entities),
        "generated_at": datetime.now(timezone.utc),
        "deepseek_response_id": result.response_id,
        "prompt_tokens": usage.prompt_tokens,
        "prompt_cache_hit_tokens": usage.prompt_cache_hit_tokens,
        "prompt_cache_miss_tokens": usage.prompt_cache_miss_tokens,
        "completion_tokens": usage.completion_tokens,
        "request_cost_usd": float(usage.cost_usd),
        "cache_reused": False,
    }


def enrich(
    limit: int,
    output: Path,
    *,
    maximum_cost_usd: Decimal,
    client: DeepSeekClient,
) -> EnrichmentReport:
    session = create_session()
    try:
        existing = _existing_rows(session)
        candidates = _candidate_rows(session, existing, limit)
        content_cache = {
            str(row["content_sha256"]): row
            for row in existing
            if row.get("model") == DEEPSEEK_MODEL and row.get("prompt_version") == PROMPT_VERSION
        }
        budget = RunBudget(maximum_cost_usd)
        additions: list[dict[str, Any]] = []
        api_requests = 0
        cache_reuses = 0
        stopped_for_budget = False

        for article in candidates:
            cached = content_cache.get(str(article["content_sha256"]))
            if cached:
                additions.append(_cached_record(article, cached))
                cache_reuses += 1
                continue

            try:
                budget.reserve(maximum_request_cost_usd(str(article["article_text"])))
            except BudgetExceeded:
                stopped_for_budget = True
                break
            result = client.enrich(str(article["article_text"]))
            budget.record(result.usage.cost_usd)
            record = _new_record(article, result)
            additions.append(record)
            content_cache[str(article["content_sha256"])] = record
            api_requests += 1

        combined = {
            (str(row["snapshot_id"]), str(row["model"]), str(row["prompt_version"])): row
            for row in existing
        }
        for row in additions:
            combined[(row["snapshot_id"], row["model"], row["prompt_version"])] = row
        records = list(combined.values())
        if records:
            frame = session.create_dataframe(pa.Table.from_pylist(records, schema=SIGNAL_SCHEMA))
            frame.write.save_as_table("story_signals", mode="overwrite")
            session.catalog.set_table_description("story_signals", SIGNAL_DESCRIPTION)
            output.parent.mkdir(parents=True, exist_ok=True)
            session.table("story_signals").write.parquet(output, mode="overwrite")

        return EnrichmentReport(
            model=DEEPSEEK_MODEL,
            prompt_version=PROMPT_VERSION,
            budget_usd=float(maximum_cost_usd),
            spent_usd=float(budget.spent_usd),
            api_requests=api_requests,
            cache_reuses=cache_reuses,
            rows_added=len(additions),
            rows_total=len(records),
            stopped_for_budget=stopped_for_budget,
        )
    finally:
        session.stop(skip_usage_summary=True)


def _budget(value: str) -> Decimal:
    try:
        budget = Decimal(value)
        RunBudget(budget)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return budget


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--output", type=Path, default=Path("dist/story-signals.parquet"))
    parser.add_argument("--report", type=Path, default=Path("dist/semantic-run.json"))
    parser.add_argument(
        "--max-cost-usd",
        type=_budget,
        default=_budget(os.getenv("DEEPSEEK_MAX_COST_USD", str(MAX_RUN_BUDGET_USD))),
    )
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.limit <= 200:
        raise SystemExit("--limit must be between 1 and 200")

    token = os.getenv("DEEPSEEK_API_KEY")
    if not token:
        raise SystemExit("DEEPSEEK_API_KEY is required for semantic enrichment")
    report = enrich(
        args.limit,
        args.output,
        maximum_cost_usd=args.max_cost_usd,
        client=DeepSeekClient(token),
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(asdict(report), indent=2) + "\n")

    if args.publish:
        if not args.output.exists():
            raise SystemExit("No story signals were produced, so nothing can be published")
        report = EnrichmentReport(**{**asdict(report), "published": True})
        args.report.write_text(json.dumps(asdict(report), indent=2) + "\n")
        HfApi(token=os.getenv("HF_TOKEN")).create_commit(
            repo_id=os.getenv("BBC_NEWS_DATASET", "AlastairH/bbc-news-logger"),
            repo_type="dataset",
            operations=[
                CommitOperationAdd(
                    path_in_repo="semantic/story-signals.parquet",
                    path_or_fileobj=args.output,
                ),
                CommitOperationAdd(
                    path_in_repo="semantic/latest-run.json",
                    path_or_fileobj=args.report,
                ),
            ],
            commit_message="Refresh DeepSeek semantic story signals",
        )
    print(json.dumps(asdict(report), sort_keys=True))


if __name__ == "__main__":
    main()
