"""Checkpointed, parallel DeepSeek enrichment for unique article versions."""

from __future__ import annotations

import argparse
import json
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pyarrow as pa

from bbc_news_logger.config import DEFAULT_DATASET_ID
from bbc_news_logger.deepseek import (
    DEEPSEEK_MODEL,
    MAX_BATCH_SIZE,
    MAX_RUN_BUDGET_USD,
    PROMPT_VERSION,
    DeepSeekBatchResult,
    DeepSeekClient,
    RunBudget,
    maximum_batch_request_cost_usd,
)
from bbc_news_logger.semantics import (
    SIGNAL_PREFIX,
    SIGNAL_SCHEMA,
    SemanticCheckpoint,
    completed_hashes,
    download_dataset_tables,
    publish_shard,
    signal_rows_from_batch,
    unique_article_rows,
)
from bbc_news_logger.storage import write_parquet

MAX_BACKFILL_BUDGET_USD = Decimal("7.50")
MAX_MONTHLY_BUDGET_USD = Decimal("1.00")
REMOTE_SIGNAL_SHARD_ROWS = MAX_BATCH_SIZE * 4


@dataclass(frozen=True)
class EnrichmentReport:
    model: str
    prompt_version: str
    scope: str
    process_budget_usd: float
    prior_scope_spend_usd: float
    spent_usd: float
    api_requests: int
    rows_added: int
    rows_published_from_checkpoint: int
    failures: int
    remaining: int
    stopped_for_budget: bool


def _scope_spend(table: pa.Table | None, scope: str) -> Decimal:
    if table is None:
        return Decimal("0")
    now = datetime.now(timezone.utc)
    total = Decimal("0")
    seen: set[tuple[str, str, str]] = set()
    for row in table.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        identity = (
            content_hash,
            str(row.get("prompt_version") or ""),
            str(row.get("deepseek_response_id") or ""),
        )
        if not content_hash or identity in seen:
            continue
        if row.get("model") != DEEPSEEK_MODEL:
            continue
        generated = row.get("generated_at")
        if scope == "monthly" and (
            generated is None or generated.year != now.year or generated.month != now.month
        ):
            continue
        seen.add(identity)
        total += Decimal(str(row.get("request_cost_usd") or 0))
    return total


def _batches(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[index : index + size] for index in range(0, len(rows), size)]


def _request(client: DeepSeekClient, batch: list[dict[str, Any]]) -> DeepSeekBatchResult:
    return client.enrich_batch(
        [(str(row["content_sha256"]), str(row["article_text"])) for row in batch]
    )


def enrich(
    *,
    limit: int,
    batch_size: int,
    concurrency: int,
    checkpoint_path: Path,
    output_dir: Path,
    maximum_cost_usd: Decimal,
    scope: str,
    publish: bool,
    retry_failures: bool,
    dataset_id: str,
    client: DeepSeekClient,
) -> EnrichmentReport:
    tables = download_dataset_tables(
        ["data/article_snapshots", SIGNAL_PREFIX], dataset_id=dataset_id
    )
    article_table = tables["data/article_snapshots"]
    signal_table = tables[SIGNAL_PREFIX]
    if article_table is None:
        raise FileNotFoundError(f"No article snapshots found in {dataset_id}")

    prior_spend = _scope_spend(signal_table, scope)
    scope_cap = MAX_BACKFILL_BUDGET_USD if scope == "backfill" else MAX_MONTHLY_BUDGET_USD
    scope_remaining = max(Decimal("0"), scope_cap - prior_spend)
    process_cap = min(maximum_cost_usd, scope_remaining)

    checkpoint = SemanticCheckpoint(checkpoint_path)
    try:
        remote_done = completed_hashes(
            signal_table,
            model=DEEPSEEK_MODEL,
            version_field="prompt_version",
            version=PROMPT_VERSION,
        )
        local_rows = [
            row for row in checkpoint.rows() if str(row["content_sha256"]) not in remote_done
        ]
        published_from_checkpoint = 0
        if publish:
            for rows in _batches(local_rows, REMOTE_SIGNAL_SHARD_ROWS):
                table = pa.Table.from_pylist(rows, schema=SIGNAL_SCHEMA)
                publish_shard(
                    table,
                    prefix=SIGNAL_PREFIX,
                    dataset_id=dataset_id,
                    message=f"Checkpoint {len(rows)} DeepSeek story signals",
                )
                published_from_checkpoint += len(rows)
                remote_done.update(str(row["content_sha256"]) for row in rows)

        done = remote_done | checkpoint.completed_hashes()
        failed = set() if retry_failures else checkpoint.failed_hashes()
        candidates = [
            row
            for row in unique_article_rows(article_table)
            if row["content_sha256"] not in done and row["content_sha256"] not in failed
        ]
        selected = candidates[:limit] if limit > 0 else candidates
        print(
            json.dumps(
                {
                    "event": "deepseek_start",
                    "scope": scope,
                    "prior_scope_spend_usd": float(prior_spend),
                    "process_budget_usd": float(process_cap),
                    "candidates": len(candidates),
                    "selected": len(selected),
                    "batch_size": batch_size,
                    "concurrency": concurrency,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        pending_batches = _batches(selected, batch_size)
        output_dir.mkdir(parents=True, exist_ok=True)
        api_requests = 0
        rows_added = 0
        failure_count = 0
        stopped_for_budget = process_cap <= 0 and bool(pending_batches)
        budget = RunBudget(process_cap) if process_cap > 0 else None

        while pending_batches and budget is not None:
            wave: list[tuple[list[dict[str, Any]], Decimal]] = []
            available = budget.remaining_usd
            while pending_batches and len(wave) < concurrency:
                batch = pending_batches[0]
                reservation = maximum_batch_request_cost_usd(
                    [(str(row["content_sha256"]), str(row["article_text"])) for row in batch]
                )
                if reservation > available:
                    stopped_for_budget = True
                    break
                pending_batches.pop(0)
                wave.append((batch, reservation))
                available -= reservation
            if not wave:
                break

            futures: dict[Future[DeepSeekBatchResult], list[dict[str, Any]]] = {}
            wave_rows: list[dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                for batch, _reservation in wave:
                    futures[executor.submit(_request, client, batch)] = batch
                for future in as_completed(futures):
                    batch = futures[future]
                    hashes = [str(row["content_sha256"]) for row in batch]
                    try:
                        result = future.result()
                    except BaseException as exc:
                        checkpoint.record_failure(hashes, exc)
                        failure_count += len(batch)
                        continue
                    rows = signal_rows_from_batch(result)
                    checkpoint.record_rows(rows)
                    budget.record(result.usage.cost_usd)
                    table = pa.Table.from_pylist(rows, schema=SIGNAL_SCHEMA)
                    local_path = output_dir / f"{result.response_id or hashes[0]}.parquet"
                    write_parquet(table, local_path)
                    wave_rows.extend(rows)
                    api_requests += 1
                    rows_added += len(rows)
                    print(
                        json.dumps(
                            {
                                "event": "deepseek_response_checkpoint",
                                "api_requests": api_requests,
                                "rows_added": rows_added,
                                "response_rows": len(rows),
                                "spent_usd": float(budget.spent_usd),
                            },
                            sort_keys=True,
                        ),
                        flush=True,
                    )
            if publish and wave_rows:
                wave_table = pa.Table.from_pylist(wave_rows, schema=SIGNAL_SCHEMA)
                remote_path = publish_shard(
                    wave_table,
                    prefix=SIGNAL_PREFIX,
                    dataset_id=dataset_id,
                    message=f"Checkpoint {len(wave_rows)} DeepSeek story signals",
                )
                print(
                    json.dumps(
                        {
                            "event": "deepseek_remote_checkpoint",
                            "rows_in_shard": len(wave_rows),
                            "rows_added": rows_added,
                            "path": remote_path,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )

        return EnrichmentReport(
            model=DEEPSEEK_MODEL,
            prompt_version=PROMPT_VERSION,
            scope=scope,
            process_budget_usd=float(process_cap),
            prior_scope_spend_usd=float(prior_spend),
            spent_usd=float(budget.spent_usd if budget else Decimal("0")),
            api_requests=api_requests,
            rows_added=rows_added,
            rows_published_from_checkpoint=published_from_checkpoint,
            failures=failure_count,
            remaining=max(0, len(candidates) - rows_added),
            stopped_for_budget=stopped_for_budget,
        )
    finally:
        checkpoint.close()


def _budget(value: str) -> Decimal:
    try:
        budget = Decimal(value)
        RunBudget(budget)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return budget


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=MAX_BATCH_SIZE)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--checkpoint", type=Path, default=Path("dist/semantic-checkpoint.sqlite3"))
    parser.add_argument("--output-dir", type=Path, default=Path("dist/signal-shards"))
    parser.add_argument("--report", type=Path, default=Path("dist/semantic-run.json"))
    parser.add_argument("--dataset", default=os.getenv("BBC_NEWS_DATASET", DEFAULT_DATASET_ID))
    parser.add_argument("--scope", choices=("backfill", "monthly"), default="monthly")
    parser.add_argument("--retry-failures", action="store_true")
    parser.add_argument(
        "--max-cost-usd",
        type=_budget,
        default=_budget(os.getenv("DEEPSEEK_MAX_COST_USD", str(MAX_RUN_BUDGET_USD))),
    )
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    if args.limit < 0:
        raise SystemExit("--limit cannot be negative; use 0 for all remaining articles")
    if not 1 <= args.batch_size <= MAX_BATCH_SIZE:
        raise SystemExit(f"--batch-size must be between 1 and {MAX_BATCH_SIZE}")
    if not 1 <= args.concurrency <= 8:
        raise SystemExit("--concurrency must be between 1 and 8")

    token = os.getenv("DEEPSEEK_API_KEY")
    if not token:
        raise SystemExit("DEEPSEEK_API_KEY is required for semantic enrichment")
    report = enrich(
        limit=args.limit,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        checkpoint_path=args.checkpoint,
        output_dir=args.output_dir,
        maximum_cost_usd=args.max_cost_usd,
        scope=args.scope,
        publish=args.publish,
        retry_failures=args.retry_failures,
        dataset_id=args.dataset,
        client=DeepSeekClient(token),
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(asdict(report), indent=2) + "\n")
    print(json.dumps(asdict(report), sort_keys=True))


if __name__ == "__main__":
    main()
