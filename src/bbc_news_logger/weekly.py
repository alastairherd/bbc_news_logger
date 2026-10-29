"""Inputs, limits, and outputs for the weekly theme summaries job."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pyarrow as pa

from .deepseek import DEEPSEEK_MODEL, token_cost_usd
from .titles import repair_title

WEEKLY_PREFIX = "semantic/weekly"
WEEKLY_MODEL = DEEPSEEK_MODEL
WEEKLY_PROMPT_VERSION = "2026-10-29-v2"
MAX_THEMES = 8
MIN_ARTICLES = 3
MAX_EVIDENCE = 30
MAX_SUMMARY_CHARACTERS = 320
MAX_OUTPUT_TOKENS = 420
PROMPT_OVERHEAD_TOKENS = 1_024
MAX_RUN_COST_USD = Decimal("0.10")
MAX_MONTHLY_COST_USD = Decimal("0.25")
CITATION = re.compile(r"\[(\d+)\]")

WEEKLY_SCHEMA = pa.schema(
    [
        pa.field("week_start", pa.date32(), nullable=False),
        pa.field("week_end", pa.date32(), nullable=False),
        pa.field("theme", pa.string(), nullable=False),
        pa.field("article_count", pa.int32(), nullable=False),
        pa.field("summary", pa.string(), nullable=False),
        pa.field("evidence_story_ids", pa.list_(pa.string()), nullable=False),
        pa.field("evidence_titles", pa.list_(pa.string()), nullable=False),
        pa.field("evidence_urls", pa.list_(pa.string()), nullable=False),
        pa.field("model", pa.string(), nullable=False),
        pa.field("prompt_version", pa.string(), nullable=False),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("cost_usd", pa.float64(), nullable=False),
    ]
)


@dataclass(frozen=True)
class ThemeDigest:
    theme: str
    article_count: int
    story_ids: tuple[str, ...]
    titles: tuple[str, ...]
    urls: tuple[str, ...]
    evidence: str


def week_bounds(week_end: date) -> tuple[datetime, datetime]:
    """Return the seven complete UTC days ending before `week_end`."""

    end = datetime(week_end.year, week_end.month, week_end.day, tzinfo=timezone.utc)
    return end - timedelta(days=7), end


def _latest_signals(signals: pa.Table) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in signals.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        if content_hash and (
            content_hash not in latest or row["generated_at"] > latest[content_hash]["generated_at"]
        ):
            latest[content_hash] = row
    return latest


def theme_digests(
    articles: Iterable[dict[str, Any]], signals: pa.Table, start: datetime, end: datetime
) -> list[ThemeDigest]:
    """Group the week's labelled articles by theme and number each theme's evidence."""

    labels = _latest_signals(signals)
    week = [
        (article, labels[article["content_sha256"]])
        for article in articles
        if start <= article["first_observed_at"] < end and article["content_sha256"] in labels
    ]
    counts = Counter(theme for _article, label in week for theme in set(label["themes"]))
    themes = [theme for theme, count in counts.most_common() if count >= MIN_ARTICLES][:MAX_THEMES]
    digests = []
    for theme in themes:
        rows = sorted(
            ((article, label) for article, label in week if theme in label["themes"]),
            key=lambda pair: pair[0]["first_observed_at"],
            reverse=True,
        )[:MAX_EVIDENCE]
        titles = tuple(repair_title(article["title"]) for article, _label in rows)
        evidence = "\n".join(
            f"[{index}] {article['first_observed_at']:%Y-%m-%d}: {title}. "
            f"{label['summary'][:MAX_SUMMARY_CHARACTERS]}"
            for index, ((article, label), title) in enumerate(
                zip(rows, titles, strict=True), start=1
            )
        )
        digests.append(
            ThemeDigest(
                theme=theme,
                article_count=counts[theme],
                story_ids=tuple(article["story_id"] for article, _label in rows),
                titles=titles,
                urls=tuple(article["canonical_url"] for article, _label in rows),
                evidence=evidence,
            )
        )
    return digests


def maximum_run_cost_usd(digests: Sequence[ThemeDigest], prompt: str) -> Decimal:
    """Upper bound before any request: tokens never exceed UTF-8 bytes, output is capped, and
    every input token is priced as an uncached peak-time token."""

    input_tokens = sum(
        len(prompt.encode("utf-8")) + len(digest.evidence.encode("utf-8")) + PROMPT_OVERHEAD_TOKENS
        for digest in digests
    )
    output_tokens = MAX_OUTPUT_TOKENS * len(digests)
    return token_cost_usd(
        cache_hit_input_tokens=0,
        cache_miss_input_tokens=input_tokens,
        output_tokens=output_tokens,
        peak=True,
    )


def monthly_spend_usd(existing: pa.Table | None, today: date) -> Decimal:
    if existing is None:
        return Decimal(0)
    return sum(
        (
            Decimal(str(row["cost_usd"]))
            for row in existing.to_pylist()
            if row["generated_at"].year == today.year and row["generated_at"].month == today.month
        ),
        Decimal(0),
    )


def keep_known_citations(summary: str, evidence_count: int) -> str:
    """Drop citation markers that point outside the supplied evidence."""

    return CITATION.sub(
        lambda match: match.group(0) if 1 <= int(match.group(1)) <= evidence_count else "",
        summary,
    ).strip()


def summary_rows(
    digests: Sequence[ThemeDigest],
    summaries: Sequence[str],
    *,
    start: datetime,
    end: datetime,
    run_cost_usd: float,
    generated_at: datetime,
) -> pa.Table:
    # The whole run cost lands on the published rows, so the monthly ledger never undercounts.
    share = run_cost_usd / max(sum(1 for summary in summaries if summary.strip()), 1)
    rows = [
        {
            "week_start": start.date(),
            "week_end": (end - timedelta(days=1)).date(),
            "theme": digest.theme,
            "article_count": digest.article_count,
            "summary": keep_known_citations(summary, len(digest.story_ids)),
            "evidence_story_ids": list(digest.story_ids),
            "evidence_titles": list(digest.titles),
            "evidence_urls": list(digest.urls),
            "model": WEEKLY_MODEL,
            "prompt_version": WEEKLY_PROMPT_VERSION,
            "generated_at": generated_at,
            "cost_usd": share,
        }
        for digest, summary in zip(digests, summaries, strict=True)
        if summary.strip()
    ]
    return pa.Table.from_pylist(rows, schema=WEEKLY_SCHEMA)
