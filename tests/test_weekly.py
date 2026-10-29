from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa

from bbc_news_logger.weekly import (
    MAX_EVIDENCE,
    keep_known_citations,
    maximum_run_cost_usd,
    monthly_spend_usd,
    summary_rows,
    theme_digests,
    week_bounds,
)

START, END = week_bounds(date(2026, 10, 26))


def _article(index: int, seen: datetime) -> dict[str, object]:
    return {
        "content_sha256": f"hash-{index}",
        "story_id": f"story-{index}",
        "canonical_url": f"https://www.bbc.co.uk/news/articles/{index}",
        "title": "Budget sets out Â£4bn plan" if index == 0 else f"Headline {index}",
        "first_observed_at": seen,
    }


def _signals(themes_by_hash: dict[str, list[str]]) -> pa.Table:
    generated = datetime(2026, 10, 25, tzinfo=timezone.utc)
    return pa.Table.from_pylist(
        [
            {
                "content_sha256": content_hash,
                "themes": themes,
                "summary": f"Summary of {content_hash}.",
                "generated_at": generated,
            }
            for content_hash, themes in themes_by_hash.items()
        ]
    )


def test_week_bounds_cover_seven_complete_days() -> None:
    assert datetime(2026, 10, 19, tzinfo=timezone.utc) == START
    assert timedelta(days=7) == END - START


def test_digests_keep_frequent_themes_in_the_week_with_numbered_evidence() -> None:
    inside = START + timedelta(days=2)
    articles = [_article(index, inside + timedelta(hours=index)) for index in range(4)]
    articles.append(_article(9, START - timedelta(hours=1)))
    signals = _signals(
        {
            "hash-0": ["budget"],
            "hash-1": ["budget"],
            "hash-2": ["budget", "energy"],
            "hash-3": ["energy"],
            "hash-9": ["budget"],
        }
    )

    digests = theme_digests(articles, signals, START, END)

    assert [digest.theme for digest in digests] == ["budget"]
    budget = digests[0]
    assert budget.article_count == 3
    assert budget.story_ids == ("story-2", "story-1", "story-0")
    assert budget.titles[-1] == "Budget sets out £4bn plan"
    assert budget.evidence.startswith("[1] 2026-10-21: Headline 2. Summary of hash-2.")


def test_evidence_is_capped_per_theme() -> None:
    inside = START + timedelta(days=1)
    articles = [
        _article(index, inside + timedelta(minutes=index)) for index in range(MAX_EVIDENCE + 5)
    ]
    signals = _signals({f"hash-{index}": ["politics"] for index in range(MAX_EVIDENCE + 5)})

    digest = theme_digests(articles, signals, START, END)[0]

    assert digest.article_count == MAX_EVIDENCE + 5
    assert len(digest.story_ids) == MAX_EVIDENCE


def test_cost_bound_and_monthly_spend() -> None:
    inside = START + timedelta(days=1)
    articles = [_article(index, inside) for index in range(3)]
    digests = theme_digests(
        articles, _signals({f"hash-{index}": ["budget"] for index in range(3)}), START, END
    )
    assert Decimal("0") < maximum_run_cost_usd(digests, "prompt") < Decimal("0.01")

    existing = pa.Table.from_pylist(
        [
            {"cost_usd": 0.02, "generated_at": datetime(2026, 10, 5, tzinfo=timezone.utc)},
            {"cost_usd": 0.5, "generated_at": datetime(2026, 9, 28, tzinfo=timezone.utc)},
        ]
    )
    assert monthly_spend_usd(existing, date(2026, 10, 28)) == Decimal("0.02")
    assert monthly_spend_usd(None, date(2026, 10, 28)) == Decimal("0")


def test_rows_drop_unknown_citations_and_empty_summaries() -> None:
    assert keep_known_citations("Rose [1][4] and fell [2].", 2) == "Rose [1] and fell [2]."
    inside = START + timedelta(days=1)
    articles = [_article(index, inside) for index in range(3)]
    digests = theme_digests(
        articles, _signals({f"hash-{index}": ["budget"] for index in range(3)}), START, END
    )
    generated = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)

    table = summary_rows(
        digests,
        ["Coverage grew [1][7]."],
        start=START,
        end=END,
        run_cost_usd=0.004,
        generated_at=generated,
    )
    assert table.to_pylist()[0]["summary"] == "Coverage grew [1]."
    assert table.to_pylist()[0]["week_end"] == date(2026, 10, 25)
    two_themes = [digests[0], digests[0]]
    partial = summary_rows(
        two_themes,
        ["Grew [1].", ""],
        start=START,
        end=END,
        run_cost_usd=0.004,
        generated_at=generated,
    )
    assert partial.column("cost_usd").to_pylist() == [0.004]
    assert (
        summary_rows(
            digests, ["  "], start=START, end=END, run_cost_usd=0.0, generated_at=generated
        ).num_rows
        == 0
    )
