import json
from datetime import datetime, timedelta, timezone

from bbc_news_logger.marts import build_marts
from bbc_news_logger.models import Observation
from bbc_news_logger.storage import observations_table


def test_build_marts_produces_static_research_payloads(tmp_path) -> None:
    start = datetime(2026, 7, 12, 10, tzinfo=timezone.utc)
    rows = [
        Observation.create(
            observed_at=start,
            surface="front_page",
            position=1,
            title="Story",
            url="https://www.bbc.co.uk/news/articles/example",
        ),
        Observation.create(
            observed_at=start + timedelta(hours=2),
            surface="most_read",
            position=3,
            title="Story",
            url="https://www.bbc.co.uk/news/articles/example",
        ),
    ]

    manifest = build_marts(observations_table(rows), tmp_path)

    assert manifest["observationCount"] == 2
    assert manifest["storyCount"] == 1
    assert set(manifest["files"]) == {
        "stories.json",
        "rank-series.json",
        "daily.json",
        "surface-lag.json",
        "semantic-trends.json",
        "recurring-events.json",
    }
    assert manifest["semantics"]["coveragePercent"] == 0.0
    lag = json.loads((tmp_path / "surface-lag.json").read_text())
    assert lag[0]["lag_minutes"] == 120
