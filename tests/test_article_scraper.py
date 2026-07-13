from datetime import datetime, timezone
from pathlib import Path

import pytest

from bbc_news_logger.articles import (
    ArticleTarget,
    AsyncRateLimiter,
    fetch_articles,
    parse_article_html,
)


def test_parse_article_html() -> None:
    html = Path("tests/fixtures/art1.html").read_text()
    canonical, title, authors, article_html, article_text = parse_article_html(html)

    assert canonical == "http://example.com/one"
    assert title
    assert "Hello World" in article_text
    assert "Author A" in authors
    assert article_html


class MockResponse:
    def __init__(self, url: str, body: str) -> None:
        self.url = url
        self._body = body
        self.status = 200

    async def text(self, **_: object) -> str:
        return self._body

    async def __aenter__(self) -> "MockResponse":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None


@pytest.mark.asyncio
async def test_fetch_articles_deduplicates_and_returns_snapshots(monkeypatch) -> None:
    html_map = {
        "http://example.com/one": Path("tests/fixtures/art1.html").read_text(),
        "http://example.com/two": Path("tests/fixtures/art2.html").read_text(),
    }

    def fake_get(_self, url: str, **_kwargs: object) -> MockResponse:
        return MockResponse(url, html_map[url])

    async def no_wait(_self) -> None:
        return None

    monkeypatch.setattr("aiohttp.ClientSession.get", fake_get, raising=False)
    monkeypatch.setattr(AsyncRateLimiter, "wait", no_wait)
    first = datetime(2024, 1, 1, tzinfo=timezone.utc)
    second = datetime(2024, 1, 2, tzinfo=timezone.utc)
    targets = [
        ArticleTarget("http://example.com/one", second),
        ArticleTarget("http://example.com/one", first),
        ArticleTarget("http://example.com/two", first),
    ]

    snapshots = await fetch_articles(targets)

    assert len(snapshots) == 2
    assert all(snapshot.fetch_ok for snapshot in snapshots)
    assert all(snapshot.article_text for snapshot in snapshots)
    one = next(item for item in snapshots if item.requested_url.endswith("/one"))
    assert one.first_observed_at == first
