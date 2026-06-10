import asyncio
from pathlib import Path
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from article_content_scraper import (
    parse_article_html,
    fetch_articles,
)


@pytest.fixture
def url_df():
    return pd.DataFrame({
        "url": ["http://example.com/one", "http://example.com/one", "http://example.com/two"],
        "first_appeared_at": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-01")],
    })


def test_deduplication(url_df):
    df = url_df.sort_values("first_appeared_at")
    df = df.drop_duplicates("url", keep="first")
    assert len(df) == 2
    assert df.loc[df["url"] == "http://example.com/one", "first_appeared_at"].iloc[0] == pd.Timestamp("2024-01-01")


def test_parse_article_html():
    html = Path("tests/fixtures/art1.html").read_text()
    canonical, title, authors, article_html, article_text = parse_article_html(html)
    assert canonical == "http://example.com/one"
    assert "Hello World" in article_text
    assert "Author A" in authors


class MockResponse:
    def __init__(self, url, text):
        self.url = url
        self._text = text
        self.status = 200

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass


@pytest.mark.asyncio
async def test_fetch_articles(monkeypatch, url_df):
    html_map = {
        "http://example.com/one": Path("tests/fixtures/art1.html").read_text(),
        "http://example.com/two": Path("tests/fixtures/art2.html").read_text(),
    }

    def fake_get(self, url, **kwargs):
        return MockResponse(url, html_map[url])

    monkeypatch.setattr("aiohttp.ClientSession.get", fake_get, raising=False)

    dedup = url_df.drop_duplicates("url", keep="first")
    df = await fetch_articles(dedup)
    assert len(df) == 2
    assert df["fetch_ok"].all()
    assert df["article_text"].notna().all()
