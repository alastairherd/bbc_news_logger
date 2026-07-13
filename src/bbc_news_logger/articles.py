"""Fetch and parse article snapshots with real global request throttling."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime

import aiohttp
from selectolax.parser import HTMLParser

from .config import DEFAULT_REQUESTS_PER_SECOND, DEFAULT_TIMEOUT_SECONDS, DEFAULT_USER_AGENT
from .models import ArticleSnapshot, normalize_url, utc_now


@dataclass(frozen=True)
class ArticleTarget:
    url: str
    first_observed_at: datetime


class AsyncRateLimiter:
    """Serialize request starts so aggregate throughput respects the configured rate."""

    def __init__(self, requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND):
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = 1.0 / requests_per_second
        self._lock = asyncio.Lock()
        self._last_start = 0.0

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            delay = self._interval - (now - self._last_start)
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_start = loop.time()


def parse_article_html(html: str) -> tuple[str | None, str, list[str], str, str]:
    tree = HTMLParser(html)
    canonical_node = tree.css_first('link[rel="canonical"]')
    canonical = (
        canonical_node.attributes.get("href", "").strip() if canonical_node else None
    ) or None
    title_node = tree.css_first('meta[property="og:title"]')
    title = title_node.attributes.get("content", "").strip() if title_node else ""
    if not title and (heading := tree.css_first("h1")):
        title = heading.text(strip=True)

    authors = {
        node.text(strip=True)
        for node in tree.css('[rel="author"], [itemprop="name"]')
        if node.text(strip=True)
    }
    if (byline := tree.css_first('meta[name="byl"]')) and byline.attributes.get("content"):
        authors.add(byline.attributes["content"].strip())

    body_nodes = tree.css('[data-component="text-block"]')
    if body_nodes:
        article_html = "".join(node.html for node in body_nodes)
        article_text = " ".join(node.text(separator=" ", strip=True) for node in body_nodes)
    else:
        main = tree.css_first("main") or tree.body
        article_html = main.html if main else html
        article_text = (
            main.text(separator=" ", strip=True) if main else tree.text(separator=" ", strip=True)
        )
    return canonical, title, sorted(authors), article_html, article_text


async def fetch_one(
    session: aiohttp.ClientSession,
    target: ArticleTarget,
    limiter: AsyncRateLimiter,
) -> ArticleSnapshot:
    await limiter.wait()
    fetched_at = utc_now()
    try:
        async with session.get(
            target.url,
            headers={"User-Agent": DEFAULT_USER_AGENT},
            allow_redirects=True,
        ) as response:
            html = await response.text(errors="replace")
            canonical, title, authors, article_html, article_text = parse_article_html(html)
            return ArticleSnapshot.create(
                requested_url=target.url,
                canonical_url=canonical or str(response.url),
                first_observed_at=target.first_observed_at,
                fetched_at=fetched_at,
                title=title,
                authors=authors,
                article_text=article_text,
                article_html=article_html,
                http_status=response.status,
                fetch_ok=response.status == 200,
            )
    except Exception:
        return ArticleSnapshot.create(
            requested_url=target.url,
            canonical_url=normalize_url(target.url),
            first_observed_at=target.first_observed_at,
            fetched_at=fetched_at,
            title="",
            authors=[],
            article_text="",
            article_html="",
            http_status=None,
            fetch_ok=False,
        )


async def fetch_articles(
    targets: list[ArticleTarget],
    *,
    requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND,
    concurrency: int = 8,
) -> list[ArticleSnapshot]:
    deduplicated: dict[str, ArticleTarget] = {}
    for target in sorted(targets, key=lambda item: item.first_observed_at):
        deduplicated.setdefault(normalize_url(target.url), target)
    limiter = AsyncRateLimiter(requests_per_second)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT_SECONDS)
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        return await asyncio.gather(
            *(fetch_one(session, target, limiter) for target in deduplicated.values())
        )
