"""BBC News front-page collection with explicit validation and failure semantics."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from .config import BBC_NEWS_URL, DEFAULT_TIMEOUT_SECONDS, DEFAULT_USER_AGENT
from .models import Observation, ScrapeResult, ScrapeRun, stable_id, utc_now

MOST_READ_SELECTORS = (
    'div[data-component="mostRead"] ol',
    'section[data-component="mostRead"] ol',
    'div[data-component="MostRead"] ol',
    'section[data-component="MostRead"] ol',
    'div[data-testid="most-read"] ol',
    'section[data-testid="most-read"] ol',
    'div[data-entityid*="most-popular"] ol',
    'section[data-entityid*="most-popular"] ol',
)

PROMO_GRID_SELECTORS = (
    'div.ssrcss-1euvvif-Wrap ul[class*="-Grid"]',
    '[data-entityid="container-top-stories#1"] ul',
    'section[data-component="top-stories"] ul',
)


class ScrapeValidationError(RuntimeError):
    """Raised when the BBC response no longer satisfies the collection contract."""


def _select_first(soup: BeautifulSoup, selectors: tuple[str, ...]):
    for selector in selectors:
        if node := soup.select_one(selector):
            return node
    return None


def scrape_most_read(soup: BeautifulSoup, limit: int = 10) -> list[dict[str, object]]:
    container = _select_first(soup, MOST_READ_SELECTORS)
    if container is None:
        return []
    stories: list[dict[str, object]] = []
    for position, item in enumerate(container.find_all("li", limit=limit), start=1):
        link = item.find("a")
        if not link or not link.get("href"):
            continue
        title = link.get_text(" ", strip=True)
        if title:
            stories.append({"position": position, "title": title, "url": link["href"]})
    return stories


def scrape_front_page_promos(soup: BeautifulSoup, limit: int = 10) -> list[dict[str, object]]:
    container = _select_first(soup, PROMO_GRID_SELECTORS)
    links = container.find_all("a") if container else soup.select('a[class*="-PromoLink"]')
    stories: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for link in links:
        href = link.get("href")
        if not href or href in seen_urls:
            continue
        headline = link.select_one('[class*="-PromoHeadline"]')
        if headline is None and link.parent is not None:
            headline = link.parent.select_one('[class*="-PromoHeadline"]')
        title = (headline or link).get_text(" ", strip=True)
        if not title:
            continue
        seen_urls.add(href)
        stories.append({"position": len(stories) + 1, "title": title, "url": href})
        if len(stories) >= limit:
            break
    return stories


def parse_homepage(html: str, observed_at: datetime | None = None) -> tuple[Observation, ...]:
    observed_at = (observed_at or utc_now()).astimezone(timezone.utc)
    soup = BeautifulSoup(html, "html.parser")
    groups = {
        "most_read": scrape_most_read(soup),
        "front_page": scrape_front_page_promos(soup),
    }
    missing = [name for name, rows in groups.items() if not rows]
    if missing:
        raise ScrapeValidationError(f"No observations found for: {', '.join(missing)}")

    scrape_id = stable_id(observed_at.isoformat(), "bbc-news-home")
    observations = tuple(
        Observation.create(
            observed_at=observed_at,
            surface=surface,
            position=int(row["position"]),
            title=str(row["title"]),
            url=str(row["url"]),
            scrape_id=scrape_id,
        )
        for surface, rows in groups.items()
        for row in rows
    )
    return observations


def collect_homepage(
    *,
    url: str = BBC_NEWS_URL,
    observed_at: datetime | None = None,
    session: requests.Session | None = None,
) -> ScrapeResult:
    started_at = utc_now()
    observed_at = (observed_at or started_at).astimezone(timezone.utc)
    scrape_id = stable_id(observed_at.isoformat(), "bbc-news-home")
    client = session or requests.Session()
    status: int | None = None
    try:
        response = client.get(
            url,
            headers={"User-Agent": DEFAULT_USER_AGENT},
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        status = response.status_code
        response.raise_for_status()
        observations = parse_homepage(response.text, observed_at)
        counts = {
            surface: sum(item.surface == surface for item in observations)
            for surface in ("most_read", "front_page")
        }
        run = ScrapeRun(
            scrape_id=scrape_id,
            started_at=started_at,
            completed_at=utc_now(),
            success=True,
            http_status=status,
            most_read_count=counts["most_read"],
            front_page_count=counts["front_page"],
            workflow_run_url=_workflow_run_url(),
        )
        return ScrapeResult(observations=observations, run=run)
    except Exception as exc:
        run = ScrapeRun(
            scrape_id=scrape_id,
            started_at=started_at,
            completed_at=utc_now(),
            success=False,
            http_status=status,
            most_read_count=0,
            front_page_count=0,
            error=f"{type(exc).__name__}: {exc}",
            workflow_run_url=_workflow_run_url(),
        )
        raise ScrapeValidationError(run.error) from exc


def _workflow_run_url() -> str:
    server = os.getenv("GITHUB_SERVER_URL")
    repository = os.getenv("GITHUB_REPOSITORY")
    run_id = os.getenv("GITHUB_RUN_ID")
    if all((server, repository, run_id)):
        return f"{server}/{repository}/actions/runs/{run_id}"
    return ""
