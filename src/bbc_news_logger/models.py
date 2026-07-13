"""Typed records and stable identifiers used throughout the pipeline."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit

from .config import BBC_BASE_URL


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_url(url: str) -> str:
    """Return a stable canonical form suitable for joins and identifiers."""

    absolute = urljoin(BBC_BASE_URL, (url or "").strip())
    parts = urlsplit(absolute)
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, "", ""))


def stable_id(*parts: object, length: int = 32) -> str:
    value = "\x1f".join(str(part) for part in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


@dataclass(frozen=True)
class Observation:
    observed_at: datetime
    surface: str
    position: int
    title: str
    url: str
    story_id: str
    scrape_id: str

    @classmethod
    def create(
        cls,
        *,
        observed_at: datetime,
        surface: str,
        position: int,
        title: str,
        url: str,
        scrape_id: str | None = None,
    ) -> Observation:
        canonical = normalize_url(url)
        observed_at = observed_at.astimezone(timezone.utc)
        batch_id = scrape_id or stable_id(observed_at.isoformat(), surface)
        return cls(
            observed_at=observed_at,
            surface=surface,
            position=int(position),
            title=title.strip(),
            url=canonical,
            story_id=stable_id(canonical),
            scrape_id=batch_id,
        )


@dataclass(frozen=True)
class ScrapeRun:
    scrape_id: str
    started_at: datetime
    completed_at: datetime
    success: bool
    http_status: int | None
    most_read_count: int
    front_page_count: int
    selector_version: str = "2026-07"
    error: str = ""
    workflow_run_url: str = ""


@dataclass(frozen=True)
class ScrapeResult:
    observations: tuple[Observation, ...]
    run: ScrapeRun


@dataclass(frozen=True)
class ArticleSnapshot:
    snapshot_id: str
    requested_url: str
    canonical_url: str
    story_id: str
    first_observed_at: datetime
    fetched_at: datetime
    fetched_at_is_inferred: bool
    title: str
    authors: tuple[str, ...] = field(default_factory=tuple)
    article_text: str = ""
    article_html: str = ""
    content_sha256: str = ""
    html_sha256: str = ""
    http_status: int | None = None
    fetch_ok: bool = False

    @classmethod
    def create(
        cls,
        *,
        requested_url: str,
        canonical_url: str,
        first_observed_at: datetime,
        fetched_at: datetime,
        title: str,
        authors: list[str] | tuple[str, ...],
        article_text: str,
        article_html: str,
        http_status: int | None,
        fetch_ok: bool,
        fetched_at_is_inferred: bool = False,
    ) -> ArticleSnapshot:
        canonical = normalize_url(canonical_url or requested_url)
        text_hash = hashlib.sha256(article_text.encode("utf-8")).hexdigest()
        html_hash = hashlib.sha256(article_html.encode("utf-8")).hexdigest()
        fetched_at = fetched_at.astimezone(timezone.utc)
        return cls(
            snapshot_id=stable_id(canonical, fetched_at.isoformat(), text_hash),
            requested_url=normalize_url(requested_url),
            canonical_url=canonical,
            story_id=stable_id(canonical),
            first_observed_at=first_observed_at.astimezone(timezone.utc),
            fetched_at=fetched_at,
            fetched_at_is_inferred=fetched_at_is_inferred,
            title=title.strip(),
            authors=tuple(sorted({author.strip() for author in authors if author.strip()})),
            article_text=article_text,
            article_html=article_html,
            content_sha256=text_hash,
            html_sha256=html_hash,
            http_status=http_status,
            fetch_ok=bool(fetch_ok),
        )
