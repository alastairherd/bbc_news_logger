from datetime import datetime, timezone

import pytest

from bbc_news_logger.scrape import ScrapeValidationError, parse_homepage

HOMEPAGE = """
<html><body>
  <section data-component="mostRead"><ol>
    <li><a href="/news/articles/one">Most read one</a></li>
    <li><a href="/news/articles/two">Most read two</a></li>
  </ol></section>
  <section data-component="top-stories"><ul>
    <li><a href="/news/articles/two"><span class="PromoHeadline">Front two</span></a></li>
    <li><a href="/news/articles/three"><span class="PromoHeadline">Front three</span></a></li>
  </ul></section>
</body></html>
"""


def test_parse_homepage_creates_stable_typed_observations() -> None:
    observed_at = datetime(2026, 7, 13, 10, tzinfo=timezone.utc)
    rows = parse_homepage(HOMEPAGE, observed_at)

    assert len(rows) == 4
    assert {row.surface for row in rows} == {"front_page", "most_read"}
    assert {row.position for row in rows if row.surface == "most_read"} == {1, 2}
    assert len({row.scrape_id for row in rows}) == 1
    assert all(row.url.startswith("https://www.bbc.co.uk/news/") for row in rows)
    shared = [row for row in rows if row.url.endswith("/two")]
    assert len({row.story_id for row in shared}) == 1


def test_parse_homepage_rejects_missing_surface() -> None:
    with pytest.raises(ScrapeValidationError, match="most_read"):
        parse_homepage("<html><body><a class='x-PromoLink' href='/news/a'>A</a></body></html>")
