from datetime import datetime, timezone

import pytest

from bbc_news_logger.scrape import ScrapeValidationError, decode_homepage, parse_homepage

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


PROMO_PAGE = """
<html><body>
  <a class="x-PromoLink" href="/news/live/lead"><span role="text">
    <div class="x-LiveContainer">Live<span class="visually-hidden">.&#160;</span></div>
    <div class="x-Headline"><h2 type="headline">Chancellor delivers first Budget</h2></div>
  </span></a>
  <a class="x-PromoLink" href="/news/videos/clip"><span role="text"><p class="x-PromoHeadline">
    <span class="visually-hidden x-VisuallyHidden">Carriages at crash site. Video, 00:01:08</span>
    <span aria-hidden="true">Carriages at crash site</span></p></span></a>
  <section data-component="mostRead"><ol>
    <li><a class="x-PromoLink" href="/news/articles/read"><p class="x-PromoHeadline">
      <span aria-hidden="false">Most read headline</span></p></a></li>
  </ol></section>
</body></html>
"""


def test_promo_titles_exclude_screen_reader_labels() -> None:
    rows = parse_homepage(PROMO_PAGE, datetime(2026, 10, 28, 15, tzinfo=timezone.utc))
    titles = {row.surface: [] for row in rows}
    for row in rows:
        titles[row.surface].append(row.title)

    assert titles["front_page"][:2] == [
        "Chancellor delivers first Budget",
        "Carriages at crash site",
    ]
    assert titles["most_read"] == ["Most read headline"]


@pytest.mark.parametrize(
    ("content_type", "expected"),
    [
        ("text/html", "Flávio £4,062"),
        ("text/html; charset=utf-8", "Flávio £4,062"),
        ('text/html; charset="UTF-8"', "Flávio £4,062"),
    ],
)
def test_homepage_without_charset_is_decoded_as_utf8(content_type: str, expected: str) -> None:
    assert decode_homepage("Flávio £4,062".encode(), content_type) == expected
