"""Repairs for headline text captured before the 28 October 2026 scraper fix."""

from __future__ import annotations

import re

# Each misread character is a UTF-8 lead byte followed by continuation bytes, decoded as Latin-1.
MISREAD_UTF8 = re.compile(r"[Â-ô][\u0080-¿]{1,3}")
# Video and audio promos stored their screen-reader label too: "Headline. Video, 00:01:08 Headline".
MEDIA_DUPLICATE = re.compile(
    r"^(?P<headline>.+?)\.?\s*(?:Video|Audio)(?:,\s*\d{2}:\d{2}:\d{2})?\s*(?P=headline)$"
)
# Live promos stored their hidden "Live. " label as a prefix. Its non-breaking space was misread
# as "Â\xa0", and whitespace stripping sometimes left the lone "Â" behind.
LIVE_PREFIX = re.compile(r"^Live\s*\.(?:\s*\u00c2\s*|\s+)")


def repair_misread_utf8(text: str) -> str:
    def decode(match: re.Match[str]) -> str:
        try:
            return match.group(0).encode("latin-1").decode("utf-8")
        except UnicodeDecodeError:
            return match.group(0)

    return MISREAD_UTF8.sub(decode, text)


def repair_title(title: str) -> str:
    """Return the visible headline; titles that need no repair are returned unchanged."""

    repaired = repair_misread_utf8(title)
    if match := MEDIA_DUPLICATE.match(repaired):
        repaired = match.group("headline")
    return LIVE_PREFIX.sub("", repaired, count=1)
