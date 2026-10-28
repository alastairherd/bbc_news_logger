import pytest

from bbc_news_logger.titles import repair_title


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        (
            "Confusion about FlÃ¡vio Bolsonaro's whereabouts",
            "Confusion about Flávio Bolsonaro's whereabouts",
        ),
        ("I get Â£4,062 a year", "I get £4,062 a year"),
        (
            "Live .Â\xa0Chancellor John Healey delivering his first Budget",
            "Chancellor John Healey delivering his first Budget",
        ),
        (
            "Overturned carriages at crash. Video, 00:01:08 Overturned carriages at crash",
            "Overturned carriages at crash",
        ),
        (
            "Watch: What happens now? Video, 00:00:44 Watch: What happens now?",
            "Watch: What happens now?",
        ),
        (
            "Peter Kay gatecrashes weather report. VideoPeter Kay gatecrashes weather report",
            "Peter Kay gatecrashes weather report",
        ),
        ("Live.ÂMan, 26, arrested on suspicion", "Man, 26, arrested on suspicion"),
        ("Live music venues face closure", "Live music venues face closure"),
        ("Café owner wins award", "Café owner wins award"),
        ("Video shows rescue at sea", "Video shows rescue at sea"),
    ],
)
def test_repair_title_restores_the_visible_headline(stored: str, expected: str) -> None:
    assert repair_title(stored) == expected
