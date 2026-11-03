from __future__ import annotations

from bbc_news_logger.ontology import (
    ONTOLOGY_VERSION,
    normalize_entities,
    normalize_entity,
    normalize_event_type,
    normalize_theme,
    normalize_themes,
    reconcile_signal_rows,
)


def test_normalize_event_type_maps_synonyms_and_preserves_distinct_types() -> None:
    assert normalize_event_type("Legal_Proceeding") == "court_case"
    assert normalize_event_type("murder-trial") == "court_case"
    assert normalize_event_type("Natural Disaster") == "disaster"
    assert normalize_event_type("trade_dispute") == "business_deal"
    assert normalize_event_type("referendum") == "election"
    assert normalize_event_type("diplomacy") == "diplomacy"
    assert normalize_event_type("") == "other"


def test_normalize_theme_merges_aliases_and_preserves_proper_nouns() -> None:
    assert normalize_theme("UK_Politics") == "UK politics"
    assert normalize_theme("british politics") == "UK politics"
    assert normalize_theme("Russia-Ukraine conflict") == "Ukraine war"
    assert normalize_theme("war in Ukraine") == "Ukraine war"
    assert normalize_theme("protests") == "protest"
    assert normalize_theme("nhs waiting lists") == "NHS waiting times"
    assert normalize_theme("world cup qualifier") == "World Cup qualifier"
    assert normalize_theme("strait of hormuz") == "Strait of Hormuz"
    # "criminal trial" and "hostage release" must not be widened into "murder trial" / "crisis".
    assert normalize_theme("criminal trial") == "criminal trial"
    assert normalize_theme("hostage release") == "hostage release"
    # "Met" must not be blindly expanded into "Met Police" or "Met Office".
    assert normalize_theme("met gala fashion") == "met gala fashion"
    assert normalize_themes(
        ["Protests", "protest", "UK_politics", "cyber-attack", "cyber attack"]
    ) == [
        "protest",
        "UK politics",
        "cyber-attack",
    ]


def test_normalize_entity_protects_places_and_handles_honorifics_and_institutions() -> None:
    assert normalize_entity("Sir Keir Starmer") == "Keir Starmer"
    assert normalize_entity("Keir Starmer's") == "Keir Starmer"
    assert normalize_entity("Lord Mandelson") == "Peter Mandelson"
    assert normalize_entity("Baroness Cumberlege") == "Julia Cumberlege"
    assert normalize_entity("Prince Andrew") == "Andrew Mountbatten-Windsor"
    assert normalize_entity("Andrew Mountbatten Windsor") == "Andrew Mountbatten-Windsor"
    assert normalize_entity("The White House") == "White House"
    assert normalize_entity("The Met Police") == "Met Police"
    assert normalize_entity("The Met Office") == "Met Office"
    assert normalize_entity("Guardian") == "The Guardian"
    # Institutional / territorial titles and works must not have their title prefix stripped.
    assert normalize_entity("Archbishop of Canterbury") == "Archbishop of Canterbury"
    assert normalize_entity("Mayor of Paris") == "Mayor of Paris"
    assert normalize_entity("General Medical Council") == "General Medical Council"
    assert normalize_entity("Mr Bates vs The Post Office") == "Mr Bates vs The Post Office"
    # Single-word possessive brand names remain intact.
    assert normalize_entity("Sainsbury's") == "Sainsbury's"
    assert normalize_entity("McDonald's") == "McDonald's"
    # Place, country, show, and newspaper names starting with "The " must remain intact.
    assert normalize_entity("The Hague") == "The Hague"
    assert normalize_entity("The Gambia") == "The Gambia"
    assert normalize_entity("The Netherlands") == "The Netherlands"
    assert normalize_entity("The Traitors") == "The Traitors"
    assert normalize_entity("The Times") == "The Times"
    # "Great Britain" (e.g. Olympic team) must not be collapsed into "UK".
    assert normalize_entity("Great Britain") == "Great Britain"
    assert normalize_entity("United Kingdom") == "UK"
    assert normalize_entities(["Sir Keir Starmer", "Keir Starmer", "The Hague"]) == [
        "Keir Starmer",
        "The Hague",
    ]


def test_reconcile_signal_rows_is_deterministic_and_non_mutating() -> None:
    raw = {
        "h1": {
            "themes": ["UK Politics", "protests", "asylum seeker", "cyber-attack"],
            "named_entities": ["Sir Keir Starmer", "The White House"],
            "event_type": "Policy Announcement",
        },
        "h2": {
            "themes": ["british politics", "Protests", "asylum seekers", "cyber attack"],
            "named_entities": ["Keir Starmer", "White House"],
            "event_type": "policy_change",
        },
        "h3": {
            "themes": ["asylum seekers", "cyber-attack"],
            "named_entities": ["Archbishop of Canterbury", "General Medical Council"],
            "event_type": "other",
        },
    }
    reconciled, theme_aliases, entity_aliases = reconcile_signal_rows(raw)
    assert ONTOLOGY_VERSION == "2026-11-v2"
    assert raw["h1"]["themes"] == ["UK Politics", "protests", "asylum seeker", "cyber-attack"]
    expected_themes = ["UK politics", "protest", "asylum seekers", "cyber-attack"]
    assert reconciled["h1"]["themes"] == expected_themes
    assert reconciled["h2"]["themes"] == expected_themes
    assert reconciled["h1"]["named_entities"] == ["Keir Starmer", "White House"]
    assert reconciled["h3"]["named_entities"] == [
        "Archbishop of Canterbury",
        "General Medical Council",
    ]
    assert reconciled["h1"]["event_type"] == "policy_change"
    assert "british politics" in theme_aliases["UK politics"]
    assert "Sir Keir Starmer" in entity_aliases["Keir Starmer"]
