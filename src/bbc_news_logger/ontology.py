"""Deterministic ontology normalization and alias reconciliation for BBC News story signals."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

ONTOLOGY_VERSION = "2026-11-v1"

CORE_EVENT_TYPES = frozenset(
    {
        "election",
        "conflict",
        "court_case",
        "policy_change",
        "disaster",
        "business_deal",
        "sporting_event",
        "other",
    }
)

EVENT_TYPE_ALIASES: dict[str, str] = {
    "legal_proceeding": "court_case",
    "legal_proceedings": "court_case",
    "legal_case": "court_case",
    "court_ruling": "court_case",
    "court_hearing": "court_case",
    "criminal_trial": "court_case",
    "murder_trial": "court_case",
    "inquest": "court_case",
    "lawsuit": "court_case",
    "tribunal": "court_case",
    "sentencing": "court_case",
    "prosecution": "court_case",
    "natural_disaster": "disaster",
    "severe_weather": "disaster",
    "weather_event": "disaster",
    "accident": "disaster",
    "transport_accident": "disaster",
    "aviation_accident": "disaster",
    "rail_accident": "disaster",
    "industrial_accident": "disaster",
    "wildfire": "disaster",
    "flood": "disaster",
    "flooding": "disaster",
    "earthquake": "disaster",
    "hurricane": "disaster",
    "storm": "disaster",
    "armed_conflict": "conflict",
    "military_conflict": "conflict",
    "war": "conflict",
    "airstrike": "conflict",
    "military_strike": "conflict",
    "ceasefire": "conflict",
    "hostage_release": "conflict",
    "peace_talks": "conflict",
    "policy_announcement": "policy_change",
    "legislation": "policy_change",
    "regulation": "policy_change",
    "government_policy": "policy_change",
    "budget": "policy_change",
    "tax_policy": "policy_change",
    "welfare_reform": "policy_change",
    "general_election": "election",
    "local_election": "election",
    "by_election": "election",
    "presidential_election": "election",
    "referendum": "election",
    "primary_election": "election",
    "leadership_election": "election",
    "merger": "business_deal",
    "acquisition": "business_deal",
    "takeover": "business_deal",
    "trade_agreement": "business_deal",
    "trade_deal": "business_deal",
    "trade_dispute": "business_deal",
    "corporate_restructuring": "business_deal",
    "investment": "business_deal",
    "sports_event": "sporting_event",
    "sport_event": "sporting_event",
    "tournament": "sporting_event",
    "championship": "sporting_event",
    "match": "sporting_event",
    "transfer": "sporting_event",
}

PROPER_TOKEN_CASE: dict[str, str] = {
    "uk": "UK",
    "us": "US",
    "usa": "US",
    "eu": "EU",
    "un": "UN",
    "nhs": "NHS",
    "bbc": "BBC",
    "nato": "NATO",
    "mp": "MP",
    "mps": "MPs",
    "gdp": "GDP",
    "ai": "AI",
    "imf": "IMF",
    "who": "WHO",
    "wto": "WTO",
    "icc": "ICC",
    "icj": "ICJ",
    "idf": "IDF",
    "hmrc": "HMRC",
    "ofcom": "Ofcom",
    "ofgem": "Ofgem",
    "ofwat": "Ofwat",
    "ofsted": "Ofsted",
    "bafta": "BAFTA",
    "fifa": "FIFA",
    "uefa": "UEFA",
    "f1": "F1",
    "lgbtq": "LGBTQ",
    "hs2": "HS2",
    "cop29": "COP29",
    "cop30": "COP30",
    "g7": "G7",
    "g20": "G20",
}

PROPER_PHRASE_CASE: dict[str, str] = {
    "gaza": "Gaza",
    "ukraine": "Ukraine",
    "russia": "Russia",
    "israel": "Israel",
    "iran": "Iran",
    "china": "China",
    "taiwan": "Taiwan",
    "syria": "Syria",
    "lebanon": "Lebanon",
    "sudan": "Sudan",
    "yemen": "Yemen",
    "india": "India",
    "pakistan": "Pakistan",
    "canada": "Canada",
    "australia": "Australia",
    "france": "France",
    "germany": "Germany",
    "europe": "Europe",
    "england": "England",
    "scotland": "Scotland",
    "wales": "Wales",
    "northern ireland": "Northern Ireland",
    "london": "London",
    "middle east": "Middle East",
    "world cup": "World Cup",
    "premier league": "Premier League",
    "champions league": "Champions League",
    "six nations": "Six Nations",
    "olympics": "Olympics",
    "winter olympics": "Winter Olympics",
    "paralympics": "Paralympics",
    "wimbledon": "Wimbledon",
    "eurovision": "Eurovision",
    "oscars": "Oscars",
    "glastonbury": "Glastonbury",
    "ramadan": "Ramadan",
    "christmas": "Christmas",
    "easter": "Easter",
    "holocaust": "Holocaust",
    "islam": "Islam",
    "christianity": "Christianity",
    "judaism": "Judaism",
    "catholic church": "Catholic Church",
    "church of england": "Church of England",
    "labour party": "Labour Party",
    "conservative party": "Conservative Party",
    "liberal democrats": "Liberal Democrats",
    "reform uk": "Reform UK",
    "green party": "Green Party",
    "scottish national party": "Scottish National Party",
    "plaid cymru": "Plaid Cymru",
    "sinn fein": "Sinn Fein",
    "downing street": "Downing Street",
    "white house": "White House",
    "parliament": "Parliament",
    "supreme court": "Supreme Court",
    "high court": "High Court",
    "bank of england": "Bank of England",
    "met police": "Met Police",
    "met office": "Met Office",
    "post office": "Post Office",
    "royal family": "Royal Family",
    "royal mail": "Royal Mail",
    "british airways": "British Airways",
    "manchester united": "Manchester United",
    "manchester city": "Manchester City",
    "real madrid": "Real Madrid",
}

THEME_ALIASES: dict[str, str] = {
    "protests": "protest",
    "public protest": "protest",
    "demonstrations": "protest",
    "tributes": "tribute",
    "memorials": "tribute",
    "wildfires": "wildfire",
    "floods": "flooding",
    "Severe weather": "extreme weather",
    "severe weather": "extreme weather",
    "weather warning": "weather warnings",
    "train cancellations": "rail disruption",
    "rail cancellations": "rail disruption",
    "court proceedings": "court case",
    "court ruling": "court case",
    "court hearing": "court case",
    "legal proceedings": "court case",
    "legal case": "court case",
    "criminal trial": "murder trial",
    "peace negotiations": "peace talks",
    "ceasefire talks": "ceasefire negotiations",
    "hostage release": "hostage crisis",
    "israeli-palestinian conflict": "Israel-Gaza war",
    "israel-hamas conflict": "Israel-Gaza war",
    "israel-gaza conflict": "Israel-Gaza war",
    "gaza conflict": "Israel-Gaza war",
    "gaza war": "Israel-Gaza war",
    "russia-ukraine conflict": "Ukraine war",
    "russia-ukraine war": "Ukraine war",
    "ukraine conflict": "Ukraine war",
    "war in ukraine": "Ukraine war",
    "russian invasion of ukraine": "Ukraine war",
    "us politics": "US politics",
    "u.s. politics": "US politics",
    "american politics": "US politics",
    "uk politics": "UK politics",
    "british politics": "UK politics",
    "uk economy": "UK economy",
    "british economy": "UK economy",
    "us economy": "US economy",
    "u.s. economy": "US economy",
    "american economy": "US economy",
    "cost of living crisis": "cost of living",
    "living costs": "cost of living",
    "artificial intelligence": "AI",
    "generative ai": "AI",
    "national health service": "NHS",
    "nhs waiting lists": "NHS waiting times",
    "uk general election": "general election",
    "us presidential election": "US election",
    "u.s. election": "US election",
}

# Only strip leading "The " for institutions where BBC coverage uses both forms interchangeably.
# Never strip place/publication/show names such as "The Hague", "The Gambia", "The Netherlands",
# "The Traitors", or "The Times".
THE_STRIP_ALLOWLIST = frozenset(
    {
        "white house",
        "pentagon",
        "kremlin",
        "vatican",
        "royal family",
        "met office",
        "met police",
        "post office",
        "bank of england",
        "environment agency",
        "national trust",
        "british museum",
        "premier league",
        "champions league",
        "home office",
        "foreign office",
        "treasury",
        "cabinet office",
        "ministry of defence",
        "ministry of justice",
        "department of health",
        "supreme court",
        "high court",
        "world health organization",
        "united nations",
        "european union",
        "labour party",
        "conservative party",
        "liberal democrats",
        "green party",
        "taliban",
        "oscar",
        "oscars",
        "baftas",
    }
)

# Non-royal titles stripped when followed by a full personal name (at least two words).
STRIPPABLE_TITLES = (
    "Prime Minister ",
    "President ",
    "Chancellor ",
    "Foreign Secretary ",
    "Home Secretary ",
    "Health Secretary ",
    "Defence Secretary ",
    "Defense Secretary ",
    "Education Secretary ",
    "Justice Secretary ",
    "Attorney General ",
    "Mayor ",
    "Pope ",
    "Archbishop ",
    "General ",
    "Admiral ",
    "Colonel ",
    "Chief Constable ",
    "Det Supt ",
    "Dr ",
    "Prof ",
    "Professor ",
    "Sir ",
    "Dame ",
    "Mr ",
    "Mrs ",
    "Ms ",
    "Miss ",
)

ENTITY_ALIASES: dict[str, str] = {
    "united kingdom": "UK",
    "u.k.": "UK",
    "britain": "UK",
    "united states": "US",
    "united states of america": "US",
    "u.s.": "US",
    "u.s.a.": "US",
    "usa": "US",
    "america": "US",
    "european union": "EU",
    "e.u.": "EU",
    "united nations": "UN",
    "u.n.": "UN",
    "world health organization": "WHO",
    "world health organisation": "WHO",
    "international monetary fund": "IMF",
    "international criminal court": "ICC",
    "international court of justice": "ICJ",
    "north atlantic treaty organization": "NATO",
    "north atlantic treaty organisation": "NATO",
    "nato": "NATO",
    "fifa": "FIFA",
    "uefa": "UEFA",
    "nasa": "NASA",
    "bafta": "BAFTA",
    "ofcom": "Ofcom",
    "ofgem": "Ofgem",
    "ofwat": "Ofwat",
    "ofsted": "Ofsted",
    "hmrc": "HMRC",
    "hm revenue and customs": "HMRC",
    "hm revenue & customs": "HMRC",
    "national health service": "NHS",
    "nhs": "NHS",
    "nhs england": "NHS England",
    "bbc": "BBC",
    "bbc news": "BBC News",
    "metropolitan police": "Met Police",
    "metropolitan police service": "Met Police",
    "the met": "Met Police",
    "met police": "Met Police",
    "met office": "Met Office",
    "police scotland": "Police Scotland",
    "psni": "Police Service of Northern Ireland",
    "idf": "Israel Defense Forces",
    "israeli defense forces": "Israel Defense Forces",
    "israeli defence forces": "Israel Defense Forces",
    "israel defence forces": "Israel Defense Forces",
    "israel defense forces": "Israel Defense Forces",
    "uk government": "UK government",
    "british government": "UK government",
    "us government": "US government",
    "no 10": "Downing Street",
    "number 10": "Downing Street",
    "10 downing street": "Downing Street",
    "downing street": "Downing Street",
    "the white house": "White House",
    "white house": "White House",
    "us supreme court": "US Supreme Court",
    "uk supreme court": "Supreme Court",
    "labour": "Labour Party",
    "labour party": "Labour Party",
    "conservatives": "Conservative Party",
    "tories": "Conservative Party",
    "tory party": "Conservative Party",
    "conservative party": "Conservative Party",
    "lib dems": "Liberal Democrats",
    "liberal democrat": "Liberal Democrats",
    "liberal democrats": "Liberal Democrats",
    "snp": "Scottish National Party",
    "scottish national party": "Scottish National Party",
    "reform": "Reform UK",
    "reform uk": "Reform UK",
    "sinn féin": "Sinn Fein",
    "sinn fein": "Sinn Fein",
    "donald j trump": "Donald Trump",
    "donald j. trump": "Donald Trump",
    "president trump": "Donald Trump",
    "trump": "Donald Trump",
    "keir starmer": "Keir Starmer",
    "sir keir starmer": "Keir Starmer",
    "pm keir starmer": "Keir Starmer",
    "rachel reeves": "Rachel Reeves",
    "chancellor rachel reeves": "Rachel Reeves",
    "kemi badenoch": "Kemi Badenoch",
    "rishi sunak": "Rishi Sunak",
    "boris johnson": "Boris Johnson",
    "nigel farage": "Nigel Farage",
    "ed davey": "Ed Davey",
    "sir ed davey": "Ed Davey",
    "john swinney": "John Swinney",
    "sadiq khan": "Sadiq Khan",
    "sir sadiq khan": "Sadiq Khan",
    "andy burnham": "Andy Burnham",
    "yvette cooper": "Yvette Cooper",
    "wes streeting": "Wes Streeting",
    "david lammy": "David Lammy",
    "shabana mahmood": "Shabana Mahmood",
    "bridget phillipson": "Bridget Phillipson",
    "angela rayner": "Angela Rayner",
    "ed miliband": "Ed Miliband",
    "pat mcfadden": "Pat McFadden",
    "john healey": "John Healey",
    "mark rowley": "Mark Rowley",
    "sir mark rowley": "Mark Rowley",
    "chris whitty": "Chris Whitty",
    "sir chris whitty": "Chris Whitty",
    "keithink": "Keir Starmer",
    "lord mandelson": "Peter Mandelson",
    "peter mandelson": "Peter Mandelson",
    "baroness Cumberlege": "Julia Cumberlege",
    "king charles": "King Charles III",
    "king charles iii": "King Charles III",
    "queen camilla": "Queen Camilla",
    "prince william": "Prince William",
    "princess of wales": "Catherine, Princess of Wales",
    "kate middleton": "Catherine, Princess of Wales",
    "catherine, princess of wales": "Catherine, Princess of Wales",
    "prince harry": "Prince Harry",
    "duke of sussex": "Prince Harry",
    "prince andrew": "Andrew Mountbatten-Windsor",
    "duke of york": "Andrew Mountbatten-Windsor",
    "andrew mountbatten-windsor": "Andrew Mountbatten-Windsor",
    "vladimir putin": "Vladimir Putin",
    "president putin": "Vladimir Putin",
    "volodymyr zelensky": "Volodymyr Zelensky",
    "volodymyr zelenskyy": "Volodymyr Zelensky",
    "president zelensky": "Volodymyr Zelensky",
    "benjamin netanyahu": "Benjamin Netanyahu",
    "joe biden": "Joe Biden",
    "president biden": "Joe Biden",
    "kamala harris": "Kamala Harris",
    "jd vance": "JD Vance",
    "j.d. vance": "JD Vance",
    "elon musk": "Elon Musk",
    "mark zuckerberg": "Mark Zuckerberg",
    "sam altman": "Sam Altman",
    "emmanuel macron": "Emmanuel Macron",
    "friedrich merz": "Friedrich Merz",
    "olaf scholz": "Olaf Scholz",
    "ursula von der leyen": "Ursula von der Leyen",
    "mark carney": "Mark Carney",
    "xi jinping": "Xi Jinping",
    "narendra modi": "Narendra Modi",
    "pope francis": "Pope Francis",
    "pope leo xiv": "Pope Leo XIV",
    "ali khamenei": "Ayatollah Ali Khamenei",
    "ayatollah ali khamenei": "Ayatollah Ali Khamenei",
    "mojtaba khamenei": "Mojtaba Khamenei",
}

_WHITESPACE = re.compile(r"\s+")
_WORD_BOUNDARY_PHRASES = tuple(
    sorted(
        (
            (re.compile(rf"\b{re.escape(key)}\b", flags=re.IGNORECASE), canonical)
            for key, canonical in PROPER_PHRASE_CASE.items()
        ),
        key=lambda item: -len(item[1]),
    )
)


def _clean_space(value: object) -> str:
    text = str(value or "").replace("_", " ").strip()
    return _WHITESPACE.sub(" ", text)


def normalize_event_type(value: object) -> str:
    """Normalize event_type strings and map clear off-vocabulary synonyms onto core prompt types."""

    cleaned = re.sub(r"[\s-]+", "_", str(value or "").strip().lower()).strip("_")
    if not cleaned:
        return "other"
    mapped = EVENT_TYPE_ALIASES.get(cleaned, cleaned)
    return mapped if mapped else "other"


def _apply_proper_casing(text: str) -> str:
    tokens = text.split(" ")
    cased_tokens = []
    for token in tokens:
        parts = token.split("-")
        cased_parts = [PROPER_TOKEN_CASE.get(part.casefold(), part) for part in parts]
        cased_tokens.append("-".join(cased_parts))
    result = " ".join(cased_tokens)
    for pattern, replacement in _WORD_BOUNDARY_PHRASES:
        result = pattern.sub(replacement, result)
    return result


def normalize_theme(value: object) -> str:
    """Canonicalize a single free-form theme label while preserving material distinctions."""

    cleaned = _clean_space(value).strip(" .,;:")
    if not cleaned:
        return ""
    folded = cleaned.casefold()
    if folded in THEME_ALIASES:
        return THEME_ALIASES[folded]
    # Standardize lowercase baseline except for recognized proper nouns and acronyms.
    base = cleaned.lower()
    if base in THEME_ALIASES:
        return THEME_ALIASES[base]
    return _apply_proper_casing(base)


def normalize_themes(values: Iterable[object] | None, *, maximum: int = 5) -> list[str]:
    """Return deduplicated canonical themes in priority order."""

    if not values:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        theme = normalize_theme(raw)
        key = theme.casefold()
        if not theme or key in seen:
            continue
        seen.add(key)
        result.append(theme)
        if len(result) >= maximum:
            break
    return result


def normalize_entity(value: object) -> str:
    """Canonicalize a named entity without collapsing distinct places, works, or teams."""

    cleaned = _WHITESPACE.sub(" ", str(value or "").strip()).strip(" .,;:")
    if not cleaned:
        return ""
    if cleaned.endswith("'s") or cleaned.endswith("\u2019s"):
        cleaned = cleaned[:-2].strip()
    folded = cleaned.casefold()
    if folded in ENTITY_ALIASES:
        return ENTITY_ALIASES[folded]

    if folded.startswith("the "):
        remainder = cleaned[4:].strip()
        rem_folded = remainder.casefold()
        if rem_folded in ENTITY_ALIASES:
            return ENTITY_ALIASES[rem_folded]
        if rem_folded in THE_STRIP_ALLOWLIST:
            cleaned = remainder
            folded = rem_folded

    for prefix in STRIPPABLE_TITLES:
        if cleaned.lower().startswith(prefix.lower()):
            remainder = cleaned[len(prefix) :].strip()
            # Only strip a title when followed by a multi-word personal name
            # (e.g. "Sir Keir Starmer").
            if len(remainder.split()) >= 2:
                cleaned = remainder
                folded = cleaned.casefold()
                if folded in ENTITY_ALIASES:
                    return ENTITY_ALIASES[folded]
                break

    if folded in PROPER_TOKEN_CASE:
        return PROPER_TOKEN_CASE[folded]
    if folded in PROPER_PHRASE_CASE:
        return PROPER_PHRASE_CASE[folded]
    return cleaned


def normalize_entities(values: Iterable[object] | None, *, maximum: int = 8) -> list[str]:
    """Return deduplicated canonical named entities in priority order."""

    if not values:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        entity = normalize_entity(raw)
        key = entity.casefold()
        if not entity or key in seen:
            continue
        seen.add(key)
        result.append(entity)
        if len(result) >= maximum:
            break
    return result


def reconcile_signal_rows(
    signal_by_hash: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]], dict[str, list[str]]]:
    """Normalize themes, entities, and event types across a corpus with deterministic tie-breaking.

    Returns:
      - normalized copy of `signal_by_hash`
      - theme_aliases: mapping from canonical theme -> sorted distinct raw variants
      - entity_aliases: mapping from canonical entity -> sorted distinct raw variants
    """

    theme_form_counts: dict[str, Counter[str]] = defaultdict(Counter)
    entity_form_counts: dict[str, Counter[str]] = defaultdict(Counter)
    raw_theme_variants: dict[str, set[str]] = defaultdict(set)
    raw_entity_variants: dict[str, set[str]] = defaultdict(set)

    intermediate: dict[str, dict[str, Any]] = {}
    for content_hash, row in signal_by_hash.items():
        raw_themes = [str(item).strip() for item in (row.get("themes") or []) if str(item).strip()]
        raw_entities = [
            str(item).strip() for item in (row.get("named_entities") or []) if str(item).strip()
        ]
        norm_themes = normalize_themes(raw_themes)
        norm_entities = normalize_entities(raw_entities)

        for raw in raw_themes:
            norm = normalize_theme(raw)
            if norm:
                key = norm.casefold()
                theme_form_counts[key][norm] += 1
                if raw.casefold() != key or raw != norm:
                    raw_theme_variants[key].add(raw)

        for raw in raw_entities:
            norm = normalize_entity(raw)
            if norm:
                key = norm.casefold()
                entity_form_counts[key][norm] += 1
                if raw.casefold() != key or raw != norm:
                    raw_entity_variants[key].add(raw)

        updated = dict(row)
        updated["themes"] = norm_themes
        updated["named_entities"] = norm_entities
        updated["event_type"] = normalize_event_type(row.get("event_type"))
        intermediate[content_hash] = updated

    # Deterministic tie-breaking: highest frequency first, then lexical sort on surface form.
    canonical_theme_by_key: dict[str, str] = {
        key: min(counter.items(), key=lambda item: (-item[1], item[0]))[0]
        for key, counter in theme_form_counts.items()
    }
    canonical_entity_by_key: dict[str, str] = {
        key: min(counter.items(), key=lambda item: (-item[1], item[0]))[0]
        for key, counter in entity_form_counts.items()
    }

    reconciled: dict[str, dict[str, Any]] = {}
    for content_hash, row in intermediate.items():
        row["themes"] = [
            canonical_theme_by_key.get(theme.casefold(), theme) for theme in row["themes"]
        ]
        row["named_entities"] = [
            canonical_entity_by_key.get(entity.casefold(), entity)
            for entity in row["named_entities"]
        ]
        reconciled[content_hash] = row

    theme_aliases: dict[str, list[str]] = {}
    for key, canonical in canonical_theme_by_key.items():
        variants = sorted(
            {
                variant
                for variant in raw_theme_variants.get(key, set())
                if variant != canonical
            }
        )
        theme_aliases[canonical] = variants[:12]

    entity_aliases: dict[str, list[str]] = {}
    for key, canonical in canonical_entity_by_key.items():
        variants = sorted(
            {
                variant
                for variant in raw_entity_variants.get(key, set())
                if variant != canonical
            }
        )
        entity_aliases[canonical] = variants[:12]

    return reconciled, theme_aliases, entity_aliases
