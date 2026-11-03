"""Incremental theme hierarchy (parent categories) and Wikidata entity linking."""

from __future__ import annotations

import json
import re
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pyarrow as pa
import requests

from .deepseek import (
    DEEPSEEK_MODEL,
    REQUEST_OVERHEAD_TOKEN_ALLOWANCE,
    DeepSeekClient,
    DeepSeekError,
    token_cost_usd,
)
from .ontology import reconcile_signal_rows

TAXONOMY_PATH = "semantic/taxonomy/latest.parquet"
WIKIDATA_PATH = "semantic/wikidata/latest.parquet"

TAXONOMY_PROMPT_VERSION = "2026-11-v1"
WIKIDATA_VERSION = "2026-11-v1"

MIN_THEME_STORIES = 5
MIN_ENTITY_STORIES = 5
MAX_ENTITIES = 400
TAXONOMY_BATCH_SIZE = 50
MAX_TAXONOMY_OUTPUT_TOKENS = 2_400
MAX_TAXONOMY_RUN_COST_USD = Decimal("0.20")

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_USER_AGENT = (
    "bbc-news-logger/1.0 (https://github.com/alastairherd/bbc_news_logger; research archive)"
)

# Controlled mid-level parent categories (~45 nodes) mapped to their top-level domain.
PARENT_THEME_DOMAIN: dict[str, str] = {
    "UK government & Westminster": "politics",
    "Elections & voting": "politics",
    "Political parties & campaigns": "politics",
    "Public policy & legislation": "politics",
    "Devolution & constitutional affairs": "politics",
    "Middle East conflict": "world",
    "Russia-Ukraine war": "world",
    "US politics & society": "world",
    "European affairs": "world",
    "Asia-Pacific affairs": "world",
    "Africa & Americas": "world",
    "International diplomacy & alliances": "world",
    "Defence, security & armed forces": "world",
    "Macroeconomy & fiscal policy": "business",
    "Cost of living & household finances": "business",
    "Housing & property": "business",
    "Energy & utilities": "business",
    "Transport & travel": "business",
    "Retail, hospitality & consumer": "business",
    "Corporate & financial markets": "business",
    "Employment, pay & industrial relations": "business",
    "Policing & law enforcement": "other",
    "Courts, trials & sentencing": "other",
    "Violent & organised crime": "other",
    "Prisons, justice & legal system": "other",
    "Migration, asylum & borders": "politics",
    "Protests, civil unrest & rights": "other",
    "NHS & healthcare services": "health",
    "Diseases, conditions & treatments": "health",
    "Public health, diet & wellbeing": "health",
    "Mental health & social care": "health",
    "Schools, universities & education": "education",
    "Children, families & welfare": "education",
    "Climate change & net zero": "science_and_environment",
    "Extreme weather & natural disasters": "science_and_environment",
    "Nature, wildlife & pollution": "science_and_environment",
    "Space, physics & earth sciences": "science_and_environment",
    "Artificial intelligence & computing": "technology",
    "Cybersecurity, social media & online safety": "technology",
    "Film, TV & broadcasting": "culture",
    "Music, arts & live events": "culture",
    "Royals, celebrities & public figures": "culture",
    "Football": "sport",
    "Cricket, rugby & tennis": "sport",
    "Motorsport, Olympics & other sport": "sport",
    "Human interest & community": "other",
}

ALLOWED_PARENT_THEMES = tuple(PARENT_THEME_DOMAIN.keys())
DEFAULT_PARENT_THEME = "Human interest & community"

TAXONOMY_SYSTEM_PROMPT = f"""You organise canonical BBC News thematic labels into a controlled
mid-level taxonomy. The user supplies a JSON object with a "themes" array. Each entry has "theme"
and "top_topic". Return one valid JSON object with a "mappings" array in the same order and no
markdown. Each item in "mappings" must have:
- "theme": copied verbatim from the input
- "parent_theme": MUST be one of the following exact strings:
{json.dumps(ALLOWED_PARENT_THEMES, indent=2)}

Pick the single best-fitting parent_theme for each theme based on UK/BBC News usage."""

TAXONOMY_SCHEMA = pa.schema(
    [
        pa.field("theme", pa.string(), nullable=False),
        pa.field("parent_theme", pa.string(), nullable=False),
        pa.field("top_topic", pa.string(), nullable=False),
        pa.field("model", pa.string(), nullable=False),
        pa.field("prompt_version", pa.string(), nullable=False),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("cost_usd", pa.float64(), nullable=False),
    ]
)

WIKIDATA_SCHEMA = pa.schema(
    [
        pa.field("entity", pa.string(), nullable=False),
        pa.field("qid", pa.string(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("description", pa.string(), nullable=False),
        pa.field("entity_type", pa.string(), nullable=False),
        pa.field("wikidata_url", pa.string(), nullable=False),
        pa.field("version", pa.string(), nullable=False),
        pa.field("generated_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

# Deterministic overrides for common BBC News abbreviations or ambiguous short names
# so they always resolve to the intended UK/international concept.
CURATED_ENTITY_QIDS: dict[str, dict[str, str]] = {
    "uk": {
        "qid": "Q145",
        "label": "United Kingdom",
        "description": "island country in North-West Europe",
        "entity_type": "location",
    },
    "us": {
        "qid": "Q30",
        "label": "United States",
        "description": "country primarily located in North America",
        "entity_type": "location",
    },
    "eu": {
        "qid": "Q458",
        "label": "European Union",
        "description": "political and economic union of 27 member states in Europe",
        "entity_type": "organisation",
    },
    "un": {
        "qid": "Q1065",
        "label": "United Nations",
        "description": "intergovernmental organization",
        "entity_type": "organisation",
    },
    "nhs": {
        "qid": "Q918396",
        "label": "National Health Service",
        "description": "publicly funded healthcare systems in the United Kingdom",
        "entity_type": "organisation",
    },
    "bbc": {
        "qid": "Q9531",
        "label": "BBC",
        "description": "British public service broadcaster",
        "entity_type": "organisation",
    },
    "bbc news": {
        "qid": "Q1160945",
        "label": "BBC News",
        "description": "operational business division of the BBC",
        "entity_type": "organisation",
    },
    "met police": {
        "qid": "Q1138524",
        "label": "Metropolitan Police",
        "description": "territorial police force in Greater London",
        "entity_type": "organisation",
    },
    "met office": {
        "qid": "Q1851405",
        "label": "Met Office",
        "description": "United Kingdom's national weather service",
        "entity_type": "organisation",
    },
    "downing street": {
        "qid": "Q161885",
        "label": "10 Downing Street",
        "description": "official residence and office of the Prime Minister of the United Kingdom",
        "entity_type": "organisation",
    },
    "white house": {
        "qid": "Q35525",
        "label": "White House",
        "description": "official residence and workplace of the president of the United States",
        "entity_type": "organisation",
    },
    "uk government": {
        "qid": "Q6063",
        "label": "Government of the United Kingdom",
        "description": "central executive authority of the United Kingdom",
        "entity_type": "organisation",
    },
    "us government": {
        "qid": "Q48525",
        "label": "Federal Government of the United States",
        "description": "national government of the United States",
        "entity_type": "organisation",
    },
    "labour party": {
        "qid": "Q9630",
        "label": "Labour Party",
        "description": "centre-left political party in the United Kingdom",
        "entity_type": "organisation",
    },
    "conservative party": {
        "qid": "Q9626",
        "label": "Conservative Party",
        "description": "centre-right political party in the United Kingdom",
        "entity_type": "organisation",
    },
    "liberal democrats": {
        "qid": "Q9624",
        "label": "Liberal Democrats",
        "description": "liberal political party in the United Kingdom",
        "entity_type": "organisation",
    },
    "reform uk": {
        "qid": "Q59776158",
        "label": "Reform UK",
        "description": "right-wing populist political party in the United Kingdom",
        "entity_type": "organisation",
    },
    "scottish national party": {
        "qid": "Q10658",
        "label": "Scottish National Party",
        "description": "Scottish nationalist and social-democratic political party",
        "entity_type": "organisation",
    },
    "green party": {
        "qid": "Q9669",
        "label": "Green Party of England and Wales",
        "description": "green political party in England and Wales",
        "entity_type": "organisation",
    },
    "parliament": {
        "qid": "Q11010",
        "label": "Parliament of the United Kingdom",
        "description": "supreme legislative body of the United Kingdom",
        "entity_type": "organisation",
    },
    "supreme court": {
        "qid": "Q193055",
        "label": "Supreme Court of the United Kingdom",
        "description": "final court of appeal in the United Kingdom for civil and criminal cases",
        "entity_type": "organisation",
    },
    "high court": {
        "qid": "Q1618019",
        "label": "High Court of Justice",
        "description": "senior court of England and Wales",
        "entity_type": "organisation",
    },
    "home office": {
        "qid": "Q763386",
        "label": "Home Office",
        "description": "United Kingdom government ministerial department",
        "entity_type": "organisation",
    },
    "treasury": {
        "qid": "Q614729",
        "label": "HM Treasury",
        "description": (
            "UK government department responsible for public finance and economic policy"
        ),
        "entity_type": "organisation",
    },
    "foreign office": {
        "qid": "Q358834",
        "label": "Foreign, Commonwealth and Development Office",
        "description": "department of the Government of the United Kingdom",
        "entity_type": "organisation",
    },
    "ministry of defence": {
        "qid": "Q1142650",
        "label": "Ministry of Defence",
        "description": "United Kingdom government department responsible for defence",
        "entity_type": "organisation",
    },
    "ministry of justice": {
        "qid": "Q1779600",
        "label": "Ministry of Justice",
        "description": "ministerial department of the UK Government",
        "entity_type": "organisation",
    },
    "andrew mountbatten-windsor": {
        "qid": "Q153330",
        "label": "Prince Andrew, Duke of York",
        "description": "member of the British royal family",
        "entity_type": "person",
    },
    "andrew bailey": {
        "qid": "Q4756272",
        "label": "Andrew Bailey",
        "description": "Governor of the Bank of England",
        "entity_type": "person",
    },
    "nasa": {
        "qid": "Q23548",
        "label": "NASA",
        "description": "United States government aerospace and space agency",
        "entity_type": "organisation",
    },
    "europe": {
        "qid": "Q46",
        "label": "Europe",
        "description": "continent in the Northern Hemisphere",
        "entity_type": "location",
    },
    "house of lords": {
        "qid": "Q11007",
        "label": "House of Lords",
        "description": "upper house of the Parliament of the United Kingdom",
        "entity_type": "organisation",
    },
    "house of commons": {
        "qid": "Q11005",
        "label": "House of Commons",
        "description": "lower house of the Parliament of the United Kingdom",
        "entity_type": "organisation",
    },
    "queen elizabeth ii": {
        "qid": "Q9682",
        "label": "Elizabeth II",
        "description": (
            "Queen of the United Kingdom and other Commonwealth realms from 1952 to 2022"
        ),
        "entity_type": "person",
    },
    "sydney": {
        "qid": "Q3130",
        "label": "Sydney",
        "description": "capital city of New South Wales, Australia",
        "entity_type": "location",
    },
    "epping": {
        "qid": "Q2116414",
        "label": "Epping",
        "description": "market town in Essex, England",
        "entity_type": "location",
    },
    "donbas": {
        "qid": "Q605714",
        "label": "Donbas",
        "description": "historical, cultural, and economic region in eastern Ukraine",
        "entity_type": "location",
    },
    "ice": {
        "qid": "Q943091",
        "label": "U.S. Immigration and Customs Enforcement",
        "description": "United States federal law enforcement agency",
        "entity_type": "organisation",
    },
    "rac": {
        "qid": "Q7275645",
        "label": "RAC Limited",
        "description": "British automotive services company",
        "entity_type": "organisation",
    },
    "x": {
        "qid": "Q918",
        "label": "X",
        "description": "online social media and social networking service (formerly Twitter)",
        "entity_type": "organisation",
    },
    "samir shah": {
        "qid": "Q7409525",
        "label": "Samir Shah",
        "description": "British television and radio executive, Chair of the BBC",
        "entity_type": "person",
    },
    "greater manchester police": {
        "qid": "Q1544348",
        "label": "Greater Manchester Police",
        "description": "territorial police force in Greater Manchester, England",
        "entity_type": "organisation",
    },
    "chelsea": {
        "qid": "Q9616",
        "label": "Chelsea F.C.",
        "description": "association football club in London, England",
        "entity_type": "organisation",
    },
    "wimbledon": {
        "qid": "Q41520",
        "label": "The Championships, Wimbledon",
        "description": "Grand Slam tennis tournament held in London",
        "entity_type": "event",
    },
    "panorama": {
        "qid": "Q2049820",
        "label": "Panorama",
        "description": "BBC current affairs documentary television programme",
        "entity_type": "organisation",
    },
}

_PERSON_HINT = re.compile(
    r"\b(?:politician|prime minister|president|monarch|king|queen|prince|princess|heir apparent|"
    r"younger son|elder son|daughter of|son of|supreme leader|first minister|mayor|speaker of|"
    r"secretary of|director of|governor of|chancellor|minister|mp|senator|representative|diplomat|"
    r"actor|actress|singer|musician|footballer|player|manager|coach|administrator|journalist|"
    r"presenter|writer|author|businessman|businesswoman|executive|entrepreneur|"
    r"billionaire|socialite|judge|lawyer|barrister|bishop|archbishop|rabbi|pope|activist|"
    r"advocate|scientist|researcher|astronaut|doctor|physician|soldier|general|officer|"
    r"convicted|murdered|student|man responsible|woman|briton|born\s+\d{4}|\d{4}\s*[–-]\s*\d{4})\b",
    flags=re.IGNORECASE,
)
_LOCATION_HINT = re.compile(
    r"\b(?:country|countries|state|sovereign state|city|capital|town|village|suburb|area of|"
    r"region|province|county|borough|island|continent|sea|ocean|strait|channel|river|mountain|"
    r"beach|park|castle|palace|lodge|house in|stadium|airport|station|peninsula|territory|"
    r"constituency|london borough)\b",
    flags=re.IGNORECASE,
)
_EVENT_HINT = re.compile(
    r"\b(?:war|conflict|election|by-election|tournament|championship|competition|festival|award|"
    r"ceremony|summit|conference|pandemic|epidemic|hurricane|storm|disaster|crisis|trial|"
    r"mission|inquiry)\b",
    flags=re.IGNORECASE,
)
_ORG_HINT = re.compile(
    r"\b(?:party|organization|organisation|company|business|corporation|airline|bank|supermarket|"
    r"retailer|broadcaster|network|television series|programme|newspaper|magazine|university|"
    r"school|college|institute|think tank|foundation|hospital|service|department|ministry|agency|"
    r"body|institution|parliament|house in the parliament|legislature|police|force|court|"
    r"council|union|federation|association|league|club|team|charity|group|movement|armed forces|"
    r"military|band|platform|social network|chatbot|database)\b",
    flags=re.IGNORECASE,
)


def infer_entity_type_from_description(description: str, label: str = "") -> str:
    """Classify a Wikidata description into person, organisation, location, event, or other."""

    text = f"{description} {label}".strip()
    if not text:
        return "other"
    if _PERSON_HINT.search(description):
        return "person"
    if _ORG_HINT.search(description):
        return "organisation"
    if _LOCATION_HINT.search(description):
        return "location"
    if _EVENT_HINT.search(description):
        return "event"
    if _ORG_HINT.search(label):
        return "organisation"
    if _LOCATION_HINT.search(label):
        return "location"
    return "other"


def heuristic_parent_theme(theme: str, top_topic: str = "other") -> str:
    """Deterministic fallback mapping when a theme has not yet been classified by DeepSeek."""

    lower = theme.casefold()
    if any(
        k in lower
        for k in (
            "israel",
            "gaza",
            "hamas",
            "hezbollah",
            "iran",
            "lebanon",
            "syria",
            "yemen",
            "red sea",
            "strait of hormuz",
            "west bank",
            "middle east",
        )
    ):
        return "Middle East conflict"
    if any(
        k in lower for k in ("ukraine", "russia", "putin", "zelensky", "nato", "crimea", "donbas")
    ):
        return "Russia-Ukraine war"
    if any(
        k in lower
        for k in (
            "football",
            "premier league",
            "champions league",
            "world cup",
            "fa cup",
            "euros",
            "womens super league",
        )
    ):
        return "Football"
    if any(k in lower for k in ("cricket", "rugby", "six nations", "tennis", "wimbledon")):
        return "Cricket, rugby & tennis"
    if any(
        k in lower
        for k in (
            "f1",
            "formula 1",
            "olympics",
            "paralympics",
            "golf",
            "boxing",
            "athletics",
            "cycling",
            "snooker",
            "darts",
        )
    ):
        return "Motorsport, Olympics & other sport"
    if any(
        k in lower
        for k in (
            "nhs",
            "hospital",
            "gp ",
            "ambulance",
            "a&e",
            "waiting time",
            "waiting list",
            "midwife",
            "maternity",
            "dentist",
        )
    ):
        return "NHS & healthcare services"
    if any(
        k in lower
        for k in (
            "mental health",
            "suicide",
            "dementia",
            "autism",
            "adhd",
            "social care",
            "care home",
        )
    ):
        return "Mental health & social care"
    if any(
        k in lower
        for k in (
            "cancer",
            "vaccine",
            "covid",
            "virus",
            "disease",
            "infection",
            "assisted dying",
            "drug",
            "medicine",
            "surgery",
        )
    ):
        return "Diseases, conditions & treatments"
    if any(
        k in lower
        for k in ("obesity", "diet", "smoking", "vaping", "alcohol", "nutrition", "fitness")
    ):
        return "Public health, diet & wellbeing"
    if any(
        k in lower
        for k in (
            "asylum",
            "migrant",
            "immigration",
            "small boat",
            "channel crossing",
            "border",
            "deportation",
            "refugee",
            "visa",
        )
    ):
        return "Migration, asylum & borders"
    if any(
        k in lower for k in ("election", "by-election", "referendum", "voting", "voter", "polling")
    ):
        return "Elections & voting"
    if any(
        k in lower
        for k in (
            "labour",
            "conservative",
            "tory",
            "reform uk",
            "liberal democrat",
            "snp",
            "green party",
        )
    ):
        return "Political parties & campaigns"
    if any(
        k in lower
        for k in (
            "scotland",
            "welsh",
            "wales",
            "northern ireland",
            "devolution",
            "stormont",
            "holyrood",
        )
    ):
        return "Devolution & constitutional affairs"
    if any(
        k in lower
        for k in ("police", "policing", "met police", "arrest", "manhunt", "missing person")
    ):
        return "Policing & law enforcement"
    if any(
        k in lower
        for k in (
            "court",
            "trial",
            "inquest",
            "sentencing",
            "verdict",
            "lawsuit",
            "inquiry",
            "tribunal",
        )
    ):
        return "Courts, trials & sentencing"
    if any(
        k in lower
        for k in (
            "murder",
            "stabbing",
            "shooting",
            "knife crime",
            "sexual assault",
            "rape",
            "domestic abuse",
            "gang",
            "terror",
        )
    ):
        return "Violent & organised crime"
    if any(k in lower for k in ("prison", "jail", "justice", "probation", "parole")):
        return "Prisons, justice & legal system"
    if any(
        k in lower
        for k in ("protest", "demonstration", "riot", "unrest", "activism", "free speech")
    ):
        return "Protests, civil unrest & rights"
    if any(
        k in lower
        for k in (
            "cost of living",
            "inflation",
            "food price",
            "energy bill",
            "water bill",
            "benefit",
            "pension",
            "personal finance",
        )
    ):
        return "Cost of living & household finances"
    if any(
        k in lower
        for k in ("housing", "mortgage", "rent", "landlord", "property", "homelessness", "planning")
    ):
        return "Housing & property"
    if any(
        k in lower
        for k in (
            "rail",
            "train",
            "flight",
            "airline",
            "airport",
            "aviation",
            "road",
            "driving",
            "ev ",
            "electric vehicle",
            "hs2",
            "ferry",
            "bus",
        )
    ):
        return "Transport & travel"
    if any(
        k in lower
        for k in (
            "oil",
            "gas",
            "nuclear",
            "wind",
            "solar",
            "grid",
            "ofgem",
            "thames water",
            "water company",
        )
    ):
        return "Energy & utilities"
    if any(
        k in lower
        for k in (
            "strike",
            "pay",
            "wage",
            "union",
            "employment",
            "unemployment",
            "job",
            "workplace",
        )
    ):
        return "Employment, pay & industrial relations"
    if any(
        k in lower
        for k in (
            "tax",
            "budget",
            "gdp",
            "recession",
            "interest rate",
            "bank of england",
            "borrowing",
            "tariff",
            "trade",
        )
    ):
        return "Macroeconomy & fiscal policy"
    if any(
        k in lower
        for k in (
            "retail",
            "supermarket",
            "high street",
            "hospitality",
            "restaurant",
            "pub",
            "shopping",
            "consumer",
        )
    ):
        return "Retail, hospitality & consumer"
    if any(
        k in lower
        for k in (
            "school",
            "university",
            "student",
            "teacher",
            "exam",
            "ofsted",
            "education",
            "tuition",
        )
    ):
        return "Schools, universities & education"
    if any(
        k in lower
        for k in ("child", "family", "parent", "nursery", "childcare", "youth", "welfare")
    ):
        return "Children, families & welfare"
    if any(
        k in lower
        for k in ("climate", "emissions", "carbon", "net zero", "cop29", "cop30", "global warming")
    ):
        return "Climate change & net zero"
    if any(
        k in lower
        for k in (
            "weather",
            "storm",
            "flood",
            "wildfire",
            "heatwave",
            "earthquake",
            "hurricane",
            "drought",
        )
    ):
        return "Extreme weather & natural disasters"
    if any(
        k in lower
        for k in (
            "wildlife",
            "animal",
            "conservation",
            "biodiversity",
            "sewage",
            "pollution",
            "plastic",
            "environment",
        )
    ):
        return "Nature, wildlife & pollution"
    if any(
        k in lower
        for k in (
            "space",
            "nasa",
            "astronomy",
            "satellite",
            "moon",
            "mars",
            "physics",
            "archaeology",
            "dinosaur",
        )
    ):
        return "Space, physics & earth sciences"
    if any(
        k in lower
        for k in (
            "ai",
            "artificial intelligence",
            "openai",
            "chatgpt",
            "robot",
            "quantum",
            "semiconductor",
            "chip",
        )
    ):
        return "Artificial intelligence & computing"
    if any(
        k in lower
        for k in (
            "cyber",
            "hack",
            "scam",
            "fraud",
            "social media",
            "tiktok",
            "online safety",
            "smartphone",
            "data",
        )
    ):
        return "Cybersecurity, social media & online safety"
    if any(
        k in lower for k in ("royal", "king charles", "prince", "princess", "monarchy", "celebrity")
    ):
        return "Royals, celebrities & public figures"
    if any(
        k in lower
        for k in (
            "bbc",
            "tv",
            "television",
            "film",
            "cinema",
            "oscar",
            "bafta",
            "streaming",
            "media",
            "journalism",
        )
    ):
        return "Film, TV & broadcasting"
    if any(
        k in lower
        for k in (
            "music",
            "glastonbury",
            "eurovision",
            "concert",
            "theatre",
            "art",
            "museum",
            "book",
            "literature",
        )
    ):
        return "Music, arts & live events"
    if any(k in lower for k in ("us ", "trump", "biden", "white house", "washington", "american")):
        return "US politics & society"
    if any(
        k in lower
        for k in ("china", "taiwan", "india", "pakistan", "japan", "korea", "australia", "asia")
    ):
        return "Asia-Pacific affairs"
    if any(k in lower for k in ("france", "germany", "eu ", "european", "spain", "italy")):
        return "European affairs"
    if any(
        k in lower
        for k in (
            "defence",
            "military",
            "army",
            "navy",
            "raf",
            "weapon",
            "nuclear deterrent",
            "espionage",
        )
    ):
        return "Defence, security & armed forces"

    topic_defaults = {
        "politics": "UK government & Westminster",
        "world": "International diplomacy & alliances",
        "business": "Corporate & financial markets",
        "health": "NHS & healthcare services",
        "education": "Schools, universities & education",
        "science_and_environment": "Nature, wildlife & pollution",
        "technology": "Cybersecurity, social media & online safety",
        "culture": "Film, TV & broadcasting",
        "sport": "Motorsport, Olympics & other sport",
    }
    return topic_defaults.get(top_topic, DEFAULT_PARENT_THEME)


def extract_active_concepts(
    articles: pa.Table,
    signals: pa.Table,
) -> tuple[list[tuple[str, str, int]], list[tuple[str, int]]]:
    """Return active canonical themes `(theme, top_topic, story_count)` and top entities."""

    article_by_hash: dict[str, dict[str, Any]] = {}
    for row in articles.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        if not content_hash or not row.get("fetch_ok"):
            continue
        existing = article_by_hash.get(content_hash)
        if existing is None or row["fetched_at"] > existing["fetched_at"]:
            article_by_hash[content_hash] = row

    raw_signals: dict[str, dict[str, Any]] = {}
    for row in signals.to_pylist():
        content_hash = str(row.get("content_sha256") or "")
        existing = raw_signals.get(content_hash)
        if content_hash and (existing is None or row["generated_at"] > existing["generated_at"]):
            raw_signals[content_hash] = dict(row)

    signal_by_hash, _, _ = reconcile_signal_rows(raw_signals)

    latest_by_story: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for content_hash in article_by_hash.keys() & signal_by_hash.keys():
        article = article_by_hash[content_hash]
        story_id = str(article["story_id"])
        existing = latest_by_story.get(story_id)
        if existing is None or article["fetched_at"] > existing[0]["fetched_at"]:
            latest_by_story[story_id] = (article, signal_by_hash[content_hash])

    theme_stories: defaultdict[str, set[str]] = defaultdict(set)
    theme_topics: defaultdict[str, Counter[str]] = defaultdict(Counter)
    entity_stories: defaultdict[str, set[str]] = defaultdict(set)

    for story_id, (_article, signal) in latest_by_story.items():
        topic = str(signal.get("topic") or "other")
        for theme in set(str(t) for t in signal.get("themes") or [] if str(t)):
            theme_stories[theme].add(story_id)
            theme_topics[theme][topic] += 1
        for entity in set(str(e) for e in signal.get("named_entities") or [] if str(e)):
            entity_stories[entity].add(story_id)

    active_themes = [
        (
            theme,
            theme_topics[theme].most_common(1)[0][0] if theme_topics[theme] else "other",
            len(story_ids),
        )
        for theme, story_ids in sorted(
            theme_stories.items(), key=lambda item: (-len(item[1]), item[0])
        )
        if len(story_ids) >= MIN_THEME_STORIES
    ]

    active_entities = [
        (entity, len(story_ids))
        for entity, story_ids in sorted(
            entity_stories.items(), key=lambda item: (-len(item[1]), item[0])
        )
        if len(story_ids) >= MIN_ENTITY_STORIES
    ][:MAX_ENTITIES]

    return active_themes, active_entities


def classify_themes_with_deepseek(
    themes: Sequence[tuple[str, str]],
    *,
    client: DeepSeekClient,
    max_cost_usd: Decimal = MAX_TAXONOMY_RUN_COST_USD,
) -> tuple[list[dict[str, Any]], Decimal]:
    """Classify `(theme, top_topic)` pairs into `ALLOWED_PARENT_THEMES` in bounded batches."""

    allowed_set = set(ALLOWED_PARENT_THEMES)
    allowed_lower = {name.casefold(): name for name in ALLOWED_PARENT_THEMES}
    rows: list[dict[str, Any]] = []
    spent = Decimal("0")

    for start in range(0, len(themes), TAXONOMY_BATCH_SIZE):
        batch = themes[start : start + TAXONOMY_BATCH_SIZE]
        payload = {"themes": [{"theme": t, "top_topic": top_topic} for t, top_topic in batch]}
        user_content = json.dumps(payload, ensure_ascii=False)
        est_cost = token_cost_usd(
            cache_hit_input_tokens=0,
            cache_miss_input_tokens=(
                len(TAXONOMY_SYSTEM_PROMPT.encode("utf-8"))
                + len(user_content.encode("utf-8"))
                + REQUEST_OVERHEAD_TOKEN_ALLOWANCE
            ),
            output_tokens=MAX_TAXONOMY_OUTPUT_TOKENS,
            peak=True,
        )
        if spent + est_cost > max_cost_usd:
            break

        try:
            content, usage = client.complete_json(
                system_prompt=TAXONOMY_SYSTEM_PROMPT,
                user_content=user_content,
                max_tokens=MAX_TAXONOMY_OUTPUT_TOKENS,
            )
        except DeepSeekError:
            continue

        spent += usage.cost_usd
        mappings = content.get("mappings")
        by_theme: dict[str, str] = {}
        if isinstance(mappings, list):
            for entry in mappings:
                if not isinstance(entry, dict):
                    continue
                t_name = str(entry.get("theme") or "").strip()
                p_name = str(entry.get("parent_theme") or "").strip()
                if p_name in allowed_set:
                    by_theme[t_name.casefold()] = p_name
                elif p_name.casefold() in allowed_lower:
                    by_theme[t_name.casefold()] = allowed_lower[p_name.casefold()]

        generated_at = datetime.now(timezone.utc)
        per_row_cost = float(usage.cost_usd) / max(len(batch), 1)
        for theme_name, top_topic in batch:
            parent = by_theme.get(theme_name.casefold()) or heuristic_parent_theme(
                theme_name, top_topic
            )
            rows.append(
                {
                    "theme": theme_name,
                    "parent_theme": parent,
                    "top_topic": top_topic,
                    "model": DEEPSEEK_MODEL,
                    "prompt_version": TAXONOMY_PROMPT_VERSION,
                    "generated_at": generated_at,
                    "cost_usd": per_row_cost,
                }
            )

    return rows, spent


def resolve_entity_wikidata(
    entity: str,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Resolve a canonical entity name to a Wikidata Q-ID, label, description, and type."""

    now = datetime.now(timezone.utc)
    folded = entity.casefold()
    if folded in CURATED_ENTITY_QIDS:
        curated = CURATED_ENTITY_QIDS[folded]
        qid = curated["qid"]
        return {
            "entity": entity,
            "qid": qid,
            "label": curated["label"],
            "description": curated["description"],
            "entity_type": curated["entity_type"],
            "wikidata_url": f"https://www.wikidata.org/wiki/{qid}",
            "version": WIKIDATA_VERSION,
            "generated_at": now,
        }

    http = session or requests.Session()
    candidates: list[dict[str, Any]] = []
    for attempt in range(3):
        try:
            response = http.get(
                WIKIDATA_API_URL,
                params={
                    "action": "wbsearchentities",
                    "search": entity,
                    "language": "en",
                    "uselang": "en",
                    "type": "item",
                    "limit": 5,
                    "format": "json",
                },
                headers={"User-Agent": WIKIDATA_USER_AGENT},
                timeout=10,
            )
            if response.status_code == 429:
                time.sleep(0.5 * (attempt + 1))
                continue
            response.raise_for_status()
            candidates = response.json().get("search") or []
            break
        except Exception:
            time.sleep(0.3 * (attempt + 1))

    best: dict[str, Any] | None = None
    for candidate in candidates:
        desc = str(candidate.get("description") or "").strip()
        lower_desc = desc.lower()
        # Skip disambiguation pages, Wikimedia templates, and scholarly articles.
        if any(
            skip in lower_desc
            for skip in (
                "disambiguation",
                "wikimedia",
                "wikipedia",
                "scientific article",
                "scholarly article",
                "family name",
                "given name",
            )
        ):
            continue
        best = candidate
        break

    if best is None and candidates:
        best = candidates[0]

    if best is None:
        return {
            "entity": entity,
            "qid": "",
            "label": entity,
            "description": "",
            "entity_type": "other",
            "wikidata_url": "",
            "version": WIKIDATA_VERSION,
            "generated_at": now,
        }

    qid = str(best.get("id") or "")
    label = str(best.get("label") or entity)
    description = str(best.get("description") or "")
    entity_type = infer_entity_type_from_description(description, label)
    return {
        "entity": entity,
        "qid": qid,
        "label": label,
        "description": description,
        "entity_type": entity_type,
        "wikidata_url": f"https://www.wikidata.org/wiki/{qid}" if qid else "",
        "version": WIKIDATA_VERSION,
        "generated_at": now,
    }


def resolve_entities_wikidata(
    entities: Iterable[str],
    *,
    delay_seconds: float = 0.05,
) -> list[dict[str, Any]]:
    """Resolve a sequence of canonical entity names against Wikidata with polite rate limiting."""

    session = requests.Session()
    results: list[dict[str, Any]] = []
    for entity in entities:
        row = resolve_entity_wikidata(entity, session=session)
        results.append(row)
        if entity.casefold() not in CURATED_ENTITY_QIDS and delay_seconds > 0:
            time.sleep(delay_seconds)
    return results
