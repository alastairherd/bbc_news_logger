"""Bounded DeepSeek inference for structured BBC News story signals."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import requests

DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-v4-flash"
MAX_RUN_BUDGET_USD = Decimal("1.00")
MAX_INPUT_BYTES = 32_000
MAX_OUTPUT_TOKENS = 256
MAX_BATCH_SIZE = 8
REQUEST_OVERHEAD_TOKEN_ALLOWANCE = 1_024
PROMPT_VERSION = "2026-07-13-v1"

# DeepSeek V4 Flash prices in USD per million tokens, published 13 July 2026.
INPUT_CACHE_HIT_USD_PER_MILLION = Decimal("0.0028")
INPUT_CACHE_MISS_USD_PER_MILLION = Decimal("0.14")
OUTPUT_USD_PER_MILLION = Decimal("0.28")
MILLION = Decimal("1000000")

ALLOWED_TOPICS = {
    "politics",
    "world",
    "business",
    "science_and_environment",
    "technology",
    "health",
    "culture",
    "sport",
    "other",
}

SYSTEM_PROMPT = """You label BBC News articles for longitudinal news analysis.
Return one valid JSON object and no markdown. Use only facts in the supplied article.
The object must contain:
- topic: one of politics, world, business, science_and_environment, technology, health, culture,
  sport, other
- themes: 1 to 5 short, reusable thematic labels, ordered most important first
- summary: one neutral sentence
- named_entities: up to 8 important people, organisations, or places
- event_label: a short, specific label for the real-world event or continuing story
- event_type: a reusable event category such as election, conflict, court_case, policy_change,
  disaster, business_deal, sporting_event, or other
- story_form: one of breaking_news, update, analysis, explainer, reaction, feature,
  live_coverage, or other

Use stable general labels for themes and event_type. Keep event_label specific enough that later
articles about the same event can be compared with it."""

BATCH_SYSTEM_PROMPT = """You label BBC News articles for longitudinal news analysis.
The user supplies a JSON object containing an articles array. Return one valid JSON object with an
articles array in the same order and no markdown. Copy each article id exactly. For each article,
use only facts in its supplied text and include:
- topic: one of politics, world, business, science_and_environment, technology, health, culture,
  sport, other
- themes: 1 to 5 short reusable labels
- summary: one neutral sentence
- named_entities: up to 8 important people, organisations, or places
- event_label: a short specific label for the real-world event or continuing story
- event_type: a reusable category such as election, conflict, court_case, policy_change, disaster,
  business_deal, sporting_event, or other
- story_form: one of breaking_news, update, analysis, explainer, reaction, feature, live_coverage,
  or other

Do not omit an article. Use stable general themes and event types. Keep each event label specific
enough that later articles about the same event can be compared with it."""


class DeepSeekError(RuntimeError):
    """Raised when DeepSeek returns an unusable response."""


class BudgetExceeded(RuntimeError):
    """Raised before a request that could exceed the configured run budget."""


@dataclass(frozen=True)
class SemanticSignals:
    topic: str
    themes: tuple[str, ...]
    summary: str
    named_entities: tuple[str, ...]
    event_label: str
    event_type: str
    story_form: str


@dataclass(frozen=True)
class TokenUsage:
    prompt_tokens: int
    prompt_cache_hit_tokens: int
    prompt_cache_miss_tokens: int
    completion_tokens: int

    @property
    def cost_usd(self) -> Decimal:
        return token_cost_usd(
            cache_hit_input_tokens=self.prompt_cache_hit_tokens,
            cache_miss_input_tokens=self.prompt_cache_miss_tokens,
            output_tokens=self.completion_tokens,
        )


@dataclass(frozen=True)
class DeepSeekResult:
    signals: SemanticSignals
    usage: TokenUsage
    response_id: str


@dataclass(frozen=True)
class DeepSeekBatchResult:
    signals: tuple[tuple[str, SemanticSignals], ...]
    usage: TokenUsage
    response_id: str


@dataclass
class RunBudget:
    maximum_usd: Decimal
    spent_usd: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if self.maximum_usd <= 0:
            raise ValueError("The run budget must be greater than zero")
        if self.maximum_usd > MAX_RUN_BUDGET_USD:
            raise ValueError(f"The run budget cannot exceed ${MAX_RUN_BUDGET_USD}")

    @property
    def remaining_usd(self) -> Decimal:
        return self.maximum_usd - self.spent_usd

    def reserve(self, maximum_request_cost_usd: Decimal) -> None:
        if maximum_request_cost_usd > self.remaining_usd:
            raise BudgetExceeded(
                "The next bounded request could exceed the remaining run budget "
                f"(${self.remaining_usd:.6f} remaining)"
            )

    def record(self, actual_cost_usd: Decimal) -> None:
        self.spent_usd += actual_cost_usd
        if self.spent_usd > self.maximum_usd:
            raise BudgetExceeded(
                f"DeepSeek reported ${self.spent_usd:.6f} of usage, above the "
                f"${self.maximum_usd:.2f} run budget"
            )


def token_cost_usd(
    *, cache_hit_input_tokens: int, cache_miss_input_tokens: int, output_tokens: int
) -> Decimal:
    """Calculate V4 Flash cost from the token counters returned by DeepSeek."""

    return (
        Decimal(cache_hit_input_tokens) * INPUT_CACHE_HIT_USD_PER_MILLION
        + Decimal(cache_miss_input_tokens) * INPUT_CACHE_MISS_USD_PER_MILLION
        + Decimal(output_tokens) * OUTPUT_USD_PER_MILLION
    ) / MILLION


def truncate_utf8(text: str, maximum_bytes: int = MAX_INPUT_BYTES) -> str:
    """Bound article input without splitting a UTF-8 code point."""

    encoded = text.encode("utf-8")
    if len(encoded) <= maximum_bytes:
        return text
    return encoded[:maximum_bytes].decode("utf-8", errors="ignore")


def maximum_request_cost_usd(article: str) -> Decimal:
    """Return a conservative upper bound used before sending a request.

    Modern BPE tokenizers cannot emit more tokens than the UTF-8 bytes they consume. The
    additional allowance covers the system prompt and chat-message framing.
    """

    bounded = truncate_utf8(article)
    maximum_prompt_tokens = (
        len(SYSTEM_PROMPT.encode("utf-8"))
        + len(bounded.encode("utf-8"))
        + REQUEST_OVERHEAD_TOKEN_ALLOWANCE
    )
    return token_cost_usd(
        cache_hit_input_tokens=0,
        cache_miss_input_tokens=maximum_prompt_tokens,
        output_tokens=MAX_OUTPUT_TOKENS,
    )


def maximum_batch_request_cost_usd(articles: Sequence[tuple[str, str]]) -> Decimal:
    """Conservative upper bound for one multi-article request."""

    if not 1 <= len(articles) <= MAX_BATCH_SIZE:
        raise ValueError(f"DeepSeek batches must contain 1 to {MAX_BATCH_SIZE} articles")
    bounded = {
        "articles": [
            {"id": article_id, "text": truncate_utf8(text)} for article_id, text in articles
        ]
    }
    maximum_prompt_tokens = (
        len(BATCH_SYSTEM_PROMPT.encode("utf-8"))
        + len(json.dumps(bounded, ensure_ascii=False).encode("utf-8"))
        + REQUEST_OVERHEAD_TOKEN_ALLOWANCE
    )
    return token_cost_usd(
        cache_hit_input_tokens=0,
        cache_miss_input_tokens=maximum_prompt_tokens,
        output_tokens=MAX_OUTPUT_TOKENS * len(articles),
    )


def _string_list(
    value: Any, *, field: str, maximum: int, allow_empty: bool = False
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise DeepSeekError(f"DeepSeek field {field!r} was not a JSON array")
    cleaned = tuple(str(item).strip() for item in value if str(item).strip())
    if not cleaned and not allow_empty:
        raise DeepSeekError(f"DeepSeek field {field!r} was empty")
    return cleaned[:maximum]


def parse_signals(content: str) -> SemanticSignals:
    """Validate the model's JSON response before it reaches the public dataset."""

    try:
        value = json.loads(content)
    except (TypeError, json.JSONDecodeError) as exc:
        raise DeepSeekError("DeepSeek did not return valid JSON") from exc
    if not isinstance(value, dict):
        raise DeepSeekError("DeepSeek returned JSON that was not an object")

    topic = str(value.get("topic", "")).strip().lower()
    if topic not in ALLOWED_TOPICS:
        raise DeepSeekError(f"DeepSeek returned an unsupported topic: {topic!r}")
    summary = str(value.get("summary", "")).strip()
    event_label = str(value.get("event_label", "")).strip()
    if not summary or not event_label:
        raise DeepSeekError("DeepSeek omitted summary or event_label")

    return SemanticSignals(
        topic=topic,
        themes=_string_list(value.get("themes"), field="themes", maximum=5),
        summary=summary,
        named_entities=_string_list(
            value.get("named_entities"),
            field="named_entities",
            maximum=8,
            allow_empty=True,
        ),
        event_label=event_label,
        event_type=str(value.get("event_type", "other")).strip().lower() or "other",
        story_form=str(value.get("story_form", "other")).strip().lower() or "other",
    )


def parse_signal_batch(
    content: str, expected_ids: Sequence[str]
) -> tuple[tuple[str, SemanticSignals], ...]:
    """Validate a complete batch and retain the caller's deterministic order."""

    try:
        value = json.loads(content)
    except (TypeError, json.JSONDecodeError) as exc:
        raise DeepSeekError("DeepSeek did not return valid batch JSON") from exc
    rows = value.get("articles") if isinstance(value, dict) else None
    if not isinstance(rows, list):
        raise DeepSeekError("DeepSeek batch response did not contain an articles array")
    by_id: dict[str, SemanticSignals] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise DeepSeekError("DeepSeek batch contained a non-object article")
        article_id = str(row.get("id", ""))
        if not article_id or article_id in by_id:
            raise DeepSeekError("DeepSeek batch contained a missing or duplicate article id")
        signal_fields = dict(row)
        signal_fields.pop("id", None)
        by_id[article_id] = parse_signals(json.dumps(signal_fields))
    if set(by_id) != set(expected_ids):
        raise DeepSeekError("DeepSeek batch did not return exactly the requested article ids")
    return tuple((article_id, by_id[article_id]) for article_id in expected_ids)


def parse_usage(value: dict[str, Any]) -> TokenUsage:
    prompt_tokens = int(value.get("prompt_tokens", 0))
    details = value.get("prompt_tokens_details") or {}
    cache_hit = int(
        value.get("prompt_cache_hit_tokens", details.get("cached_tokens", 0)) or 0
    )
    cache_miss_value = value.get("prompt_cache_miss_tokens")
    cache_miss = (
        int(cache_miss_value)
        if cache_miss_value is not None
        else max(0, prompt_tokens - cache_hit)
    )
    return TokenUsage(
        prompt_tokens=prompt_tokens,
        prompt_cache_hit_tokens=cache_hit,
        prompt_cache_miss_tokens=cache_miss,
        completion_tokens=int(value.get("completion_tokens", 0)),
    )


class DeepSeekClient:
    """Small synchronous client with one attempt per explicitly budgeted request."""

    def __init__(
        self,
        api_key: str,
        *,
        session: requests.Session | None = None,
        timeout_seconds: int = 120,
    ) -> None:
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY is required for semantic enrichment")
        self.api_key = api_key
        self.session = session
        self.timeout_seconds = timeout_seconds

    def _complete(
        self, *, system_prompt: str, user_content: str, max_tokens: int
    ) -> dict[str, Any]:
        http = self.session or requests
        response = http.post(
            DEEPSEEK_API_URL,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": DEEPSEEK_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                "response_format": {"type": "json_object"},
                "thinking": {"type": "disabled"},
                "max_tokens": max_tokens,
                "stream": False,
            },
            timeout=(10, self.timeout_seconds),
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            try:
                message = response.json().get("error", {}).get("message", response.text)
            except requests.JSONDecodeError:
                message = response.text
            raise DeepSeekError(
                f"DeepSeek request failed with HTTP {response.status_code}: {str(message)[:500]}"
            ) from exc

        try:
            payload = response.json()
        except requests.JSONDecodeError as exc:
            raise DeepSeekError("DeepSeek returned a non-JSON completion response") from exc
        if not isinstance(payload, dict):
            raise DeepSeekError("DeepSeek returned a non-object completion response")
        return payload

    def enrich(self, article: str) -> DeepSeekResult:
        payload = self._complete(
            system_prompt=SYSTEM_PROMPT,
            user_content=truncate_utf8(article),
            max_tokens=MAX_OUTPUT_TOKENS,
        )
        try:
            content = payload["choices"][0]["message"]["content"]
            usage = parse_usage(payload["usage"])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise DeepSeekError("DeepSeek returned an incomplete completion response") from exc
        return DeepSeekResult(
            signals=parse_signals(content),
            usage=usage,
            response_id=str(payload.get("id", "")),
        )

    def enrich_batch(self, articles: Sequence[tuple[str, str]]) -> DeepSeekBatchResult:
        if not 1 <= len(articles) <= MAX_BATCH_SIZE:
            raise ValueError(f"DeepSeek batches must contain 1 to {MAX_BATCH_SIZE} articles")
        expected_ids = tuple(article_id for article_id, _ in articles)
        if len(set(expected_ids)) != len(expected_ids):
            raise ValueError("DeepSeek batch article ids must be unique")
        user_content = json.dumps(
            {
                "articles": [
                    {"id": article_id, "text": truncate_utf8(text)}
                    for article_id, text in articles
                ]
            },
            ensure_ascii=False,
        )
        payload = self._complete(
            system_prompt=BATCH_SYSTEM_PROMPT,
            user_content=user_content,
            max_tokens=MAX_OUTPUT_TOKENS * len(articles),
        )
        try:
            content = payload["choices"][0]["message"]["content"]
            usage = parse_usage(payload["usage"])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise DeepSeekError("DeepSeek returned an incomplete batch response") from exc
        return DeepSeekBatchResult(
            signals=parse_signal_batch(content, expected_ids),
            usage=usage,
            response_id=str(payload.get("id", "")),
        )
