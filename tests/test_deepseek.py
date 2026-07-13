"""Tests for the bounded native DeepSeek integration."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from bbc_news_logger.deepseek import (
    DEEPSEEK_MODEL,
    MAX_INPUT_BYTES,
    MAX_OUTPUT_TOKENS,
    BudgetExceeded,
    DeepSeekClient,
    DeepSeekError,
    RunBudget,
    maximum_request_cost_usd,
    parse_signals,
    parse_usage,
    token_cost_usd,
    truncate_utf8,
)


class FakeResponse:
    status_code = 200
    text = ""

    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self.payload


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.request: dict[str, object] | None = None

    def post(self, _url: str, **kwargs: object) -> FakeResponse:
        self.request = kwargs
        return self.response


def signal_json() -> str:
    return json.dumps(
        {
            "topic": "world",
            "themes": ["diplomacy", "international security"],
            "summary": "Officials met to discuss a new agreement.",
            "named_entities": ["Example Government"],
            "event_label": "Example peace talks",
            "event_type": "diplomacy",
            "story_form": "update",
        }
    )


def test_token_cost_uses_v4_flash_rates() -> None:
    cost = token_cost_usd(
        cache_hit_input_tokens=1_000_000,
        cache_miss_input_tokens=1_000_000,
        output_tokens=1_000_000,
    )
    assert cost == Decimal("0.4228")


def test_run_budget_cannot_be_raised_above_one_dollar() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        RunBudget(Decimal("1.01"))

    budget = RunBudget(Decimal("1.00"))
    budget.record(Decimal("0.99"))
    with pytest.raises(BudgetExceeded, match="remaining run budget"):
        budget.reserve(Decimal("0.02"))


def test_input_is_truncated_on_utf8_boundary() -> None:
    value = truncate_utf8("é" * MAX_INPUT_BYTES)
    assert len(value.encode("utf-8")) <= MAX_INPUT_BYTES
    assert value


def test_worst_case_request_is_small_enough_for_bounded_batches() -> None:
    assert maximum_request_cost_usd("x" * (MAX_INPUT_BYTES * 2)) < Decimal("0.01")


def test_signal_parser_rejects_uncontrolled_topics() -> None:
    payload = json.loads(signal_json())
    payload["topic"] = "celebrity gossip"
    with pytest.raises(DeepSeekError, match="unsupported topic"):
        parse_signals(json.dumps(payload))


def test_usage_parser_accounts_for_cache_hits() -> None:
    usage = parse_usage(
        {
            "prompt_tokens": 1_000,
            "prompt_cache_hit_tokens": 750,
            "prompt_cache_miss_tokens": 250,
            "completion_tokens": 100,
        }
    )
    assert usage.prompt_cache_hit_tokens == 750
    assert usage.prompt_cache_miss_tokens == 250
    assert usage.cost_usd == Decimal("0.0000651")


def test_client_disables_thinking_and_caps_output() -> None:
    response = FakeResponse(
        {
            "id": "completion-1",
            "choices": [{"message": {"content": signal_json()}}],
            "usage": {
                "prompt_tokens": 100,
                "prompt_cache_hit_tokens": 0,
                "prompt_cache_miss_tokens": 100,
                "completion_tokens": 50,
            },
        }
    )
    session = FakeSession(response)
    result = DeepSeekClient("secret", session=session).enrich("An article")

    assert result.signals.themes == ("diplomacy", "international security")
    assert session.request is not None
    body = session.request["json"]
    assert isinstance(body, dict)
    assert body["model"] == DEEPSEEK_MODEL
    assert body["thinking"] == {"type": "disabled"}
    assert body["response_format"] == {"type": "json_object"}
    assert body["max_tokens"] == MAX_OUTPUT_TOKENS
