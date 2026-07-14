"""Contract tests for the standalone Hugging Face research service core."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).parents[1] / "spaces" / "research" / "research_core.py"
SPEC = importlib.util.spec_from_file_location("research_core", MODULE_PATH)
assert SPEC and SPEC.loader
research_core = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = research_core
SPEC.loader.exec_module(research_core)


def evidence(url: str = "https://www.bbc.co.uk/news/articles/example") -> list[dict[str, object]]:
    return [
        {
            "title": "Talks resume after summit",
            "url": url,
            "date": "2026-07-01",
            "summary": "Officials resumed negotiations after a summit.",
            "topic": "world",
            "themes": ["diplomacy", "security guarantees"],
            "story_form": "update",
            "event_type": "diplomacy",
            "similarity": 0.82,
        }
    ]


def test_request_accepts_only_bounded_bbc_evidence() -> None:
    query, rows = research_core.normalize_request("What changed?", evidence() * 20)
    assert query == "What changed?"
    assert len(rows) == 1  # duplicate URLs are not billed twice
    assert rows[0].id == 1

    with pytest.raises(research_core.ResearchInputError, match="valid BBC"):
        research_core.normalize_request("What changed?", evidence("https://example.com/story"))


def test_answer_discards_invented_source_numbers() -> None:
    answer = research_core.parse_answer(
        json.dumps(
            {
                "answer": "Negotiations resumed [1].",
                "findings": [
                    {"claim": "Talks resumed.", "sources": [1, 99]},
                    {"claim": "Uncited claim.", "sources": [99]},
                ],
                "limitations": "Only one archived report was retrieved.",
            }
        ),
        1,
    )
    assert answer["findings"] == [{"claim": "Talks resumed.", "sources": [1]}]


class FakeResponse:
    text = ""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "answer": "Talks resumed [1].",
                                "findings": [],
                                "limitations": "",
                            }
                        )
                    }
                }
            ],
            "usage": {
                "prompt_tokens": 100,
                "prompt_cache_miss_tokens": 100,
                "completion_tokens": 30,
            },
        }


class FakeSession:
    request: dict[str, object] | None = None

    def post(self, _url: str, **kwargs: object) -> FakeResponse:
        self.request = kwargs
        return FakeResponse()


def test_deepseek_call_is_json_non_thinking_and_output_bounded() -> None:
    query, rows = research_core.normalize_request("What changed?", evidence())
    session = FakeSession()
    answer = research_core.call_deepseek("limited-key", query, rows, session=session)
    assert answer["answer"] == "Talks resumed [1]."
    assert session.request is not None
    body = session.request["json"]
    assert body["model"] == "deepseek-v4-flash"
    assert body["thinking"] == {"type": "disabled"}
    assert body["max_tokens"] == research_core.MAX_OUTPUT_TOKENS
