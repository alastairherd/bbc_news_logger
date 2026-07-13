"""Materialize the public dataset into a persistent Fenic catalog."""

from __future__ import annotations

import os
from importlib import import_module
from pathlib import Path

import fenic as fc
from huggingface_hub import snapshot_download

DATASET_ID = os.getenv("BBC_NEWS_DATASET", "AlastairH/bbc-news-logger")
APP_NAME = os.getenv("FENIC_APP_NAME", "bbc_news_research_lab")
DB_PATH = Path(os.getenv("FENIC_DB_PATH", ".fenic"))
DEFAULT_OPENROUTER_MODEL = "nvidia/nemotron-3-ultra-550b-a55b:free"

TABLES = {
    "observations": (
        "data/observations/",
        "Hourly position-level observations from the BBC News front page and Most Read list.",
    ),
    "article_snapshots": (
        "data/article_snapshots/",
        "Parsed article snapshots with stable story keys, metadata, and plain text.",
    ),
    "scrape_runs": (
        "data/scrape_runs/",
        "Operational metadata and validation counts for collection runs.",
    ),
    "story_signals": (
        "semantic/",
        "Fenic/OpenRouter topic, summary, and named-entity extraction for recent articles.",
    ),
}


def create_session(*, semantic: bool = False) -> fc.Session:
    DB_PATH.mkdir(parents=True, exist_ok=True)
    semantic_config = None
    if semantic:
        # Fenic 0.10 configures OpenRouter through the OpenAI SDK. OpenAI SDK 2.45
        # requires its conventional variable even though Fenic supplies OpenRouter
        # authorization headers; mirror the same token until Fenic passes api_key.
        if openrouter_key := os.getenv("OPENROUTER_API_KEY"):
            os.environ.setdefault("OPENAI_API_KEY", openrouter_key)
        # Fenic 0.10 sends `max_completion_tokens`, but some OpenRouter models
        # (including Nemotron 3 Ultra) advertise only `max_tokens`. Suppress the
        # incompatible field and put the equivalent cap in OpenRouter's extra body.
        client_module = import_module(
            "fenic._inference.openrouter.openrouter_batch_chat_completions_client"
        )
        client_class = client_module.OpenRouterBatchChatCompletionsClient
        profile_module = import_module("fenic._inference.openrouter.openrouter_profile_manager")
        profile_class = profile_module.OpenRouterCompletionProfileConfiguration
        if not getattr(client_class, "_bbc_news_openrouter_compat", False):
            original_extra_body = profile_class.extra_body.fget

            def compatible_extra_body(profile: object) -> dict[str, object]:
                body = original_extra_body(profile) if original_extra_body else {}
                body["max_tokens"] = int(os.getenv("OPENROUTER_MAX_TOKENS", "256"))
                return body

            client_class._get_max_output_token_request_limit = lambda _self, _request: None
            client_class._bbc_news_openrouter_compat = True
            profile_class.extra_body = property(compatible_extra_body)

        model = os.getenv("OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL)
        semantic_config = fc.SemanticConfig(
            language_models={
                "openrouter": fc.OpenRouterLanguageModel(
                    model_name=model,
                    profiles={
                        "extraction": fc.OpenRouterLanguageModel.Profile(
                            reasoning_effort="none"
                        )
                    },
                    default_profile="extraction",
                    structured_output_strategy="prefer_tools",
                )
            },
            default_language_model="openrouter",
        )
    return fc.Session.get_or_create(
        fc.SessionConfig(
            app_name=APP_NAME,
            db_path=DB_PATH,
            semantic=semantic_config,
        )
    )


def dataset_paths(prefix: str) -> list[str]:
    snapshot = Path(
        snapshot_download(
            repo_id=DATASET_ID,
            repo_type="dataset",
            allow_patterns=f"{prefix}**/*.parquet",
            token=os.getenv("HF_TOKEN"),
            max_workers=8,
        )
    )
    return [str(path) for path in sorted((snapshot / prefix).rglob("*.parquet"))]


def bootstrap() -> dict[str, int]:
    session = create_session()
    counts: dict[str, int] = {}
    for table_name, (prefix, description) in TABLES.items():
        paths = dataset_paths(prefix)
        if not paths:
            if table_name in {"scrape_runs", "story_signals"}:
                continue
            raise FileNotFoundError(f"No Parquet files found for {table_name} in {DATASET_ID}")
        frame = session.read.parquet(paths)
        frame.write.save_as_table(table_name, mode="overwrite")
        session.catalog.set_table_description(table_name, description)
        counts[table_name] = session.table(table_name).count()
    session.stop(skip_usage_summary=True)
    return counts


if __name__ == "__main__":
    print(bootstrap())
