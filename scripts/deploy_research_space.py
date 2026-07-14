"""Publish the non-Docker research Space without exposing its API key."""

from __future__ import annotations

import os
from pathlib import Path

from huggingface_hub import HfApi

SPACE_ID = os.getenv("HF_RESEARCH_SPACE", "AlastairH/bbc-news-archive-research")


def main() -> None:
    token = os.environ["HF_TOKEN"]
    deepseek_key = os.environ["DEEPSEEK_API_KEY"]
    api = HfApi(token=token)
    api.create_repo(
        repo_id=SPACE_ID,
        repo_type="space",
        space_sdk="gradio",
        exist_ok=True,
    )
    api.upload_folder(
        repo_id=SPACE_ID,
        repo_type="space",
        folder_path=Path("spaces/research"),
        ignore_patterns=["__pycache__/**", "*.pyc"],
        commit_message="Deploy cited BBC archive research service",
    )
    api.add_space_secret(
        SPACE_ID,
        "DEEPSEEK_API_KEY",
        deepseek_key,
        description="Limited DeepSeek key for bounded cited archive answers",
    )


if __name__ == "__main__":
    main()
