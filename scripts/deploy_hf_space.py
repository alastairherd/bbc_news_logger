"""Create or update the free Gradio embedding worker Space."""

from __future__ import annotations

import os
from pathlib import Path

from huggingface_hub import HfApi


def main() -> None:
    token = os.getenv("HF_TOKEN")
    if not token:
        raise SystemExit("HF_TOKEN is required to deploy the Space")
    repo_id = os.getenv("HF_SPACE_ID", "AlastairH/bbc-news-semantic-backfill")
    api = HfApi(token=token)
    api.create_repo(repo_id=repo_id, repo_type="space", space_sdk="gradio", exist_ok=True)
    api.add_space_secret(
        repo_id=repo_id,
        key="HF_TOKEN",
        value=token,
        description="Writes completed BGE checkpoint shards to the BBC News dataset",
    )
    api.upload_folder(
        repo_id=repo_id,
        repo_type="space",
        folder_path=Path("spaces/bge-worker"),
        path_in_repo=".",
        commit_message="Deploy checkpointed BGE Small worker",
    )
    print(f"Deployed https://huggingface.co/spaces/{repo_id}")


if __name__ == "__main__":
    main()
