"""Free Hugging Face CPU Space for the resumable historical embedding backfill."""

from __future__ import annotations

import os
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import gradio as gr

from bbc_news_logger.semantics import EMBEDDING_MODEL, run_embedding_refresh

DATASET_ID = os.getenv("BBC_NEWS_DATASET", "AlastairH/bbc-news-logger")
AUTO_START = os.getenv("AUTO_START_BACKFILL", "1") == "1"

state: dict[str, Any] = {
    "state": "idle",
    "dataset": DATASET_ID,
    "model": EMBEDDING_MODEL,
    "started_at": None,
    "finished_at": None,
    "report": None,
    "error": None,
}
state_lock = threading.Lock()


def snapshot() -> dict[str, Any]:
    with state_lock:
        return dict(state)


def run_backfill() -> dict[str, Any]:
    with state_lock:
        if state["state"] == "running":
            return dict(state)
        state.update(
            state="running",
            started_at=datetime.now(timezone.utc).isoformat(),
            finished_at=None,
            report=None,
            error=None,
        )

    def work() -> None:
        try:
            if not os.getenv("HF_TOKEN"):
                raise RuntimeError("Add a write-capable HF_TOKEN in the Space secrets")
            report = run_embedding_refresh(
                dataset_id=DATASET_ID,
                limit=0,
                batch_size=128,
                publish=True,
            )
        except BaseException as exc:
            with state_lock:
                state.update(
                    state="failed",
                    error=str(exc),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                )
        else:
            with state_lock:
                state.update(
                    state="complete",
                    report=asdict(report),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                )

    threading.Thread(target=work, name="bge-backfill", daemon=True).start()
    return snapshot()


with gr.Blocks(title="BBC News semantic backfill") as demo:
    gr.Markdown(
        "# BBC News semantic backfill\n"
        "BGE Small runs on this Space's free CPU. Each completed batch is saved to the dataset, "
        "so the worker can resume after the Space sleeps."
    )
    status = gr.JSON(label="Worker status", value=snapshot)
    start = gr.Button("Start missing embeddings", variant="primary")
    start.click(fn=run_backfill, outputs=status)
    timer = gr.Timer(5)
    timer.tick(fn=snapshot, outputs=status)

if AUTO_START:
    run_backfill()

demo.launch()
