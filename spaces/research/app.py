"""Non-Docker Hugging Face CPU Space for cited archive synthesis."""

from __future__ import annotations

import hashlib
import os
import threading
import time
from collections import defaultdict, deque
from decimal import Decimal
from typing import Any

import gradio as gr
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from research_core import (
    ResearchInputError,
    ResearchProviderError,
    build_user_prompt,
    call_deepseek,
    maximum_request_cost,
    normalize_request,
)

MAX_PROCESS_BUDGET_USD = Decimal("1.00")
configured_budget = Decimal(os.getenv("RESEARCH_MAX_COST_USD", "1.00"))
PROCESS_BUDGET_USD = min(max(configured_budget, Decimal("0")), MAX_PROCESS_BUDGET_USD)
RATE_WINDOW_SECONDS = 600
RATE_REQUESTS = 5

app = FastAPI(title="BBC News archive research", docs_url=None, redoc_url=None)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://alastairherd.github.io",
        "http://localhost:4321",
        "http://127.0.0.1:4321",
    ],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)


class ResearchRequest(BaseModel):
    query: str
    evidence: list[dict[str, Any]]


cache: dict[str, dict[str, Any]] = {}
requests_by_client: defaultdict[str, deque[float]] = defaultdict(deque)
lock = threading.Lock()
provider_slots = threading.Semaphore(2)
spent_usd = Decimal("0")
reserved_usd = Decimal("0")


def _rate_limit(client: str) -> None:
    now = time.monotonic()
    with lock:
        entries = requests_by_client[client]
        while entries and entries[0] < now - RATE_WINDOW_SECONDS:
            entries.popleft()
        if len(entries) >= RATE_REQUESTS:
            raise HTTPException(429, "Please wait before asking another archive question.")
        entries.append(now)


def _reserve(cost: Decimal) -> None:
    global reserved_usd
    with lock:
        if spent_usd + reserved_usd + cost > PROCESS_BUDGET_USD:
            raise HTTPException(503, "The archive's $1 DeepSeek budget is currently exhausted.")
        reserved_usd += cost


def _release(reservation: Decimal, actual: Decimal = Decimal("0")) -> None:
    global reserved_usd, spent_usd
    with lock:
        reserved_usd = max(Decimal("0"), reserved_usd - reservation)
        spent_usd += actual


@app.get("/api/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "model": "deepseek-v4-flash",
        "budgetUsd": float(PROCESS_BUDGET_USD),
        "spentUsd": float(spent_usd),
        "cachedAnswers": len(cache),
    }


@app.post("/api/research")
async def research(body: ResearchRequest, request: Request) -> dict[str, Any]:
    client = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    client = client or (request.client.host if request.client else "unknown")
    _rate_limit(client)
    try:
        query, evidence = normalize_request(body.query, body.evidence)
    except ResearchInputError as exc:
        raise HTTPException(422, str(exc)) from exc

    canonical = build_user_prompt(query, evidence)
    cache_key = hashlib.sha256(canonical.encode()).hexdigest()
    if cache_key in cache:
        return {**cache[cache_key], "cached": True}

    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        raise HTTPException(503, "DeepSeek is not configured for this Space.")
    reservation = maximum_request_cost(canonical)
    _reserve(reservation)
    try:
        with provider_slots:
            answer = await run_in_threadpool(call_deepseek, api_key, query, evidence)
        actual = Decimal(str(answer.get("usage", {}).get("costUsd", 0)))
        _release(reservation, actual)
    except ResearchProviderError as exc:
        _release(reservation)
        raise HTTPException(502, str(exc)) from exc
    except Exception:
        _release(reservation)
        raise
    cache[cache_key] = answer
    return {**answer, "cached": False}


with gr.Blocks(title="BBC News archive research API") as demo:
    gr.Markdown(
        """# BBC News archive research API

This free-CPU Space adds bounded, cited DeepSeek synthesis to the Surface Lab's browser-side
semantic search. Retrieval remains in the browser; this service receives only the selected BBC
evidence. [Open the research interface](https://alastairherd.github.io/bbc_news_logger/signals/).
"""
    )

app = gr.mount_gradio_app(app, demo, path="/")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=7860)
