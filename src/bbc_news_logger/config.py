"""Shared configuration for collection and publication."""

from __future__ import annotations

import os
from pathlib import Path

BBC_NEWS_URL = "https://www.bbc.co.uk/news"
BBC_BASE_URL = "https://www.bbc.co.uk"
DEFAULT_DATASET_ID = os.getenv("HF_DATASET_ID", "AlastairH/bbc-news-logger")
DEFAULT_RAW_DATASET_ID = os.getenv("HF_RAW_DATASET_ID", "AlastairH/bbc-news-logger-raw")
DEFAULT_USER_AGENT = os.getenv(
    "BBC_NEWS_USER_AGENT",
    "bbc-news-logger/0.2 (+https://github.com/alastairherd/bbc_news_logger)",
)
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_REQUESTS_PER_SECOND = 5.0
SCHEMA_VERSION = 1
LOCAL_DATA_DIR = Path(os.getenv("BBC_NEWS_LOCAL_DATA", "site-data"))
