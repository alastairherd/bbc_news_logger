---
title: BBC News Semantic Backfill
emoji: 📚
colorFrom: blue
colorTo: gray
sdk: gradio
app_file: app.py
python_version: 3.12
---

# BBC News semantic backfill

This free CPU worker calculates missing `BAAI/bge-small-en-v1.5` embeddings for the public
BBC News Surface Lab dataset. Completed batches are written back to the dataset as Parquet
checkpoints, so sleeping or restarting the Space does not discard completed work.

The Space requires a write-capable `HF_TOKEN` secret. It does not hold or call the paid DeepSeek
API. Set `AUTO_START_BACKFILL=0` to require a manual start from this page.
