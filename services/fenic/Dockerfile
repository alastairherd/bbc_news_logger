FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FENIC_DB_PATH=/data \
    PORT=7860

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY services ./services

RUN pip install --no-cache-dir uv \
    && uv sync --frozen --no-dev --extra semantic

RUN mkdir -p /data
EXPOSE 7860

CMD ["sh", "-c", "uv run --no-sync python services/fenic/bootstrap.py && uv run --no-sync uvicorn services.fenic.serve:app --host 0.0.0.0 --port ${PORT}"]
