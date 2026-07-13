#!/usr/bin/env bash
set -euo pipefail

limit=200
publish=true
scope=backfill

for argument in "$@"; do
  case "$argument" in
    --local-only)
      publish=false
      ;;
    --monthly)
      scope=monthly
      ;;
    *)
      limit="$argument"
      ;;
  esac
done

if [[ -f CREDS.txt ]]; then
  set -a
  # shellcheck disable=SC1091
  source CREDS.txt
  set +a
fi

if [[ -z "${DEEPSEEK_API_KEY:-}" && -n "${DEEPSEEK_API:-}" ]]; then
  export DEEPSEEK_API_KEY="$DEEPSEEK_API"
fi

if [[ -z "${DEEPSEEK_API_KEY:-}" ]]; then
  echo "DEEPSEEK_API_KEY is required (DEEPSEEK_API in CREDS.txt is also accepted)" >&2
  exit 1
fi

uv sync --extra embedding

arguments=(
  --limit "$limit"
  --batch-size 8
  --concurrency 4
  --scope "$scope"
  --max-cost-usd 1.00
  --checkpoint dist/semantic-checkpoint.sqlite3
  --output-dir dist/signal-shards
  --report dist/semantic-run.json
)
if [[ "$publish" == true ]]; then
  arguments+=(--publish)
fi

uv run --no-sync python -m services.fenic.enrich "${arguments[@]}"
