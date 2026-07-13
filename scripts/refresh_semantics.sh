#!/usr/bin/env bash
set -euo pipefail

limit=200
publish=true

for argument in "$@"; do
  case "$argument" in
    --local-only)
      publish=false
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

uv sync --extra semantic

arguments=(
  --limit "$limit"
  --max-cost-usd 1.00
  --output dist/story-signals.parquet
  --report dist/semantic-run.json
)
if [[ "$publish" == true ]]; then
  arguments+=(--publish)
fi

uv run --no-sync python -m services.fenic.enrich "${arguments[@]}"
