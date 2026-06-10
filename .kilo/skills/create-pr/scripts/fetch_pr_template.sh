#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <output-file>" >&2
  exit 1
fi

output_file="$1"

curl -sS -L \
  "https://raw.githubusercontent.com/reductstore/.github/main/.github/pull_request_template.md" \
  -o "$output_file"

if [[ ! -s "$output_file" ]]; then
  echo "Template download failed or empty: $output_file" >&2
  exit 1
fi

echo "Template saved to $output_file"
