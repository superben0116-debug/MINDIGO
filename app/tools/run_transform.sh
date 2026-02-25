#!/usr/bin/env bash
set -euo pipefail
BASE="/Users/baicai/Downloads/终极 ERP/docs/inputs"
OUT="/Users/baicai/Downloads/终极 ERP/docs/outputs"

if [ ! -d "$BASE" ]; then
  echo "inputs folder not found: $BASE" >&2
  exit 1
fi

mkdir -p "$OUT"
python3 "/Users/baicai/Downloads/终极 ERP/app/tools/transformer.py"

echo "\nDone. Outputs in: $OUT"
ls -la "$OUT"
