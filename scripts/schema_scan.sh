#!/bin/bash
# Scan all mirrored dossier files and emit, per file, the set of observed
# leaf paths (node-id level stripped) with their JSON types.
#
# Usage: scripts/schema_scan.sh [RAW_DIR] [OUT_DIR]
# Output: OUT_DIR/paths.tsv with lines "YYYYMMDD<TAB>path<TAB>type",
#         one per (file, path, type) combination.
set -euo pipefail

RAW_DIR="${1:-data/raw}"
OUT_DIR="${2:-data/schema}"
JOBS="${JOBS:-$(nproc)}"

mkdir -p "$OUT_DIR/per_file"

scan_one() {
    f="$1"
    base="$(basename "$f")"
    date="${base:0:8}"
    out="$2/per_file/${base%.json}.tsv"
    [ -s "$out" ] && return 0
    jq -r --arg d "$date" '
        .[]
        | paths(type != "object" and type != "array") as $p
        | "\($d)\t\($p | join("."))\t\(getpath($p) | type)"
    ' "$f" | sort -u > "$out.tmp" && mv "$out.tmp" "$out"
}
export -f scan_one

find "$RAW_DIR" -name '*_dossier.json' -print0 \
    | xargs -0 -P "$JOBS" -I{} bash -c 'scan_one "$@"' _ {} "$OUT_DIR"

cat "$OUT_DIR"/per_file/*.tsv > "$OUT_DIR/paths.tsv"
echo "wrote $OUT_DIR/paths.tsv ($(wc -l < "$OUT_DIR/paths.tsv") lines)"
