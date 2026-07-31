#!/bin/bash
# Benchmark compression candidates on one month of snapshots.
#
# Usage: scripts/bench_compression.sh YYYYMM [RAW_DIR] [WORK_DIR]
# Emits a TSV of results to stdout: variant, bytes, ratio vs raw, seconds.
#
# All zstd runs use -T1 so the long-window variants aren't distorted by
# multithreaded chunking; ratios stay comparable across variants.
set -euo pipefail

MONTH="$1"
RAW_DIR="${2:-data/raw}"
WORK="${3:-data/bench/$MONTH}"

mkdir -p "$WORK"
FILES=("$RAW_DIR/$MONTH"*_dossier.json)
[ -e "${FILES[0]}" ] || { echo "no files for $MONTH in $RAW_DIR" >&2; exit 1; }

RAW_BYTES=$(du -cb "${FILES[@]}" | tail -1 | cut -f1)
echo "month $MONTH: ${#FILES[@]} files, $RAW_BYTES raw bytes" >&2

# Canonicalized copies: node entries and object keys sorted (jq -S).
# canon  = sorted, pretty-printed (close to original formatting)
# compact = sorted + compact (whitespace removed)
mkdir -p "$WORK/canon" "$WORK/compact"
for f in "${FILES[@]}"; do
    b="$(basename "$f")"
    [ -s "$WORK/canon/$b" ] || jq -S . "$f" > "$WORK/canon/$b"
    [ -s "$WORK/compact/$b" ] || jq -S -c . "$f" > "$WORK/compact/$b"
done

bench() {
    local name="$1"; shift
    local out="$WORK/$name.zst"
    local start end bytes secs
    start=$(date +%s.%N)
    "$@" > "$out"
    end=$(date +%s.%N)
    bytes=$(stat -c%s "$out")
    secs=$(echo "$end $start" | awk '{printf "%.1f", $1-$2}')
    awk -v n="$name" -v b="$bytes" -v r="$RAW_BYTES" -v s="$secs" \
        'BEGIN {printf "%s\t%d\t%.1fx\t%ss\n", n, b, r/b, s}'
}

tar_of() { tar -C "$1" -cf - --sort=name .; }

echo -e "variant\tbytes\tratio\ttime"

# 1. Per-file zstd -22 (baseline): sum of individually compressed files.
start=$(date +%s.%N)
PF_BYTES=0
for f in "${FILES[@]}"; do
    zstd -q -f --ultra -22 -T1 -c "$f" > "$WORK/pf.tmp.zst"
    PF_BYTES=$((PF_BYTES + $(stat -c%s "$WORK/pf.tmp.zst")))
done
rm -f "$WORK/pf.tmp.zst"
end=$(date +%s.%N)
awk -v b="$PF_BYTES" -v r="$RAW_BYTES" -v s="$(echo "$end $start" | awk '{printf "%.1f", $1-$2}')" \
    'BEGIN {printf "per-file zstd22\t%d\t%.1fx\t%ss\n", b, r/b, s}'

# 2-4. tar of raw files at increasing window sizes.
mkdir -p "$WORK/rawlink"
for f in "${FILES[@]}"; do ln -sf "$(realpath "$f")" "$WORK/rawlink/$(basename "$f")"; done
bench "tar-raw zstd22 wlog27"  bash -c "tar -C '$WORK/rawlink' -chf - --sort=name . | zstd -q --ultra -22 -T1 -c"
bench "tar-raw zstd22 long28"  bash -c "tar -C '$WORK/rawlink' -chf - --sort=name . | zstd -q --ultra -22 -T1 --long=28 -c"
bench "tar-raw zstd22 long31"  bash -c "tar -C '$WORK/rawlink' -chf - --sort=name . | zstd -q --ultra -22 -T1 --long=31 -c"

# 5. Concatenation instead of tar (original plan's option).
bench "concat-raw zstd22 wlog27" bash -c "cat $(printf '%q ' "${FILES[@]}") | zstd -q --ultra -22 -T1 -c"

# 6-7. Canonicalized (sorted) pretty JSON.
bench "tar-canon zstd22 wlog27" bash -c "tar -C '$WORK/canon' -cf - --sort=name . | zstd -q --ultra -22 -T1 -c"
bench "tar-canon zstd22 long31" bash -c "tar -C '$WORK/canon' -cf - --sort=name . | zstd -q --ultra -22 -T1 --long=31 -c"

# 8-9. Canonicalized compact JSON.
bench "tar-compact zstd22 wlog27" bash -c "tar -C '$WORK/compact' -cf - --sort=name . | zstd -q --ultra -22 -T1 -c"
bench "tar-compact zstd22 long31" bash -c "tar -C '$WORK/compact' -cf - --sort=name . | zstd -q --ultra -22 -T1 --long=31 -c"
