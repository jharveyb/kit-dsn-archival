#!/bin/bash
# Benchmark Parquet conversion for one month of snapshots via DuckDB.
#
# Usage: scripts/bench_parquet.sh YYYYMM [RAW_DIR] [WORK_DIR]
# Rows carry snapshot_ts (from the filename) and node_id (the object key);
# nested objects stay as Parquet struct columns.
set -euo pipefail

MONTH="$1"
RAW_DIR="${2:-data/raw}"
WORK="${3:-data/bench/parquet_$MONTH}"

mkdir -p "$WORK"
FILES=("$RAW_DIR/$MONTH"*_dossier.json)
[ -e "${FILES[0]}" ] || { echo "no files for $MONTH in $RAW_DIR" >&2; exit 1; }
RAW_BYTES=$(du -cb "${FILES[@]}" | tail -1 | cut -f1)

JSONL="$WORK/month.jsonl"
if [ ! -s "$JSONL" ]; then
    for f in "${FILES[@]}"; do
        b="$(basename "$f")"                       # YYYYMMDD_HHMMSS_dossier.json
        ts="${b:0:4}-${b:4:2}-${b:6:2}T${b:9:2}:${b:11:2}:${b:13:2}"
        jq -c --arg ts "$ts" 'to_entries[] | {snapshot_ts: $ts, node_id: .key} + .value' "$f"
    done > "$JSONL.tmp" && mv "$JSONL.tmp" "$JSONL"
fi

duckdb "$WORK/bench.duckdb" <<SQL
CREATE OR REPLACE TABLE m AS
SELECT * FROM read_json_auto('$JSONL', union_by_name=true, timestampformat='%Y-%m-%dT%H:%M:%S');

COPY (FROM m ORDER BY snapshot_ts, node_id)
  TO '$WORK/by_ts.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22);
COPY (FROM m ORDER BY node_id, snapshot_ts)
  TO '$WORK/by_node.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22);
COPY (FROM m ORDER BY lastConnect, node_id)
  TO '$WORK/by_lastconnect.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22);

CREATE OR REPLACE TABLE md AS SELECT CAST(snapshot_ts AS DATE) AS snapshot_day, * FROM m;
COPY (FROM md ORDER BY snapshot_day, node_id)
  TO '$WORK/by_day' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22,
                     PARTITION_BY (snapshot_day), OVERWRITE_OR_IGNORE);
SELECT count(*) AS rows, count(DISTINCT snapshot_ts) AS snapshots FROM m;
SQL

echo -e "\nvariant\tbytes\tratio_vs_raw"
for v in by_ts by_node by_lastconnect; do
    bytes=$(stat -c%s "$WORK/$v.parquet")
    awk -v n="$v" -v b="$bytes" -v r="$RAW_BYTES" 'BEGIN {printf "%s\t%d\t%.1fx\n", n, b, r/b}'
done
bytes=$(du -sb "$WORK/by_day" | cut -f1)
awk -v b="$bytes" -v r="$RAW_BYTES" 'BEGIN {printf "by_day(partitioned)\t%d\t%.1fx\n", b, r/b}'
echo "raw month bytes: $RAW_BYTES"
