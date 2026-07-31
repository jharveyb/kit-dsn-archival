# Data handling & benchmarking

## Safety

- `data/raw/` is immutable — the only local copy of a dataset that may vanish
  upstream. Transformations write to `data/compressed/`, `data/bench/`, or the
  scratchpad, never in place.
- `data/` stays gitignored; never commit dataset files, only code and docs.
- Any repackaged artifact must round-trip: decompressed bytes must sha256-match
  the entries in `data/manifest.jsonl`. Content-exact-but-not-byte-exact
  transforms (key reordering, Parquet) are fine as *derived* formats, but the
  canonical archive stays byte-exact.

## Benchmark methodology

- Use `zstd --ultra -22 -T1` for cross-variant ratio comparisons —
  multithreading changes chunking and would muddy window-size experiments.
- Benchmark at monthly granularity, one month per era: **2015-08** and
  **2025-06** are the established reference months. Record size / ratio / time
  in the README results table.
- Long jobs (full-dataset compress, schema scan, month benchmarks) run in the
  background; intermediates go to the scratchpad or `data/bench/`, not the repo.

## Established conclusions (don't re-derive)

- Cross-file redundancy is the big lever: monthly tar + zstd beats per-file
  zstd 1.4x (2015) to 2x (2025-era).
- `--long`/window sizes above the default are a **no-op at level 22** — its
  128 MiB window already spans the redundancy (matches concentrate in adjacent
  days). Consequence: archives decompress with plain `zstd -d`, no flags.
- Canonicalization (`jq -S` sorting) is era-dependent: helps 2015-era files
  (20.6x → 32.3x, unstable node order) but **hurts** 2025-era files
  (37.0x → 32.2x, already consistently ordered). Don't apply it blindly.
- Parquet sorted by `node_id` is the best ratio in both eras (35.8x / 38.4x),
  ~40% better than sorting by `snapshot_ts`; daily partitioning costs ~2x vs
  one monthly file. DuckDB writes it via `COPY ... (FORMAT PARQUET, COMPRESSION
  ZSTD, COMPRESSION_LEVEL 22)`.
- Recommended publication (per README): monthly tar.zst of untouched originals
  (~1 GB total, byte-exact) + monthly Parquet sorted by `node_id` (~0.7 GB),
  both under ~2 GB combined.
