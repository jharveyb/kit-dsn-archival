# KIT DSN Archival

Archive of the KIT DSN Bitcoin network monitoring dataset, so that another party
can mirror or host this 10+ year Bitcoin P2P dataset after the original site goes
offline.

- Source: <https://www.dsn.kastel.kit.edu/bitcoin/data.html> (mirrored in [docs/](docs/))
- Methodology: [Characterization of the Bitcoin Peer-to-Peer Network (2015–2018)](https://publikationen.bibliothek.kit.edu/1000091933)
  (PDF mirrored in [docs/](docs/))
- License: CC BY 4.0
- The dataset's [Zenodo record](https://doi.org/10.5281/zenodo.14627525) contains only a
  readme, **not the data** — no public archive of the snapshots exists elsewhere.

## Dataset facts

- 4,167 files (`YYYYMMDD_HHMMSS_dossier.json`), 2015-07-15 → 2026-07-27, still growing daily.
- **25.42 GB raw**; ~2.2 MB/file in 2015 growing to ~7 MB in 2025.
- 3,796 unique dates: ~230 days missing at the source; some days have multiple snapshots.
- 6 snapshots are empty (`{}`) and one (`20190522_121247`) is truncated at the source —
  see [SCHEMA.md](SCHEMA.md) for these and for full schema-drift analysis.

## Mirroring tool

Rust CLI in this repo:

```bash
cargo build --release
./target/release/kit-dsn-archival list                 # fetch snapshot listing
./target/release/kit-dsn-archival fetch --jobs 4       # concurrent, resumable mirror
./target/release/kit-dsn-archival verify --hash        # full integrity check
./target/release/kit-dsn-archival compress -j "$(nproc)" -T 1   # per-file zstd --ultra -22
```

`compress` shells out to the system `zstd` and writes
`data/compressed/<name>.json.zst` per file. `-j` runs that many zstd processes in
parallel; `-T` sets threads per process, but zstd only engages extra threads on
inputs of hundreds of MB, so for these ≤7 MB files `-j $(nproc) -T 1` is the
throughput configuration (~16x speedup measured on 24 cores). Like `fetch` it is
idempotent, writes atomically, and accepts `--from`/`--to` date bounds.

`fetch` verifies every download against Content-Length, writes atomically via
`.part` files, retries with backoff, and records size + sha256 + Last-Modified +
ETag per file in `data/manifest.jsonl`. Re-running is idempotent (it only fetches
what's missing), which also picks up newly published days.

Current local mirror status: **4,167/4,167 files, 25.42 GB, all sha256-verified.**

## Compression benchmarks

One month of each era, `zstd --ultra -22 -T1` throughout. "canon" = node entries
and keys sorted with `jq -S` (content-exact, not byte-exact); "compact" = sorted +
whitespace stripped. Parquet via DuckDB (zstd level 22, monthly file), rows =
`(snapshot_ts, node_id, …fields)`.

| variant | 2015-08 (87.6 MB, 31 files) | 2025-06 (295 MB, 39 files) |
| --- | --- | --- |
| per-file zstd | 14.9x | 17.7x |
| tar + zstd | 20.6x | **37.0x** |
| tar + zstd `--long=28`/`31` | 20.6x | 37.0x |
| concat + zstd | 20.7x | 36.9x |
| tar canon + zstd | 32.3x | 32.2x |
| tar canon-compact + zstd | 33.6x | 33.7x |
| Parquet sorted by snapshot_ts | 26.1x | 27.1x |
| **Parquet sorted by node_id** | **35.8x** | **38.4x** |
| Parquet partitioned per day | 15.6x | 20.3x |

Scripts: [scripts/bench_compression.sh](scripts/bench_compression.sh),
[scripts/bench_parquet.sh](scripts/bench_parquet.sh).

### Findings

1. **Cross-file redundancy is the big lever.** Nodes persist day to day, so a monthly
   tar compresses 1.4x–2x better than per-file compression.
2. **`--long`/large windows buy nothing at level 22** — its built-in 128 MiB window
   already spans the redundancy (matches concentrate in adjacent days). This is good
   news: archives decompress with plain `zstd -d`, no `--long` flags required.
3. **Canonicalization (sorting node entries) helps old data, hurts new data.**
   2015-era files store nodes in an unstable order, so sorting aligns them across
   days (20.6x → 32.3x). 2025-era files are already consistently ordered, and
   re-sorting + reformatting loses to the raw files (37.0x → 32.2x).
4. **Parquet sorted by `node_id` wins both eras** (35.8x / 38.4x) and is directly
   queryable. Sort order matters: ~40% better than sorting by timestamp. Daily
   partitioning wastes 2x vs a monthly file.

### Recommendation

Publish two artifacts, both at monthly granularity:

1. **Canonical archive (byte-exact): monthly `tar` of the untouched originals +
   `zstd -22`.** Estimated ~0.8–1 GB for the whole dataset. Fidelity is provable
   via `data/manifest.jsonl` (sha256 of every original file).
2. **Derived analysis format: monthly Parquet, rows sorted by `node_id`,
   zstd-compressed.** Estimated ~0.7 GB total. Schema per [SCHEMA.md](SCHEMA.md)
   (nullable union across eras; `snapshot_ts` from filename, `node_id` from key;
   skip the one truncated file).

Both together stay under ~2 GB — hostable as GitHub release assets, a torrent, or
an archive.org item.

## Repo layout

- `src/` — Rust mirroring CLI
- `scripts/` — schema scan + compression/Parquet benchmarks
- `docs/` — mirrored KIT documentation pages, methodology paper, Zenodo readme
- `SCHEMA.md` — observed schema vs documentation, drift timeline, known-bad files
- `data/` (gitignored) — `raw/` mirror, `listing.txt`, `manifest.jsonl`, `schema/`
- `manual_json/`, `compressed/`, `zstd_dict*` — early manual experiments (superseded
  by the benchmarks above)
