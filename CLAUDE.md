# KIT DSN Archival

Mirror and repackage the KIT DSN Bitcoin P2P monitoring dataset
(<https://www.dsn.kastel.kit.edu/bitcoin/data.html>, CC BY 4.0) before the site
goes offline. Daily snapshots since 2015-07-15, still growing; the full raw
mirror (4,167 files / 25.42 GB as of 2026-07-29) lives in `data/raw/` and is
sha256-verified. No other public archive of this data exists — the dataset's
Zenodo record holds only a readme.

## Hard rule

**Never modify or delete anything under `data/raw/`.** It may be the only
surviving copy of this dataset. Transformations write elsewhere; integrity is
checked with `kit-dsn-archival verify --hash` against `data/manifest.jsonl`.

## Layout

- `src/main.rs` — the whole Rust CLI (clap subcommands: `list`, `fetch`,
  `compress`, `verify`)
- `scripts/` — schema scan + compression/Parquet benchmark scripts (bash)
- `docs/` — mirrored KIT documentation pages, methodology paper PDF, Zenodo readme
- `SCHEMA.md` — observed schema vs documentation, drift timeline, known-bad files
- `README.md` — dataset facts, benchmark results table, packaging recommendation
- `data/` (gitignored) — `raw/` mirror, `listing.txt`, `manifest.jsonl`,
  `compressed/`, `schema/`, `bench/`
- `manual_json/`, `compressed/`, `zstd_dict*` — pre-CLI manual experiments, superseded

## Commands

```bash
cargo build --release && cargo clippy && cargo test
./target/release/kit-dsn-archival list                        # refresh snapshot listing
./target/release/kit-dsn-archival fetch --jobs 4              # resumable mirror (default 4 conns)
./target/release/kit-dsn-archival compress -j "$(nproc)" -T 1 # per-file zstd --ultra -22
./target/release/kit-dsn-archival verify --hash               # full integrity check
```

All commands are idempotent — re-running `fetch` also picks up newly published days.

## Rules

Detailed knowledge is in `.claude/rules/`: `dataset.md` (server + data quirks),
`rust-cli.md` (code conventions), `data-handling.md` (safety + benchmark
methodology and conclusions).
