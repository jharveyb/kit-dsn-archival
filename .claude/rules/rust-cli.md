# Rust CLI conventions

- Single-file binary crate (`src/main.rs`), clap derive subcommands. Pure
  helpers (`rewrite_host`, `filename_of`, `date_of`, `in_range`) stay
  free-standing functions with unit tests in the `tests` module; give any new
  pure helper the same treatment.
- **Every long-running command is idempotent and resumable.** Skip work whose
  output already exists: `fetch` skips when a manifest entry exists and the disk
  size matches; `compress` skips non-empty `.zst` outputs. Re-running must
  always be safe and must pick up newly published days.
- **Atomic writes everywhere**: stream to `*.part`, verify, then rename into
  place; remove the `.part` on failure. zstd gets `-f` specifically so a stale
  `.part` from an interrupted run can't wedge retries.
- `data/manifest.jsonl` is append-only provenance (file, url, size, sha256,
  Last-Modified, ETag, fetched_at). On load, later entries supersede earlier
  ones — never rewrite the file in place.
- Concurrency pattern:
  `futures::stream::iter(...).buffer_unordered(jobs.get())` consumed by a
  `while let Some(...) = results.next().await` loop. Job counts are
  `std::num::NonZeroUsize` so clap rejects `0` at parse time (no runtime
  clamping). Subprocesses go through `tokio::process` — async handles, no
  dedicated OS threads.
- The reqwest client sets `.gzip(false)` deliberately: identity transfer keeps
  Content-Length verification exact (the server never gzips anyway). Downloads
  are verified byte-count-vs-Content-Length and sha256-recorded in one pass.
- zstd runs as an external process (`zstd -q -f --ultra -22 -T<n>`) because the
  Rust zstd crate has no multithreading. Note `-T` only engages extra threads
  on inputs of roughly ≥512 MB at level 22 (worker job ≈ 4x the 128 MiB
  window), so for these ≤7.5 MB files parallelism comes from `-j` (multiple
  processes), not `-T`.
- Retries: network operations retry up to 4 attempts with exponential backoff;
  per-item failures are reported and the command exits non-zero if any remain.
