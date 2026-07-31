# Dataset & server facts

## Server

- Listing endpoint: `https://www.dsn.kastel.kit.edu/bitcoin/snapshots/?curl` —
  one URL per line, newest first. The URLs point at `dsn.tm.kit.edu`, which
  301-redirects; rewrite the host to `www.dsn.kastel.kit.edu` up front (the CLI's
  `rewrite_host` does this) to avoid a redirect round-trip per file.
- HEAD returns exact `Content-Length`, `Last-Modified`, and `ETag`;
  `Accept-Ranges: bytes` is supported. The server ignores `Accept-Encoding`
  (never gzips), so received byte count can be verified against Content-Length.
- Politeness: default to 4 concurrent connections — the site's own bulk-download
  instructions use `xargs -P 4`.
- The site still publishes a new snapshot daily; file counts in docs/README are
  snapshots in time, not fixed totals.

## Data shape

- Files are named `YYYYMMDD_HHMMSS_dossier.json`. Each is a single JSON object
  mapping a 16-hex-char anonymized node id to a node record. **The snapshot
  timestamp exists only in the filename** — any conversion must carry it into
  the output (`snapshot_ts`), and the object key becomes `node_id`.
- ~230 calendar days in the range are missing at the source; some days have
  several snapshots (2024-11-28 has 13). Filename = identity; keep all files.
- File sizes: ~2.2 MB (2015) to ~7.5 MB (2025+).

## Known-bad source files

These defects exist on the KIT server itself (our transfers are hash-verified);
every JSON-consuming pipeline must tolerate them:

- Empty snapshots (`{}`, 2 bytes): `20160221_125956`, `20160430_121909`,
  `20160502_121957`, `20160503_122021`, `20210708_124700`, `20211012_123000`.
- `20190522_121247_dossier.json` is truncated at exactly 2 MiB and is not valid
  JSON — skip or special-case it.

## Schema drift (summary — full tables in SCHEMA.md)

- The schema grew over time: `versionstr`/`services`/`versionid` appear
  2016-01-29 (the latter two as literal `null` until 2017-05-20), `ip.tunnel*`
  2016-06-16, the `inv` block 2018 (in two stages). `ip.tunnelserver` disappears
  after 2023-03-28.
- Docs-vs-data deviations: longitude is `geo.long` (docs say `lon`);
  `versionstr` is the number `0` for isolated nodes in 15 files during
  2021-08..10; `whois` sub-fields are nullable.
- Parquet/typed schemas must be the nullable union across all eras.
