# KIT DSN Archival

Attempt to compress the KIT DSN data archives, such that another party could
mirror or host that 10-year Bitcoin P2P dataset.

## Approaches

### Per-file ZSTD compression

`zstdmt -z --ultra -22 $FILENAME`

This gives a massive space savings of 94% across both a 2015 and 2025 sample (18x compression ratio).

### ZSTD compression with a dictionary

To build a dictionary from a daily snapshot:

```bash
./json_splitter.sh manual_json/kit_dsn_20251111_125023_dossier.json
./create_zstd_dict.sh
```

To use the dictionary during compression:

`zstdmt -z --ultra -22 -D zstd_dict $FILENAME`

This actually reduced the space savings a bit.
