#!/bin/bash

DICTDIR="$(pwd)/zstd_dict_entries"

# Use all files in zstd_dict_entries as input; optimize for compression
# level 22.
zstd --train "$DICTDIR" -r -22 -o "$(pwd)/zstd_dict"