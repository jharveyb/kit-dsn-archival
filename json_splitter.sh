#!/bin/bash

INP="$1"
DICTDIR="$(pwd)/zstd_dict_entries"

mkdir -p "$DICTDIR"

# For a JSON object of a series of key-value pairs, write each key-value pair
# as a JSON object to a numbered file.
i=0
jq -c 'to_entries[] | {(.key): .value}' "$INP" | while IFS= read -r item; do
    printf '%s\n' "$item" | jq '.' > "$DICTDIR/${i}.json"
    ((i++))
done
