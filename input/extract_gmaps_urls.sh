#!/bin/bash

INPUT_FILE="/home/muham/development/infinitecrawler/input/combined.csv"
OUTPUT_FILE="/home/muham/development/infinitecrawler/output/gmaps_urls.txt"

mkdir -p "$(dirname "$OUTPUT_FILE")"

awk -F',' '{gsub(/^[[:space:]]+/, "", $NF); if ($NF ~ /^https:\/\/www\.google\.com\/maps/) print $NF}' "$INPUT_FILE" > "$OUTPUT_FILE"

echo "Extracted $(wc -l < "$OUTPUT_FILE") URLs to $OUTPUT_FILE"
