#!/bin/bash
set -e

# Configuration
PKG_DIR="/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/data/processed_datasets/country_artists/replication_package_country_songs_2026_04_01"
RAW_DATA_DIR="$PKG_DIR/data/raw_tabs_country"
OUTPUT_DIR="$PKG_DIR/output"
SCRIPTS_DIR="$PKG_DIR/scripts"
SOURCE_RAW_DIR="/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/data/raw_tabs_country"

echo ">>> Starting Cold Start Verification Pipeline..."

# 1. Setup Directories
mkdir -p "$RAW_DATA_DIR"
mkdir -p "$OUTPUT_DIR"
rm -rf "$OUTPUT_DIR"/*

# 2. Copy subset of raw data (10 JSON files)
echo ">>> Copying 10 sample JSON files..."
ls "$SOURCE_RAW_DIR"/*.json | head -n 10 | xargs -I {} cp {} "$RAW_DATA_DIR/"

# 3. Step-by-Step Execution
echo ">>> Executing Step 1: Base Dataset Extraction..."
python3 "$SCRIPTS_DIR/01_create_base_dataset.py"

echo ">>> Executing Step 2: Artist Biography Collection..."
python3 "$SCRIPTS_DIR/02_build_artist_bio_lookup.py" --limit 5

echo ">>> Executing Step 3: MusicBrainz Strict Enrichment..."
python3 "$SCRIPTS_DIR/03_enrich_release_years_strict.py"

echo ">>> Executing Step 4: MusicBrainz Fuzzy Enrichment..."
python3 "$SCRIPTS_DIR/04_enrich_release_years_fuzzy.py"

echo ">>> Executing Step 5: Discogs Enrichment..."
python3 "$SCRIPTS_DIR/05_enrich_release_years_discogs.py"

echo ">>> Executing Step 6: Final Internet Recovery Enrichment..."
python3 "$SCRIPTS_DIR/06_enrich_release_years_final_recovery.py"

echo ">>> Verification Pipeline Finished Successfully!"
ls -lh "$OUTPUT_DIR"
