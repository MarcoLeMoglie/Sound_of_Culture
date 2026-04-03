import csv
import pandas as pd
import musicbrainzngs
import time
import json
import os
import sys
import ssl

# SSL Workaround for macOS certificate issues
ssl._create_default_https_context = ssl._create_unverified_context

# User Agent for MusicBrainz (Mandatory)
from pathlib import Path

# Configuration
musicbrainzngs.set_useragent("SoundOfCultureEnrichment", "2.0", "https://github.com/MarcoLeMoglie/Sound_of_Culture")

BASE_DIR = Path("data/processed_datasets/country_artists")
INPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full_Enriched.csv"
OUTPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full_Enriched_v3.csv"
CACHE_FILE = BASE_DIR / "intermediate" / "json_caches" / "release_years_cache_fuzzy.json"

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def get_release_year_fuzzy(artist, song):
    """
    More relaxed search for MusicBrainz.
    If the strict search 'artist:"A" AND recording:"B"' failed,
    generic search 'A B' and filter manually.
    """
    try:
        # Pre-process names to avoid 'Request Entity Too Large' errors (HTTP 413)
        safe_song = str(song)[:80]
        safe_artist = str(artist)[:80]
        
        # 1. Fuzzy search (recording title + artist name as keywords)
        query = f'"{safe_song}" {safe_artist}'
        result = musicbrainzngs.search_recordings(query=query, limit=15)
        
        recordings = result.get('recording-list', [])
        if not recordings:
            return None
            
        years = []
        for rec in recordings:
            rec_artist = rec.get('artist-credit-phrase', '').lower()
            rec_title = rec.get('title', '').lower()
            
            # Simple fuzzy check: needs significant word overlap
            artist_words = set(artist.lower().replace(",", "").split())
            rec_artist_words = set(rec_artist.lower().replace(",", "").split())
            
            # Intersection needs to be substantial
            if not (artist_words & rec_artist_words):
                continue
            
            # Title overlap
            song_words = set(song.lower().replace(",", "").split())
            rec_title_words = set(rec_title.lower().replace(",", "").split())
            if not (song_words & rec_title_words):
                continue
                
            # Check for dates
            releases = rec.get('release-list', [])
            for rel in releases:
                date = rel.get('date')
                if date:
                    year_match = date.split('-')[0]
                    if year_match.isdigit():
                        years.append(int(year_match))
        
        if years:
            return min(years)
        return None
        
    except musicbrainzngs.WebServiceError as exc:
        print(f"MusicBrainz API Error: {exc}")
        if "503" in str(exc):
            time.sleep(15) 
        return "ERROR_RETRY"
    except Exception as e:
        print(f"Unexpected Error: {e}")
        return None

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Input file {INPUT_CSV} not found.")
        return

    # Robust reading using csv module to handle complex quoting
    rows = []
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    
    df = pd.DataFrame(rows)
    df.columns = [c.strip().replace('"', '').replace("'", "") for c in df.columns]
    
    # Ensure numeric types where possible
    numeric_cols = ['id', 'version', 'votes', 'rating', 'capo', 'upload_year', 'complexity', 'repetition', 'melodicness', 'energy', 'finger_movement', 'disruption', 'root_stability', 'intra_root_variation', 'harmonic_palette', 'loop_strength', 'structure_variation', 'playability', 'harmonic_softness', 'release_year']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    cache = load_cache()
    
    # Target only missing years (NaN, empty string, or 'nan' string)
    missing_mask = df['release_year'].isna() | (df['release_year'].astype(str) == '') | (df['release_year'].astype(str) == 'nan')
    unique_missing = df[missing_mask][['artist_name', 'song_name']].drop_duplicates().values
    total_missing = len(unique_missing)
    print(f"Starting Phase 2 Enrichment (MusicBrainz Fuzzy) for {total_missing} missing songs...")

    count = 0
    for artist, song in unique_missing:
        key = f"{artist} - {song}"
        
        if key in cache and cache[key] != "ERROR_RETRY":
            continue
            
        time.sleep(1.2)
        
        year = get_release_year_fuzzy(artist, song)
        cache[key] = year
        
        count += 1
        if count % 20 == 0:
            save_cache(cache)
            print(f"Progress: {len(cache)}/{total_missing} missing items in cache. Last: {key} -> {year}")

    save_cache(cache)
    
    print("Mapping recovered years back to the main dataset...")
    def find_year(row):
        if pd.isna(row['release_year']):
            key = f"{row['artist_name']} - {row['song_name']}"
            return cache.get(key)
        return row['release_year']

    df['release_year'] = df.apply(find_year, axis=1)
    df.to_csv(OUTPUT_CSV, index=False)
    
    # DTA Export with string truncation for Stata limits (244 chars)
    dta_path = OUTPUT_CSV.replace(".csv", ".dta")
    try:
        df_stata = df.copy()
        for col in df_stata.columns:
            if not pd.api.types.is_numeric_dtype(df_stata[col]):
                df_stata[col] = df_stata[col].fillna('').astype(str).str.slice(0, 244)
        df_stata.to_stata(dta_path, write_index=False, version=118)
        print(f"DTA file saved successfully: {dta_path}")
    except Exception as e:
         print(f"Error saving DTA: {e}")

    print(f"Phase 2 Enrichment complete (Fuzzy). Saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
