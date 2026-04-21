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
musicbrainzngs.set_useragent("SoundOfCultureEnrichment", "1.0", "https://github.com/MarcoLeMoglie/Sound_of_Culture")

BASE_DIR = Path("data/phase_01_dataset_construction/processed/country_artists")
INPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full.csv"
OUTPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full_Enriched.csv"
CACHE_FILE = BASE_DIR / "intermediate" / "json_caches" / "release_years_cache.json"

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def get_release_year(artist, song):
    """Queries MusicBrainz for the earliest release year of a recording."""
    try:
        # 1. Search for recording
        query = f'artist:"{artist}" AND recording:"{song}"'
        result = musicbrainzngs.search_recordings(query=query, limit=10)
        
        recordings = result.get('recording-list', [])
        if not recordings:
            return None
            
        years = []
        for rec in recordings:
            # Check if artist matches loosely
            rec_artist = rec.get('artist-credit-phrase', '').lower()
            if artist.lower() not in rec_artist and rec_artist not in artist.lower():
                continue
                
            # Check for dates in release-list
            releases = rec.get('release-list', [])
            for rel in releases:
                date = rel.get('date')
                if date:
                    # Extract year (YYYY-MM-DD or YYYY)
                    year_match = date.split('-')[0]
                    if year_match.isdigit():
                        years.append(int(year_match))
        
        if years:
            return min(years)
        return None
        
    except musicbrainzngs.WebServiceError as exc:
        print(f"MusicBrainz API Error: {exc}")
        if "503" in str(exc):
            time.sleep(10) # Heavy backoff on 503
        return "ERROR_RETRY"
    except Exception as e:
        print(f"Unexpected Error: {e}")
        return None

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Input file {INPUT_CSV} not found.")
        return

    df = pd.read_csv(INPUT_CSV)
    cache = load_cache()
    
    unique_songs = df[['artist_name', 'song_name']].drop_duplicates().values
    total = len(unique_songs)
    print(f"Starting enrichment for {total} unique songs...")

    count = 0
    for artist, song in unique_songs:
        key = f"{artist} - {song}"
        
        if key in cache and cache[key] != "ERROR_RETRY":
            continue # Skip cached results
            
        # Respect 1 req/sec limit
        time.sleep(1.1)
        
        year = get_release_year(artist, song)
        cache[key] = year
        
        count += 1
        if count % 10 == 0:
            save_cache(cache)
            print(f"Progress: {len(cache)}/{total} items in cache. Last: {key} -> {year}")

    # Final Save
    save_cache(cache)
    
    # Map back to dataframe
    print("Mapping years back to the main dataset...")
    df['release_year'] = df.apply(lambda x: cache.get(f"{x['artist_name']} - {x['song_name']}"), axis=1)
    
    # Save enriched CSV
    df.to_csv(OUTPUT_CSV, index=False)
    
    # Save Stata DTA
    dta_path = OUTPUT_CSV.replace(".csv", ".dta")
    try:
        # Clean for Stata
        df_stata = df.copy()
        for col in df_stata.columns:
            if not pd.api.types.is_numeric_dtype(df_stata[col]):
                df_stata[col] = df_stata[col].fillna('').astype(str).apply(lambda x: x[:244])
        df_stata.to_stata(dta_path, write_index=False, version=118)
    except Exception as e:
         print(f"Error saving DTA: {e}")

    print(f"Enrichment complete. Saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
