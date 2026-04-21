import pandas as pd
import discogs_client
import time
import json
import os
import sys

# User Agent for Discogs (Mandatory)
d = discogs_client.Client('SoundOfCultureEnrichment/1.0', user_token=None)

from pathlib import Path

# Configuration
BASE_DIR = Path("data/phase_01_dataset_construction/processed/country_artists")
INPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full_Enriched.csv"
OUTPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full_Enriched_v2.csv"
CACHE_FILE = BASE_DIR / "intermediate" / "json_caches" / "release_years_cache_discogs.json"

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def get_release_year_discogs(artist, song):
    """Queries Discogs for the release year."""
    try:
        # Search by artist and title (positional arg 1)
        # Using type='release' and strict artist filtering
        results = d.search(song, artist=artist, type='release')
        
        # Get first page of results
        page = results.page(1)
        if not page:
            return None
            
        # Check first 5 matches for artist relevance
        for release in page[:5]:
            # Discogs artist matching is already handled by artist=artist param
            # Just extract the year
            year = getattr(release, 'year', None)
            if year and isinstance(year, int) and year > 1900:
                return year
        return None
        
    except Exception as e:
        print(f"Discogs API Error for {artist} - {song}: {e}")
        # Discogs rate limit (60/min) is managed by calling sleep
        return "ERROR_RETRY"

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Input file {INPUT_CSV} not found.")
        return

    df = pd.read_csv(INPUT_CSV)
    cache = load_cache()
    
    # Target only missing years
    missing_mask = df['release_year'].isna()
    unique_missing = df[missing_mask][['artist_name', 'song_name']].drop_duplicates().values
    total_missing = len(unique_missing)
    print(f"Starting Phase 2 Enrichment (Discogs) for {total_missing} missing songs...")

    count = 0
    for artist, song in unique_missing:
        key = f"{artist} - {song}"
        
        if key in cache and cache[key] != "ERROR_RETRY":
            continue
            
        # Respect 20 req/min limit for unauthenticated (3.0s)
        time.sleep(3.0)
        
        year = get_release_year_discogs(artist, song)
        cache[key] = year
        
        count += 1
        if count % 20 == 0:
            save_cache(cache)
            print(f"Progress: {len(cache)}/{total_missing} missing items in cache. Last: {key} -> {year}")

    # Final Save
    save_cache(cache)
    
    # Map back to dataframe (ONLY for missing ones)
    print("Mapping recovered years back to the main dataset...")
    
    # We update the 'release_year' column using the new cache for those that were NaN
    def find_year(row):
        if pd.isna(row['release_year']):
            key = f"{row['artist_name']} - {row['song_name']}"
            return cache.get(key)
        return row['release_year']

    df['release_year'] = df.apply(find_year, axis=1)
    
    # Final cleanup: ensure numeric
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
    
    # Save enriched CSV v2
    df.to_csv(OUTPUT_CSV, index=False)
    
    # Save Stata DTA
    dta_path = OUTPUT_CSV.replace(".csv", ".dta")
    try:
        df_stata = df.copy()
        for col in df_stata.columns:
            if not pd.api.types.is_numeric_dtype(df_stata[col]):
                df_stata[col] = df_stata[col].fillna('').astype(str).apply(lambda x: x[:244])
        df_stata.to_stata(dta_path, write_index=False, version=118)
    except Exception as e:
         print(f"Error saving DTA: {e}")

    print(f"Phase 2 Enrichment complete. Saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
