import pandas as pd
import discogs_client
import time
import json
import os
import sys

# User Agent for Discogs (Mandatory)
# No token provided as default; user can add their own for higher rate limits
d = discogs_client.Client('SoundOfCultureEnrichment/1.0', user_token=None)

# Paths relative to replication root (script is in scripts/)
script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(script_dir, "../output/Sound_of_Culture_Country_Full_v1.csv")
OUTPUT_CSV = os.path.join(script_dir, "../output/Sound_of_Culture_Country_Full_v2.csv")
CACHE_FILE = os.path.join(script_dir, "../data/release_years_cache_discogs.json")

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
        # Search by artist and title
        results = d.search(song, artist=artist, type='release')
        
        # Get first page of results
        page = results.page(1)
        if not page:
            return None
            
        # Check first 5 matches for artist relevance
        for release in page[:5]:
            year = getattr(release, 'year', None)
            if year and isinstance(year, int) and year > 1900:
                return year
        return None
        
    except Exception as e:
        # Respect rate limits on error
        if "429" in str(e):
             time.sleep(30)
        return "ERROR_RETRY"

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Input file {INPUT_CSV} not found.")
        return

    df = pd.read_csv(INPUT_CSV)
    cache = load_cache()
    
    # Target only missing years (NaN or 0)
    df['release_year'] = pd.to_numeric(df.get('release_year'), errors='coerce')
    missing_mask = df['release_year'].isna() | (df['release_year'] == 0)
    unique_missing = df[missing_mask][['artist_name', 'song_name']].drop_duplicates().values
    total_missing = len(unique_missing)
    
    print(f"Starting Phase 3 Enrichment (Discogs) for {total_missing} missing songs...")

    count = 0
    for artist, song in unique_missing:
        key = f"{artist} - {song}"
        
        if key in cache and cache[key] != "ERROR_RETRY":
            continue
            
        # Respect Discogs limit (roughly 2-3 seconds for safety)
        time.sleep(3.0)
        
        year = get_release_year_discogs(artist, song)
        cache[key] = year
        
        count += 1
        if count % 10 == 0:
            save_cache(cache)
            print(f"Progress: {count}/{total_missing} processed. Found: {year}")

    save_cache(cache)
    
    print("Mapping Discogs years back to the dataset...")
    def find_year(row):
        if pd.isna(row['release_year']) or row['release_year'] == 0:
            key = f"{row['artist_name']} - {row['song_name']}"
            val = cache.get(key)
            if val and val != "ERROR_RETRY":
                 return val
        return row['release_year']

    df['release_year'] = df.apply(find_year, axis=1)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Phase 3 complete. Saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
