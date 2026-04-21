import pandas as pd
import requests
import time
import json
import os
import re
import csv
from typing import Optional
from pathlib import Path

# Configuration
BASE_DIR = Path("data/phase_01_dataset_construction/processed/country_artists")
INPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full_Enriched_v3.csv"
OUTPUT_CSV = BASE_DIR / "Sound_of_Culture_Country_Full_Enriched_v4.csv"
CACHE_FILE = BASE_DIR / "intermediate" / "json_caches" / "release_years_cache_wiki.json"
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
USER_AGENT = "SoundOfCultureBot/1.0 (https://github.com/MarcoLeMoglie/Sound_of_Culture; marco@example.com) Requests/2.31.0"

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def get_wikidata_year(artist: str, song: str) -> Optional[int]:
    """Queries Wikidata for a musical work by artist and song title."""
    song_esc = song.replace('"', '\\"')
    artist_esc = artist.replace('"', '\\"')
    
    query = f"""
    SELECT DISTINCT ?year WHERE {{
      ?songItem rdfs:label ?songLabel .
      FILTER(LCASE(STR(?songLabel)) = LCASE("{song_esc}"))
      
      ?artistItem rdfs:label ?artistLabel .
      FILTER(LCASE(STR(?artistLabel)) = LCASE("{artist_esc}"))
      
      {{ ?songItem wdt:P175 ?artistItem . }} UNION {{ ?songItem wdt:P86 ?artistItem . }}
      
      ?songItem wdt:P577 ?date .
      BIND(YEAR(?date) AS ?year)
    }} LIMIT 1
    """
    
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'application/sparql-results+json'
    }
    
    # Retry logic
    for attempt in range(3):
        try:
            response = requests.get(WIKIDATA_SPARQL_URL, params={'query': query, 'format': 'json'}, headers=headers, timeout=12)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', {}).get('bindings', [])
                if results:
                    return int(results[0]['year']['value'])
                return None
            elif response.status_code == 429:
                time.sleep(10 * (attempt + 1))
            else:
                time.sleep(1)
        except (requests.exceptions.RequestException, Exception):
            time.sleep(2 * (attempt + 1))
            
    return None

def get_wikipedia_year(artist: str, song: str) -> Optional[int]:
    """Falls back to Wikipedia search for Infobox parsing."""
    search_url = "https://en.wikipedia.org/w/api.php"
    search_params = {
        "action": "query",
        "list": "search",
        "srsearch": f'"{song}" {artist} song',
        "format": "json"
    }
    
    headers = {'User-Agent': USER_AGENT}
    
    try:
        res = requests.get(search_url, params=search_params, headers=headers, timeout=10).json()
        search_results = res.get("query", {}).get("search", [])
        if not search_results:
            return None
            
        page_title = search_results[0]["title"]
        content_params = {
            "action": "query",
            "prop": "revisions",
            "titles": page_title,
            "rvprop": "content",
            "format": "json",
            "rvslots": "main"
        }
        
        res = requests.get(search_url, params=content_params, headers=headers, timeout=10).json()
        pages = res.get("query", {}).get("pages", {})
        page_id = list(pages.keys())[0]
        content = pages[page_id]["revisions"][0]["slots"]["main"]["*"]
        
        match = re.search(r'Released\s*=\s*.*?(\d{4})', content, re.IGNORECASE)
        if match:
            return int(match.group(1))
            
    except:
        pass
    return None

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Input file {INPUT_CSV} not found.")
        return

    # Use pandas read with error handling and encoding
    try:
        df = pd.read_csv(INPUT_CSV, encoding='latin-1', on_bad_lines='skip')
    except Exception as e:
        print(f"Pandas Read Error: {e}")
        return

    # Clean columns (handle quotes/spaces)
    df.columns = [c.replace('"', '').replace("'", "").strip() for c in df.columns]
    print(f"DEBUG FINAL: Columns={df.columns.tolist()}")
    
    required = ['artist_name', 'song_name', 'release_year']
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"ERROR: Missing columns: {missing}.")
        return

    cache = load_cache()
    
    # Identify missing tracks accurately
    missing_mask = df['release_year'].isna() | (df['release_year'].astype(str).str.lower().isin(['nan', '', 'none', '0', '0.0']))
    unique_missing = df[missing_mask][['artist_name', 'song_name']].drop_duplicates().values
    total_missing = len(unique_missing)
    
    print(f"Starting Wikipedia/Wikidata Enrichment for {total_missing} items...")
    
    count = 0
    new_found = 0
    
    print(f"Looping through {len(unique_missing)} items...")
    for artist, song in unique_missing:
        key = f"{artist} - {song}"
        # print(f"Processing: {key}") # Verbose
        if key in cache and cache[key] is not None:
            continue
            
        # 1. Wikidata
        year = get_wikidata_year(artist, song)
        
        # 2. Wikipedia
        if not year:
            year = get_wikipedia_year(artist, song)
            
        cache[key] = year
        count += 1
        if year:
            new_found += 1
            print(f"  [FOUND] {key} -> {year}")
            
        if count % 5 == 0:
            save_cache(cache)
            print(f"Progress: {count}/{total_missing}. Total found in this session: {new_found}")
            
    save_cache(cache)
    
    # Final merging
    print("Merging results...")
    def map_back(row):
        val = row['release_year']
        if pd.isna(val) or str(val).lower() in ['nan', '', 'none', '0', '0.0']:
            key = f"{row['artist_name']} - {row['song_name']}"
            return cache.get(key)
        return val
        
    df['release_year'] = df.apply(map_back, axis=1)
    df.to_csv(OUTPUT_CSV, index=False)
    
    # Export DTA
    dta_path = OUTPUT_CSV.replace(".csv", ".dta")
    try:
        df_stata = df.copy()
        for col in df_stata.columns:
            if not pd.api.types.is_numeric_dtype(df_stata[col]):
                df_stata[col] = df_stata[col].fillna('').astype(str).str.slice(0, 244)
        df_stata.to_stata(dta_path, write_index=False, version=118)
        print("Success.")
    except Exception as e:
        print(f"DTA Error: {e}")

if __name__ == "__main__":
    main()
