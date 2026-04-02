import os
import sys
import json
import time
import random
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add current dir to path to import scraper_client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper_client import UltimateGuitarClient
from discover_artists_chords import discover_artist_chords

def run_expansion_discovery():
    client = UltimateGuitarClient()
    # Path relative to project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    output_file = os.path.join(project_root, 'data/input_songs_bulk_country_expansion.json')
    
    # 1. Extract combined new artists
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
    res_csv = os.path.join(project_root, 'data/processed_datasets/country_artists/country_artists_restricted.csv')
    mas_csv = os.path.join(project_root, 'data/processed_datasets/country_artists/country_artists_master.csv')
    seed_file = os.path.join(project_root, 'data/seed_country_artists.json')
    
    df_res = pd.read_csv(res_csv)
    df_mas = pd.read_csv(mas_csv)
    
    artists_csv = set(df_res['name_primary'].dropna().unique()) | set(df_mas['name_primary'].dropna().unique())
    if 'stage_name' in df_res.columns:
        artists_csv |= set(df_res['stage_name'].dropna().unique())
    if 'stage_name' in df_mas.columns:
        artists_csv |= set(df_mas['stage_name'].dropna().unique())
        
    with open(seed_file, 'r') as f:
        artists_seed = set(json.load(f))
        
    new_artists = sorted(list(artists_csv - artists_seed))
    print(f"Total new artists to search: {len(new_artists)}")
    
    # 2. Discovery with ThreadPool
    all_tabs = []
    # Using 3 workers for faster expansion discovery
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_artist = {executor.submit(discover_artist_chords, a, client): a for a in new_artists}
        
        for i, future in enumerate(as_completed(future_to_artist)):
            artist = future_to_artist[future]
            try:
                artist_tabs = future.result()
                all_tabs.extend(artist_tabs)
                if (i + 1) % 50 == 0:
                    print(f"Progress: {i+1}/{len(new_artists)} artists searched. Found {len(all_tabs)} tabs so far.")
            except Exception as e:
                print(f"Error for {artist}: {e}")

    # 3. Filtering Top 3 versions
    song_map = {}
    for t in all_tabs:
        key = (t['artist_name'].lower(), t['song_name'].lower())
        if key not in song_map:
            song_map[key] = []
        song_map[key].append(t)
        
    final_expansion = []
    for key, versions in song_map.items():
        sorted_v = sorted(versions, key=lambda x: (x.get('votes', 0), x.get('rating', 0)), reverse=True)
        final_expansion.extend(sorted_v[:3])
        
    print(f"Discovery complete. Total unique songs: {len(song_map)}. Total Chord versions to download: {len(final_expansion)}")
    
    with open(output_file, 'w') as f:
        json.dump(final_expansion, f, indent=4)
        
    # Also update seed list if desired, or let user decide
    print(f"Expansion metadata saved to {output_file}")

if __name__ == "__main__":
    run_expansion_discovery()
