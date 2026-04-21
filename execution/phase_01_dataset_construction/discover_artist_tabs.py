
import csv
import json
import os
import time
import random
import sys

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from execution.phase_01_dataset_construction.scraper_client import UltimateGuitarClient

def load_artists(csv_path):
    artists = []
    if not os.path.exists(csv_path):
        print(f"Error: CSV not found at {csv_path}")
        return []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            artists.append(row['name_primary'])
    return artists

def discover_for_artist(client, artist_name):
    all_tabs = []
    page = 1
    
    while True:
        try:
            print(f"  Searching '{artist_name}' - Page {page}...")
            results = client.search(artist_name, page=page)
            tabs = results.get('tabs', [])
            
            if not tabs:
                break
                
            # Filter and collect
            count_added = 0
            for t in tabs:
                # Basic fuzzy check: ensure artist name is at least mentioned
                if artist_name.lower() in t.get('artist_name', '').lower():
                    all_tabs.append({
                        "id": t.get('id'),
                        "song_name": t.get('song_name'),
                        "artist_name": t.get('artist_name'),
                        "type": t.get('type'),
                        "rating": t.get('rating'),
                        "votes": t.get('votes')
                    })
                    count_added += 1
            
            print(f"  Added {count_added} tabs from page {page}.")
            
            # If we got less than 50 tabs, it's likely the last page
            if len(tabs) < 50:
                break
                
            page += 1
            if page > 20: # Safety cap for extremely prolific artists (rare)
                break
                
            time.sleep(random.uniform(0.5, 1.5))
            
        except Exception as e:
            print(f"  Error on page {page} for {artist_name}: {e}")
            break
            
    return all_tabs

def main():
    artist_csv = "data/phase_01_dataset_construction/processed/country_artists/country_artists_master.csv"
    output_json = "data/phase_01_dataset_construction/intermediate/json/input_songs_bulk_country.json"
    
    client = UltimateGuitarClient()
    artists = load_artists(artist_csv)
    print(f"Loaded {len(artists)} artists from database.")
    
    # Resume logic
    processed_artists = set()
    all_discovered_tabs = []
    
    if os.path.exists(output_json):
        try:
            with open(output_json, 'r') as f:
                data = json.load(f)
                all_discovered_tabs = data.get('tabs', [])
                processed_artists = set(data.get('processed_artists', []))
                print(f"Resuming: {len(processed_artists)} artists already processed. {len(all_discovered_tabs)} tabs found so far.")
        except:
            print("Could not load existing output, starting fresh.")

    # Sort artists to ensure deterministic order
    artists.sort()
    
    try:
        for i, artist in enumerate(artists):
            if artist in processed_artists:
                continue
                
            print(f"[{i+1}/{len(artists)}] Discovering for: {artist}")
            artist_tabs = discover_for_artist(client, artist)
            all_discovered_tabs.extend(artist_tabs)
            processed_artists.add(artist)
            
            # Periodically save
            if (i + 1) % 10 == 0:
                with open(output_json, 'w') as f:
                    json.dump({
                        "tabs": all_discovered_tabs,
                        "processed_artists": list(processed_artists)
                    }, f, indent=2)
                print(f"--- Saved progress. Total tabs: {len(all_discovered_tabs)} ---")
            
            # Rate limiting between artists
            time.sleep(random.uniform(1.0, 2.5))
            
    except KeyboardInterrupt:
        print("\nInterrupted by user. Saving progress...")
    finally:
        # Final save
        with open(output_json, 'w') as f:
            json.dump({
                "tabs": all_discovered_tabs,
                "processed_artists": list(processed_artists)
            }, f, indent=2)
        print(f"Final discovery saved to {output_json}")
        print(f"Found total of {len(all_discovered_tabs)} tabs for {len(processed_artists)} artists.")

if __name__ == "__main__":
    main()
