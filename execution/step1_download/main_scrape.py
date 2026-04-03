import json
import os
import time
import random
from scraper_client import UltimateGuitarClient

def load_input(filepath):
    """Loads the list of items to scrape from a JSON file."""
    if not os.path.exists(filepath):
        print(f"Input file {filepath} not found.")
        return []
    with open(filepath, 'r') as f:
        return json.load(f)

def save_tab(artist, song, tab_id, data, output_dir, year=None):
    """Saves the tab data to a JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    if year:
         data["chart_year"] = year

    # Clean filenames
    safe_artist = "".join(c for c in artist if c.isalnum() or c in (' ', '_', '-')).rstrip()
    safe_song = "".join(c for c in song if c.isalnum() or c in (' ', '_', '-')).rstrip()
    
    filename = f"{safe_artist}_{safe_song}_{tab_id}.json".replace(" ", "_")
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Saved: {filename}")

def main():
    import sys
    input_file = sys.argv[1] if len(sys.argv) > 1 else "data/intermediate/json/input_songs.json"

    # Output directory will be set dynamically inside the loop
    
    # Initialize client
    client = UltimateGuitarClient()
    print(f"Orchestrator started. Device ID: {client.device_id}")


    # Load items
    items = load_input(input_file)
    if not items:
        print("No items to process. Please create data/intermediate/json/input_songs.json with a list of songs/artists.")
        return

    print(f"Processing {len(items)} queries...")
    
    for idx, item in enumerate(items):
        artist = item.get("artist")
        title = item.get("title")
        year = item.get("year")
        
        if not artist or not title:
             continue
             
        query = f"{artist} {title}"
        tab_type = item.get("type", "Chords")
        limit = item.get("limit", 5)
        
        output_dir = f"data/raw_tabs_{tab_type.lower()}"
        
        # --- RESUME SKIP CHECK ---
        safe_artist = "".join(c for c in artist if c.isalnum() or c in (' ', '_', '-')).rstrip().replace(" ", "_")
        safe_song = "".join(c for c in title if c.isalnum() or c in (' ', '_', '-')).rstrip().replace(" ", "_")
        
        existing = []
        if os.path.exists(output_dir):
             existing = [f for f in os.listdir(output_dir) if f.startswith(f"{safe_artist}_{safe_song}_")]

        if existing:
             print(f"[{idx+1}/{len(items)}] Skipping '{query}', already downloaded ({len(existing)} versions).")
             continue
             
        print(f"\n[{idx+1}/{len(items)}] Searching for: '{query}' (Year: {year})")

        
        try:
            results = client.search(query, tab_type=tab_type)
            tabs = results.get("tabs", [])
            print(f"Found {len(tabs)} matches.")
            
            # Take up to 'limit' tabs
            count = 0
            for t in tabs:
                if count >= limit:
                    break
                    
                tab_id = t.get("id")
                song_name = t.get("song_name")
                artist_name = t.get("artist_name")
                
                print(f"  -> Fetching Tab ID: {tab_id} ({song_name} - {artist_name})")
                try:
                    full_info = client.get_tab_info(tab_id)
                    save_tab(artist_name, song_name, tab_id, full_info, output_dir, year=year)
                    count += 1

                except Exception as e:
                    print(f"  Error fetching ID {tab_id}: {e}")
                
                # Sleep to respect API
                time.sleep(random.uniform(1.0, 2.5))
                
        except Exception as e:
            print(f"Error searching for '{query}': {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
