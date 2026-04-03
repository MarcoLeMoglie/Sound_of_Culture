
import os
import sys
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add current dir to path to import scraper_client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper_client import UltimateGuitarClient

def discover_artist_chords(artist_name, client):
    """
    Searches for all 'Chords' tabs for a specific artist.
    Returns a list of tab metadata.
    """
    discovered = []
    page = 1
    max_pages = 5 # Should be enough for most artists
    
    while page <= max_pages:
        try:
            # We search for artist name. API returns tabs matching this artist.
            data = client.search(artist_name, page=page)
            tabs = data.get('tabs', [])
            if not tabs:
                break
                
            for t in tabs:
                # Strict check: artist name must match (case-insensitive)
                if t.get('type') == 'Chords' and artist_name.lower() in t.get('artist_name', '').lower():
                    discovered.append({
                        "id": t.get('id'),
                        "artist_name": t.get('artist_name'),
                        "song_name": t.get('song_name'),
                        "rating": t.get('rating'),
                        "votes": t.get('votes'),
                        "type": t.get('type')
                    })
            
            # Check if there are more pages
            pagination = data.get('pagination', {})
            total_pages = pagination.get('total', 1)
            if page >= total_pages:
                break
            
            page += 1
            time.sleep(random.uniform(1.0, 2.0)) # Small delay between search pages
            
        except Exception as e:
            print(f"Error searching {artist_name} page {page}: {e}")
            break
            
    return discovered

def process_discovery():
    client = UltimateGuitarClient()
    seed_file = "data/intermediate/json/seed_country_artists.json"
    output_file = "data/intermediate/json/input_songs_bulk_country.json"
    progress_file = "data/intermediate/json/discovery_progress_artists.json"
    
    if not os.path.exists(seed_file):
        print(f"Error: {seed_file} not found.")
        return
        
    with open(seed_file, 'r') as f:
        artists = json.load(f)
        
    print(f"Starting discovery for {len(artists)} artists...")
    
    # Load progress if exists
    processed_artists = set()
    all_tabs = []
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            progress_data = json.load(f)
            processed_artists = set(progress_data.get('processed_artists', []))
            all_tabs = progress_data.get('tabs', [])
            print(f"Resuming discovery. {len(processed_artists)} artists already processed.")

    # We use a ThreadPool for faster discovery (searches are faster than info loads)
    # But we keep it conservative to avoid 429
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_artist = {executor.submit(discover_artist_chords, a, client): a for a in artists if a not in processed_artists}
        
        count = len(processed_artists)
        for future in as_completed(future_to_artist):
            artist = future_to_artist[future]
            try:
                artist_tabs = future.result()
                all_tabs.extend(artist_tabs)
                processed_artists.add(artist)
                count += 1
                
                if count % 10 == 0 or count == len(artists):
                    print(f"Progress: {count}/{len(artists)} artists. Found {len(all_tabs)} total tabs.")
                    # Intermediate save
                    with open(progress_file, 'w') as f:
                        json.dump({
                            "processed_artists": list(processed_artists),
                            "tabs": all_tabs
                        }, f, indent=4)
                        
            except Exception as e:
                print(f"Exception for artist {artist}: {e}")

    # Final filtering: Keep Top 3 versions per song
    print(f"\nDiscovery complete. Found {len(all_tabs)} raw tabs.")
    print("Filtering for Top 3 versions per unique song...")
    
    song_map = {}
    for t in all_tabs:
        key = (t['artist_name'].lower(), t['song_name'].lower())
        if key not in song_map:
            song_map[key] = []
        song_map[key].append(t)
        
    final_list = []
    for key, versions in song_map.items():
        # Sort by votes then rating
        sorted_versions = sorted(versions, key=lambda x: (x.get('votes', 0), x.get('rating', 0)), reverse=True)
        # Take Top 3
        top_3 = sorted_versions[:3]
        final_list.extend(top_3)
        
    print(f"Final Count: {len(final_list)} Chord versions from {len(song_map)} unique songs.")
    
    with open(output_file, 'w') as f:
        json.dump(final_list, f, indent=4)
        
    print(f"Results saved to {output_file}")
    
    # Remove progress file on success
    if os.path.exists(progress_file):
        os.remove(progress_file)

if __name__ == "__main__":
    process_discovery()
