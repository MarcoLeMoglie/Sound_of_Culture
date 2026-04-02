import json
import os
import time
import random
import sys

# Add current dir to path to import scraper_client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper_client import UltimateGuitarClient

def discover_best_chords():
    client = UltimateGuitarClient()
    genres = [49, 72] # Country, Americana
    # Query grid to maximize coverage (API requires title)
    queries = [" ", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", 
               "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
               "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
    
    output_file = "data/input_songs_bulk_country.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Global dictionary to track up to 3 best versions per song: (artist, song) -> [tab_info, ...]
    best_tabs_per_song = {}
    
    processed_queries = []
    if os.path.exists("data/discovery_progress.json"):
        with open("data/discovery_progress.json", "r") as f:
            processed_queries = json.load(f)
            print(f"Resuming. Already processed {len(processed_queries)} queries.")

    for query in queries:
        if query in processed_queries:
            continue
            
        for g_id in genres:
            print(f"\nSearching genre {g_id} with query '{query}'...")
            
            for page in range(1, 101): # API usually limit to 100 pages
                try:
                    # Use the search method (modified/tested to accept genre)
                    # Note: We use manual request because search() in scraper_client is simple
                    url = f"{client.API_ENDPOINT}/tab/search"
                    params = {
                        "title": query,
                        "page": page,
                        "type": 300, # Chords
                        "genres[]": g_id
                    }
                    headers = client._get_headers()
                    import requests
                    response = requests.get(url, params=params, headers=headers, timeout=15)
                    
                    if response.status_code != 200:
                        print(f"  Error {response.status_code} on page {page}")
                        break
                        
                    data = response.json()
                    tabs = data.get("tabs", [])
                    
                    if not tabs:
                        print(f"  No more tabs on page {page}")
                        break
                    
                    added_count = 0
                    for t in tabs:
                        artist = t.get("artist_name", "Unknown")
                        song = t.get("song_name", "Unknown")
                        key = (artist.lower(), song.lower())
                        
                        rating = t.get("rating", 0.0)
                        votes = t.get("votes", 0)
                        
                        tab_info = {
                            "id": t["id"],
                            "artist_name": artist,
                            "song_name": song,
                            "rating": rating,
                            "votes": votes,
                            "type": "Chords"
                        }

                        if key not in best_tabs_per_song:
                            best_tabs_per_song[key] = [tab_info]
                            added_count += 1
                        else:
                            versions = best_tabs_per_song[key]
                            # Check if ID already in list
                            if any(v["id"] == t["id"] for v in versions):
                                continue
                                
                            versions.append(tab_info)
                            # Sort by rating desc, then votes desc
                            versions.sort(key=lambda x: (x["rating"], x["votes"]), reverse=True)
                            # Keep only top 3
                            best_tabs_per_song[key] = versions[:3]
                            # Check if current tab is in the top 3
                            if any(v["id"] == t["id"] for v in best_tabs_per_song[key]):
                                added_count += 1
                    
                    print(f"  Page {page}: Found {len(tabs)} tabs. New versions in top 3: {added_count}")
                    
                    # Random delay
                    time.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    print(f"  Exception on page {page}: {e}")
                    break
        
        # Save progress after each query
        processed_queries.append(query)
        with open("data/discovery_progress.json", "w") as f:
            json.dump(processed_queries, f)
            
        # Save current best list
        # Flatten the results for saving
        all_tabs = []
        for versions in best_tabs_per_song.values():
            all_tabs.extend(versions)
            
        with open(output_file, "w") as f:
            json.dump(all_tabs, f, indent=4)
        
        print(f"--- Query '{query}' finished. Total unique songs: {len(best_tabs_per_song)}, Total JSONs to download: {len(all_tabs)} ---")
        time.sleep(1)

    print(f"\nDiscovery complete! Total unique best chords found: {len(best_tabs)}")

if __name__ == "__main__":
    discover_best_chords()
