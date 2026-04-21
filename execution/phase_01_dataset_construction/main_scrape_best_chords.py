
import json
import os
import time
import random
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from execution.phase_01_dataset_construction.scraper_client import UltimateGuitarClient

# Thread-safe printing and count tracking
print_lock = Lock()
counter_lock = Lock()
completed_count = 0

def load_discovery(filepath):
    if not os.path.exists(filepath):
        print(f"Discovery file {filepath} not found.")
        return []
    with open(filepath, 'r') as f:
        data = json.load(f)
        return data

def save_tab(artist, song, tab_id, data, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    safe_artist = "".join(c for c in artist if c.isalnum() or c in (' ', '_', '-')).rstrip().replace(" ", "_")
    safe_song = "".join(c for c in song if c.isalnum() or c in (' ', '_', '-')).rstrip().replace(" ", "_")
    filename = f"{safe_artist}_{safe_song}_{tab_id}.json"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

def download_worker(t, client, output_dir, total_to_process):
    global completed_count
    tab_id = t['id']
    artist = t['artist_name']
    song = t['song_name']
    
    # Check if already downloaded
    safe_artist = "".join(c for c in artist if c.isalnum() or c in (' ', '_', '-')).rstrip().replace(" ", "_")
    safe_song = "".join(c for c in song if c.isalnum() or c in (' ', '_', '-')).rstrip().replace(" ", "_")
    filename = f"{safe_artist}_{safe_song}_{tab_id}.json"
    
    if os.path.exists(os.path.join(output_dir, filename)):
        with counter_lock:
            completed_count += 1
        return "skipped"

    # Rate limiting jitter per thread
    time.sleep(random.uniform(0.1, 1.0))
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            with print_lock:
                print(f"[{completed_count+1}/{total_to_process}] Downloading: {artist} - {song} (ID: {tab_id})", flush=True)
            
            full_info = client.get_tab_info(tab_id)
            save_tab(artist, song, tab_id, full_info, output_dir)
            
            with counter_lock:
                completed_count += 1
            
            # Delay between requests in the SAME thread
            time.sleep(random.uniform(2.0, 5.0))
            return "success"
            
        except Exception as e:
            if "429" in str(e):
                backoff = (attempt + 1) * 30
                with print_lock:
                    print(f"  Rate limit hit for ID {tab_id}. Backing off {backoff}s...", flush=True)
                time.sleep(backoff)
            elif "451" in str(e):
                with print_lock:
                    print(f"  ID {tab_id} unavailable for legal reasons (451). Skipping.", flush=True)
                with counter_lock:
                    completed_count += 1
                return "legal_unavailable"
            else:
                with print_lock:
                    print(f"  Error downloading ID {tab_id} (Attempt {attempt+1}): {e}", flush=True)
                time.sleep(5)
                
    return "failed"

def main():
    discovery_file = "data/phase_01_dataset_construction/intermediate/json/input_songs_bulk_country.json"
    output_dir = "data/phase_01_dataset_construction/raw/ultimate_guitar_country_chords"
    NUM_WORKERS = 3 # 3 concurrent workers for ~3x speedup
    
    client = UltimateGuitarClient()
    tabs_to_download = load_discovery(discovery_file)
    
    if not tabs_to_download:
        print("No tabs found to download.")
        return

    # Unique tabs by ID
    unique_tabs = list({t['id']: t for t in tabs_to_download}.values())
    total_to_process = len(unique_tabs)
    print(f"Total unique tabs to process: {total_to_process}")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(download_worker, t, client, output_dir, total_to_process) for t in unique_tabs]
        for future in as_completed(futures):
            # We don't necessarily need the result here as progress is printed in workers
            pass

    print("Bulk download complete.")

if __name__ == "__main__":
    main()
