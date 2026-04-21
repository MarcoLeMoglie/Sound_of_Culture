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

def load_discovery(discovery_file):
    if not os.path.exists(discovery_file):
        print(f"Error: {discovery_file} not found.")
        return []
    with open(discovery_file, "r") as f:
        return json.load(f)

def download_worker(tab_metadata, client, output_dir, total_count):
    global completed_count
    tab_id = tab_metadata['id']
    artist = tab_metadata['artist_name'].replace("/", "_").replace(" ", "_")
    song = tab_metadata['song_name'].replace("/", "_").replace(" ", "_")
    filename = f"{artist}_{song}_{tab_id}.json"
    filepath = os.path.join(output_dir, filename)

    # Skip if already exists
    if os.path.exists(filepath):
        with counter_lock:
            completed_count += 1
        return "exists"

    # Retry logic for 429
    max_retries = 3
    for attempt in range(max_retries):
        try:
            tab_data = client.get_tab_info(tab_id)
            if not tab_data:
                return "empty"
                
            raw_json = tab_data.get('raw_json', {})
            
            with open(filepath, "w") as f:
                json.dump(raw_json, f, indent=2)
            
            with counter_lock:
                completed_count += 1
                if completed_count % 10 == 0 or completed_count == total_count:
                    with print_lock:
                        print(f"Progress: {completed_count}/{total_count} ({(completed_count/total_count)*100:.1f}%)", flush=True)
            
            # Small random delay between requests per thread
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
    discovery_file = "data/phase_01_dataset_construction/intermediate/json/input_songs_bulk_country_expansion.json"
    output_dir = "data/phase_01_dataset_construction/raw/ultimate_guitar_country_chords"
    NUM_WORKERS = 5 # Increased workers for expansion download
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

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
            # Results are handled in workers
            pass

    print("Bulk download complete.")

if __name__ == "__main__":
    main()
