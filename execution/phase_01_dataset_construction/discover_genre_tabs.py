import requests
from bs4 import BeautifulSoup
import json
import time
import random
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)
from execution.phase_01_dataset_construction.scraper_client import UltimateGuitarClient

def discover_genre(genre_id=None, subgenre_id=None, limit_pages=5):
    """
    Scrapes the 'Explore' section of Ultimate Guitar for a specific genre/subgenre.
    Returns a list of discovered tabs.
    """
    base_url = "https://www.ultimate-guitar.com/explore"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
    }
    
    params = {}
    if genre_id:
        params["genres[]"] = genre_id
    if subgenre_id:
        params["subgenres[]"] = subgenre_id
        
    discovered_tabs = []
    
    for page in range(1, limit_pages + 1):
        params["page"] = page
        print(f"Fetching page {page}...")
        
        try:
            response = requests.get(base_url, params=params, headers=headers, timeout=15)
            if response.status_code != 200:
                print(f"Error {response.status_code} on page {page}")
                break
                
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Ultimate Guitar stores its data in a JSON object inside a script tag
            # We look for window.UGAPP.store.page
            script_tag = soup.find('script', text=lambda t: t and 'window.UGAPP.store.page' in t)
            if not script_tag:
                print(f"Could not find data script on page {page}")
                # Fallback to visual parsing if needed, but UG is very JS-heavy
                break
                
            # Extract JSON from script
            json_text = script_tag.string.strip()
            # Find the start and end of the JSON object
            start_idx = json_text.find('{')
            end_idx = json_text.rfind('}') + 1
            data = json.loads(json_text[start_idx:end_idx])
            
            # Navigate to the tabs list
            # Structure: data['data']['data']['tabs']
            tabs = data.get('data', {}).get('data', {}).get('tabs', [])
            
            if not tabs:
                print(f"No more tabs found on page {page}")
                break
                
            for t in tabs:
                discovered_tabs.append({
                    "id": t.get('id'),
                    "song_name": t.get('song_name'),
                    "artist_name": t.get('artist_name'),
                    "type": t.get('type'),
                    "rating": t.get('rating'),
                    "votes": t.get('votes'),
                    "date": t.get('date')
                })
            
            print(f"Found {len(tabs)} tabs on page {page}. Total: {len(discovered_tabs)}")
            
            # Random delay
            time.sleep(random.uniform(2, 5))
            
        except Exception as e:
            print(f"Failed on page {page}: {e}")
            break
            
    return discovered_tabs

if __name__ == "__main__":
    # Test for Americana (Subgenre 72) - smaller set
    print("Starting discovery for Americana (Subgenre 72)...")
    americana_tabs = discover_genre(subgenre_id=72, limit_pages=2)
    
    # Save test results
    output_file = "data/discovery_test_americana.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        json.dump(americana_tabs, f, indent=4)
    
    print(f"Saved {len(americana_tabs)} tabs to {output_file}")
