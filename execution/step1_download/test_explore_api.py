
import sys
import os
import requests
import json

# Add current dir to path to import scraper_client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper_client import UltimateGuitarClient

def test_explore_api():
    client = UltimateGuitarClient()
    url = f"{client.API_ENDPOINT}/tab/explore"
    
    print("Testing 'tab/explore' with Country filter (49)...")
    params = {
        "page": 1,
        "type[]": 300, # Chords
        "genres[]": 49
    }
    response = requests.get(url, params=params, headers=client._get_headers())
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return

    data = response.json()
    # In explore API, the structure is different. It's usually { "tabs": [...] } or in a data field.
    tabs = data.get("tabs", [])
    if not tabs and "data" in data:
        tabs = data["data"].get("tabs", [])
        
    print(f"Found {len(tabs)} tabs.")
    for t in tabs[:10]:
        print(f" - {t.get('artist_name')} - {t.get('song_name')} (ID: {t.get('id')}) (Genre: {t.get('genre')})")

    # Save full response for inspection
    with open("data/explore_api_result.json", "w") as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    test_explore_api()
