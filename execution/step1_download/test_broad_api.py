import sys
import os
import json
import requests

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)
from execution.phase_01_dataset_construction.scraper_client import UltimateGuitarClient

def test_broad_api():
    client = UltimateGuitarClient()
    url = f"{client.API_ENDPOINT}/tab/search"
    headers = client._get_headers()
    
    # Try searching for a very common character with genre filter
    params = {
        "title": "a", # Common letter
        "genres[]": 49, # Country
        "type": 300, # Chords
        "page": 1
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            tabs = data.get('tabs', [])
            total = data.get('total', 0)
            print(f"API Result for title='a' and genres[]=49: {len(tabs)} tabs found. Total theoretical: {total}")
            if tabs:
                print(f"Sample: {tabs[0].get('song_name')} by {tabs[0].get('artist_name')}")
        else:
            print(f"Error {response.status_code}: {response.text}")
            
    except Exception as e:
        print(f"Broad API Test failed: {e}")

if __name__ == "__main__":
    test_broad_api()
