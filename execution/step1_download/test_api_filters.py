import sys
import os
import json

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)
from execution.phase_01_dataset_construction.scraper_client import UltimateGuitarClient

def test_api_filters():
    client = UltimateGuitarClient()
    print(f"Testing API with Device ID: {client.device_id}")
    
    # Try search with genre filter (guessing params)
    print("\nAttempting search with genre=49 (Country)...")
    try:
        url = f"{client.API_ENDPOINT}/tab/search"
        params = {
            "page": 1,
            "genres[]": 49, # Try list format
            "type": 300      # Chords
        }
        headers = client._get_headers()
        import requests
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            tabs = data.get('tabs', [])
            print(f"Found {len(tabs)} tabs with genres[]=49")
            if tabs:
                print(f"Sample: {tabs[0].get('song_name')} by {tabs[0].get('artist_name')}")
        else:
            print(f"Error {response.status_code}: {response.text}")
            
        # Try single value
        params["genre"] = 49
        del params["genres[]"]
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            tabs = data.get('tabs', [])
            print(f"Found {len(tabs)} tabs with genre=49")

    except Exception as e:
        print(f"API Test failed: {e}")

if __name__ == "__main__":
    test_api_filters()
