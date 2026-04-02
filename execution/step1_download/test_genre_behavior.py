
import sys
import os
import requests
import json

# Add current dir to path to import scraper_client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper_client import UltimateGuitarClient

def test_genre_filter():
    client = UltimateGuitarClient()
    url = f"{client.API_ENDPOINT}/tab/search"
    
    # Test 1: Search "Afterhours" with Country genre (49)
    print("Testing 'Afterhours' with Country filter (49)...")
    params = {
        "title": "Afterhours",
        "page": 1,
        "type": 300,
        "genres[]": 49
    }
    response = requests.get(url, params=params, headers=client._get_headers())
    data = response.json()
    tabs = data.get("tabs", [])
    print(f"Found {len(tabs)} tabs.")
    for t in tabs[:5]:
        print(f" - {t['artist_name']} - {t['song_name']} (ID: {t['id']})")

    # Test 2: Search "Afterhours" without genre filter
    print("\nTesting 'Afterhours' WITHOUT genre filter...")
    params = {
        "title": "Afterhours",
        "page": 1,
        "type": 300
    }
    response = requests.get(url, params=params, headers=client._get_headers())
    data = response.json()
    tabs = data.get("tabs", [])
    print(f"Found {len(tabs)} tabs.")

    # Test 3: Search "Aerosmith" with Country genre (49)
    print("\nTesting 'Aerosmith' with Country filter (49)...")
    params = {
        "title": "Aerosmith",
        "page": 1,
        "type": 300,
        "genres[]": 49
    }
    response = requests.get(url, params=params, headers=client._get_headers())
    data = response.json()
    tabs = data.get("tabs", [])
    print(f"Found {len(tabs)} tabs.")

if __name__ == "__main__":
    test_genre_filter()
