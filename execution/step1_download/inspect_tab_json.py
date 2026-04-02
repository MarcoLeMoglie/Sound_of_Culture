
import sys
import os
import requests
import json

# Add current dir to path to import scraper_client
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scraper_client import UltimateGuitarClient

def inspect_tab_metadata():
    client = UltimateGuitarClient()
    url = f"{client.API_ENDPOINT}/tab/search"
    params = {
        "title": "Afterhours",
        "page": 1,
        "type": 300
    }
    response = requests.get(url, params=params, headers=client._get_headers())
    data = response.json()
    tabs = data.get("tabs", [])
    if tabs:
        print(json.dumps(tabs[0], indent=4))

if __name__ == "__main__":
    inspect_tab_metadata()
