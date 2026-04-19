
import sys
import os
import requests
import json

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if project_root not in sys.path:
    sys.path.append(project_root)
from execution.phase_01_dataset_construction.scraper_client import UltimateGuitarClient

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
