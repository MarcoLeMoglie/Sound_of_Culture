import hashlib
import requests
from datetime import datetime, timezone
import random
import string

class UltimateGuitarClient:
    API_ENDPOINT = "https://api.ultimate-guitar.com/api/v1"
    USER_AGENT = "UGT_ANDROID/4.11.1 (Pixel; 8.1.0)"

    def __init__(self, device_id=None):
        if device_id is None:
            self.device_id = self._generate_device_id()
        else:
            self.device_id = device_id
        
        # Mapping of Tab Types based on Go tool analysis
        self.TAB_TYPES = {
            "Video": 100,
            "Tabs": 200,
            "Chords": 300,
            "Bass": 400,
            "Pro": 500,
            "Power": 600,
            "Drums": 700,
            "Ukulele": 800,
            "Official": 900
        }

    def _generate_device_id(self):
        """Generates a random 16-character hex device ID."""
        return ''.join(random.choices(string.hexdigits.lower(), k=16))

    def _generate_api_key(self):
        """Generates the X-UG-API-KEY signature."""
        now_utc = datetime.now(timezone.utc)
        date_str = now_utc.strftime("%Y-%m-%d")
        hour = now_utc.hour
        # Formula: DeviceID + DateStr + Hour + createLog()
        payload = f"{self.device_id}{date_str}:{hour}createLog()"
        return hashlib.md5(payload.encode()).hexdigest()

    def _get_headers(self):
        """Builds default headers with signature."""
        api_key = self._generate_api_key()
        return {
            "Accept-Charset": "utf-8",
            "Accept": "application/json",
            "User-Agent": self.USER_AGENT,
            "Connection": "close",
            "X-UG-CLIENT-ID": self.device_id,
            "X-UG-API-KEY": api_key
        }

    def search(self, query, page=1, tab_type=None):
        """
        Search for tabs on Ultimate Guitar.
        :param query: Search string (song, artist, etc.)
        :param page: Page number
        :param tab_type: Filter by type (e.g. 'Tabs', 'Chords', 'Bass', 'Drums')
        """
        url = f"{self.API_ENDPOINT}/tab/search"
        params = {
            "title": query,
            "page": page
        }
        
        if tab_type:
            if isinstance(tab_type, str):
                type_id = self.TAB_TYPES.get(tab_type)
                if type_id:
                    params["type"] = type_id
            elif isinstance(tab_type, int):
                params["type"] = tab_type

        headers = self._get_headers()
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_tab_info(self, tab_id):
        """
        Fetch details for a specific tab by ID.
        :param tab_id: Ultimate Guitar Tab ID
        """
        url = f"{self.API_ENDPOINT}/tab/info"
        params = {
            "tab_id": tab_id,
            "tab_access_type": "private"
        }
        
        headers = self._get_headers()
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

# Demo Usage
if __name__ == "__main__":
    client = UltimateGuitarClient()
    print(f"Initialized with Device ID: {client.device_id}")
    
    # Test Search
    try:
        print("\nSearching for 'Hotel California' (Tabs)...")
        results = client.search("Hotel California", tab_type="Tabs")
        print(f"Found {len(results.get('tabs', []))} tabs.")
        if results.get('tabs'):
            first_tab = results['tabs'][0]
            print(f"First Result: {first_tab.get('song_name')} by {first_tab.get('artist_name')} (ID: {first_tab.get('id')})")
            
            # Test Get Info
            print(f"\nFetching info for ID: {first_tab.get('id')}...")
            info = client.get_tab_info(first_tab.get('id'))
            print(f"Song: {info.get('song_name')}")
            print(f"Artist: {info.get('artist_name')}")
            print(f"Content Length: {len(info.get('content', ''))} characters")
            print("\nContent Snippet:")
            print(info.get('content', '')[:200])
            
    except Exception as e:
        print(f"Error: {e}")
