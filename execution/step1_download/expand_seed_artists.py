
import requests
import json
import time

def fetch_artists_by_tag(tag, limit=100, offset=0):
    """
    Fetches artists from MusicBrainz with a specific tag.
    Returns names and count.
    """
    url = f"https://musicbrainz.org/ws/2/artist/"
    params = {
        "query": f"tag:{tag} AND (country:US OR area:US)",
        "fmt": "json",
        "limit": limit,
        "offset": offset
    }
    headers = {
        "User-Agent": "SoundOfCultureResearch/1.0 ( marco.lm86@libero.it )"
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            artists = [a['name'] for a in data.get('artists', [])]
            total = data.get('count', 0)
            return artists, total
        else:
            print(f"Error fetching {tag}: {response.status_code}")
            return [], 0
    except Exception as e:
        print(f"Exception for {tag}: {e}")
        return [], 0

def expand_seeds():
    tags = ["country", "folk", "americana", "bluegrass", "rockabilly", "outlaw country", "western swing"]
    all_new_artists = set()
    
    # Load existing
    seed_file = "data/intermediate/json/seed_country_artists.json"
    with open(seed_file, 'r') as f:
        existing_artists = set(json.load(f))
    
    print(f"Starting with {len(existing_artists)} existing seed artists.")
    
    for tag in tags:
        print(f"Processing tag: {tag}...")
        # Get first batch of 100
        artists, total = fetch_artists_by_tag(tag, limit=100)
        all_new_artists.update(artists)
        print(f"  Found {total} total for {tag}. Added {len(artists)} initial.")
        
        # Get up to 500 per tag (conservative to avoid massive overlaps)
        for offset in range(100, min(500, total), 100):
            time.sleep(1) # MB rate limit: 1 req/sec
            batch, _ = fetch_artists_by_tag(tag, limit=100, offset=offset)
            all_new_artists.update(batch)
            print(f"  Offset {offset}: {len(all_new_artists)} unique discovered artists total.")
            
    final_seeds = sorted(list(existing_artists.union(all_new_artists)))
    print(f"\nFinal seed list size: {len(final_seeds)} artists (Added {len(final_seeds) - len(existing_artists)} new ones)")
    
    with open(seed_file, 'w') as f:
        json.dump(final_seeds, f, indent=4)
    print(f"Updated {seed_file}")

if __name__ == "__main__":
    expand_seeds()
