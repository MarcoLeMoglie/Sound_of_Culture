
import requests
from bs4 import BeautifulSoup
import json
import os
import time

def scrape_billboard_country():
    years = list(range(1980, 2025))
    output_file = "data/intermediate/json/billboard_country_artists.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
    }
    
    all_artists = set()
    all_songs = []
    
    for year in years:
        url = f"https://en.wikipedia.org/wiki/Billboard_Year-End_Hot_Country_Songs_of_{year}"
        print(f"Scraping {year} from Wikipedia...")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                # Some years might have different URL aliases
                url_alt = f"https://en.wikipedia.org/wiki/List_of_Billboard_Hot_Country_Songs_number-one_singles_of_{year}"
                print(f"  Trying alternate URL for {year}...")
                response = requests.get(url_alt, headers=headers, timeout=15)
                
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                table = soup.find('table', {'class': 'wikitable'})
                if not table:
                    # Try finding any table if 'wikitable' is not explicitly used
                    table = soup.find('table')
                
                if table:
                    rows = table.find_all('tr')[1:]
                    for r in rows:
                        cols = r.find_all(['td', 'th'])
                        if len(cols) >= 2:
                            # Typically: Title, Artist or Artist, Title
                            # We'll try to guess based on common patterns
                            text_items = [c.get_text(strip=True).strip('"') for c in cols]
                            # Heuristic: The shorter text item is usually the artist or title? No.
                            # Usually cols[0] is Rank/Title, cols[1] is Artist.
                            # In most Billboard Year-End tables: Title is 2nd, Artist is 3rd.
                            if len(cols) >= 3:
                                title = text_items[1]
                                artist = text_items[2]
                            else:
                                title = text_items[0]
                                artist = text_items[1]
                                
                            all_artists.add(artist)
                            all_songs.append({"year": year, "title": title, "artist": artist})
                    print(f"  Processed {year}. Artist count so far: {len(all_artists)}")
                else:
                    print(f"  No table found for {year}")
            else:
                print(f"  Failed {year}: {response.status_code}")
        except Exception as e:
            print(f"  Error {year}: {e}")
        
        time.sleep(0.5)

    # Save unique artists
    sorted_artists = sorted(list(all_artists))
    with open(output_file, 'w') as f:
        json.dump(sorted_artists, f, indent=4)
    
    print(f"\nScraping complete! Total unique country artists discovered: {len(sorted_artists)}")
    print(f"Artist list saved to {output_file}")

if __name__ == "__main__":
    scrape_billboard_country()
