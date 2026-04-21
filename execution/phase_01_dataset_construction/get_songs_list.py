import requests
from bs4 import BeautifulSoup
import json
import os

years = [1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2024]
output_dir = "data/phase_02_exploratory_analysis/processed/top100YearEnd8525"
output_file = os.path.join(output_dir, "songs_tracklist_top100yearend.json")
os.makedirs(os.path.dirname(output_file), exist_ok=True)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36"
}

all_songs = []

for year in years:
    url = f"https://en.wikipedia.org/wiki/Billboard_Year-End_Hot_100_singles_of_{year}"
    print(f"Scraping {year}...")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
             soup = BeautifulSoup(response.content, 'html.parser')
             table = soup.find('table', {'class': 'wikitable'})
             if table:
                  rows = table.find_all('tr')[1:] # Skip header
                  count = 0
                  for r in rows:
                       cols = r.find_all(['td', 'th'])
                       if len(cols) >= 3:
                            title_col = cols[1]
                            artist_col = cols[2]
                            
                            title = title_col.text.strip().strip('"')
                            artist = artist_col.text.strip()
                            
                            all_songs.append({
                                "year": year,
                                "title": title,
                                "artist": artist
                            })
                            count += 1
                  print(f"Found {count} songs")
             else:
                  print("No table found")
        else:
             print(f"Error {response.status_code}")
    except Exception as e:
        print(f"Failed {year}: {e}")

with open(output_file, 'w') as f:
    json.dump(all_songs, f, indent=4)
print(f"Saved {len(all_songs)} songs to {output_file}")
