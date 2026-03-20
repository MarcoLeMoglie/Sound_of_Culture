import subprocess
from bs4 import BeautifulSoup
import json

url = "https://en.wikipedia.org/wiki/Billboard_Year-End_Hot_100_singles_of_1985"
try:
    process = subprocess.Popen(["curl", "-s", url], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    
    if process.returncode == 0:
        soup = BeautifulSoup(stdout, 'html.parser')
        table = soup.find('table', {'class': 'wikitable'})
        if table:
             rows = table.find_all('tr')[1:]
             songs_1985 = []
             count = 0
             for r in rows:
                  cols = r.find_all(['td', 'th'])
                  if len(cols) >= 3:
                       title = cols[1].text.strip().strip('"')
                       artist = cols[2].text.strip()
                       songs_1985.append({
                           "year": 1985,
                           "title": title,
                           "artist": artist
                       })
                       count += 1
             print(f"Found {count} songs for 1985")
             
             # Load existing
             with open("data/processed_datasets/songs_tracklist.json", 'r') as f:
                  all_songs = json.load(f)
                  
             # Extend
             all_songs.extend(songs_1985)
             
             # Save back
             with open("data/processed_datasets/songs_tracklist.json", 'w') as f:
                  json.dump(all_songs, f, indent=4)
             print("Successfully appended 1985.")
        else:
             print("No table found with curl.")
    else:
        print(f"Curl failed: {stderr.decode()}")
except Exception as e:
    print(f"Error: {e}")
