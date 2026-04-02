import json
import os

input_file = "data/processed_datasets/top100YearEnd8525/songs_tracklist_top100yearend.json"
output_file = "data/input_songs_bulk_bass.json"

if not os.path.exists(input_file):
    print("Tracklist file not found.")
    exit(1)

with open(input_file, 'r') as f:
    songs = json.load(f)

bulk_items = []
for s in songs:
    artist = s.get("artist")
    title = s.get("title")
    year = s.get("year")
    if artist and title:
         bulk_items.append({
             "artist": artist,
             "title": title,
             "year": year,
             "type": "Bass",
             "limit": 5
         })


with open(output_file, 'w') as f:
    json.dump(bulk_items, f, indent=4)

print(f"Created {len(bulk_items)} queries in {output_file}")
