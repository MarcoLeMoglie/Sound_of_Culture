import json
import os

input_file = "data/phase_02_exploratory_analysis/processed/songs_tracklist.json"
output_file = "data/input_songs_test_scale.json"

if not os.path.exists(input_file):
    print("Tracklist file not found.")
    exit(1)

with open(input_file, 'r') as f:
    songs = json.load(f)

# Group by year to take a subset
by_year = {}
for s in songs:
    year = s.get("year")
    if year not in by_year:
        by_year[year] = []
    by_year[year].append(s)

sample_items = []
for year, tracklist in by_year.items():
    # Take top 2 from each year
    for s in tracklist[:2]:
         sample_items.append({
             "artist": s['artist'],
             "title": s['title'],
             "year": s['year'],
             "type": "Chords",
             "limit": 5
         })


with open(output_file, 'w') as f:
    json.dump(sample_items, f, indent=4)

print(f"Created {len(sample_items)} sample queries in {output_file}")
