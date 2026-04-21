import json
import os
import pandas as pd
from datetime import datetime
import re
from collections import Counter
import sys

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from execution.phase_01_dataset_construction.music_indices import calculate_indices

INSTRUMENT = "Chords"
INPUT_DIR = "data/phase_01_dataset_construction/raw/ultimate_guitar_country_chords"
OUTPUT_DIR = "data/phase_01_dataset_construction/processed/country_artists"
OUTPUT_FILENAME = "Sound_of_Culture_Country_Full"

def flatten_tab_data(filepath):
    """Loads a single tab JSON and extracts row data."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None
    
    # Extract flat fields
    row = {
        "id": data.get("id"),
        "song_name": data.get("song_name"),
        "artist_name": data.get("artist_name"),
        "type": data.get("type"),
        "version": data.get("version"),
        "votes": data.get("votes", 0),
        "rating": data.get("rating", 0.0),
        "difficulty": data.get("difficulty"),
        "tuning": data.get("tuning"),
        "capo": data.get("capo"),
        "url_web": data.get("urlWeb"),
    }
    
    # Process date
    timestamp = data.get("date")
    upload_date = ""
    upload_year = None
    if timestamp:
        try:
            date_obj = datetime.fromtimestamp(int(timestamp))
            upload_date = date_obj.strftime("%Y-%m-%d")
            upload_year = date_obj.year
        except:
            pass
            
    row["upload_date"] = upload_date
    row["upload_year"] = upload_year
    row["genre"] = data.get("advertising", {}).get("targeting_song_genre", "country")
    
    content = data.get("content", "")
    strumming = data.get("strumming", [])
    
    # 1. Main Key
    row["main_key"] = data.get("tonality_name", "")
    
    # 2. BPM from strumming
    bpm = None
    if strumming and isinstance(strumming, list):
        for s in strumming:
            part_bpm = s.get("bpm")
            if part_bpm and not bpm:
                bpm = part_bpm
                break
    row["bpm"] = bpm
    
    # 3. Song Structure Header
    content_parts = re.split(r'\[(?!ch|/ch|tab|/tab)([^\]]+)\]', content)
    sections_list = []
    if len(content_parts) > 1:
        for i in range(1, len(content_parts), 2):
            sections_list.append(content_parts[i].strip())
    row["song_structure"] = ", ".join(sections_list)
    
    # --- STRUCTURE FLAGS ---
    sections_lower = [s.lower() for s in sections_list]
    def check_presence(keywords):
         return 1 if any(any(k in s for k in keywords) for s in sections_lower) else 0
         
    row["has_intro"] = check_presence(["intro"])
    row["has_verse"] = check_presence(["verse"])
    row["has_chorus"] = check_presence(["chorus"])
    row["has_bridge"] = check_presence(["bridge"])
    row["has_outro"] = check_presence(["outro"])
    
    # 4. Chord Counts
    chord_matches = re.findall(r'\[ch\](.*?)\[/ch\]', content)
    chord_counts = Counter(chord_matches)
    
    sorted_chords = chord_counts.most_common(3)
    row["chord_1"] = sorted_chords[0][0] if len(sorted_chords) > 0 else ""
    row["chord_1_count"] = sorted_chords[0][1] if len(sorted_chords) > 0 else 0
    row["chord_2"] = sorted_chords[1][0] if len(sorted_chords) > 1 else ""
    row["chord_2_count"] = sorted_chords[1][1] if len(sorted_chords) > 1 else 0
    row["chord_3"] = sorted_chords[2][0] if len(sorted_chords) > 2 else ""
    row["chord_3_count"] = sorted_chords[2][1] if len(sorted_chords) > 2 else 0
    
    # --- SYNTHETIC INDICES ---
    indices = calculate_indices(data)
    row.update(indices)

    return row

def merge_versions_aggregated(group):
    """Aggregates multiple versions (top rating priority)."""
    if len(group) == 1:
        return group.iloc[0]
        
    # Sort by votes then rating
    g_sorted = group.sort_values(by=['votes', 'rating'], ascending=False)
    top_5 = g_sorted.head(5)
    
    synthetic_indices = [
        'complexity', 'repetition', 'melodicness', 'energy', 
        'finger_movement', 'disruption', 'root_stability', 
        'intra_root_variation', 'harmonic_palette', 'loop_strength', 
        'structure_variation', 'playability', 'harmonic_softness'
    ]
    
    res_row = {}
    for col in top_5.columns:
        if col in ['song_name', 'artist_name']:
             continue
        
        if col in synthetic_indices:
             if pd.api.types.is_numeric_dtype(top_5[col]):
                  res_row[col] = top_5[col].mean()
             else:
                  res_row[col] = top_5.iloc[0][col]
        else:
             # First non-missing
             chosen_val = ""
             for _, r in top_5.iterrows():
                  val = r[col]
                  if not pd.isna(val) and str(val).strip() != "" and str(val).lower() != "nan":
                       chosen_val = val
                       break
             res_row[col] = chosen_val
             
    return pd.Series(res_row)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if not os.path.exists(INPUT_DIR):
        print(f"Input directory {INPUT_DIR} not found.")
        return
        
    json_files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if f.endswith('.json')]
    print(f"Found {len(json_files)} JSON files in {INPUT_DIR}")
    
    rows = []
    for i, f in enumerate(json_files):
        row = flatten_tab_data(f)
        if row:
            rows.append(row)
        if (i+1) % 1000 == 0:
            print(f"Processed {i+1}/{len(json_files)} files...")
            
    if not rows:
        print("No data processed.")
        return
        
    df = pd.DataFrame(rows)
    print(f"Extracted {len(df)} observations. Aggregating versions...")
    
    # Ensure numeric for sorting
    df['votes'] = pd.to_numeric(df['votes'], errors='coerce').fillna(0)
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0.0)
    
    df_best = df.groupby(['song_name', 'artist_name'], group_keys=False).apply(merge_versions_aggregated).reset_index()
    print(f"Final dataset has {len(df_best)} unique songs.")
    
    # Save CSV
    csv_path = os.path.join(OUTPUT_DIR, f"{OUTPUT_FILENAME}.csv")
    df_best.to_csv(csv_path, index=False)
    print(f"Saved CSV: {csv_path}")
    
    # Save Stata DTA
    dta_path = os.path.join(OUTPUT_DIR, f"{OUTPUT_FILENAME}.dta")
    try:
        # Clean for Stata (fix string lengths and nulls)
        for col in df_best.columns:
            if not pd.api.types.is_numeric_dtype(df_best[col]):
                df_best[col] = df_best[col].fillna('').astype(str).apply(lambda x: x[:244]) # Stata 13 limit
        
        df_best.to_stata(dta_path, write_index=False, version=118)
        print(f"Saved Stata DTA: {dta_path}")
    except Exception as e:
        print(f"Error saving DTA: {e}")

if __name__ == "__main__":
    main()
