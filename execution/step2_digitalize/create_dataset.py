import json
import os
import pandas as pd
from datetime import datetime

INSTRUMENT = "Chords" # Default
OUTPUT_DIR = "data/processed_datasets/top100YearEnd8525"
OUTPUT_NAME_MAP = {
    "Chords": "dataset_chords_top100yearend",
    "Bass": "dataset_bass_top100yearend",
}

TYPE_MAPPING = {
    "Chords": "Chords",
    "Bass": "Bass Tabs"
}

def flatten_tab_data(filepath):
    """Loads a single tab JSON and extracts row data."""
    with open(filepath, 'r') as f:
        data = json.load(f)
    
    # --- STRICT FILTER ---
    if data.get("type") != TYPE_MAPPING.get(INSTRUMENT, INSTRUMENT):
        return None # Skip other instruments


        
    # Extract flat fields
    row = {
        "id": data.get("id"),
        "song_name": data.get("song_name"),
        "artist_name": data.get("artist_name"),
        "type": data.get("type"),
        "part": data.get("part"),
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
            # If conversion fails, keep default empty values
            pass
            
    row["upload_date"] = upload_date
    row["upload_year"] = upload_year
    row["chart_year"] = data.get("chart_year", "")
    row["genre"] = data.get("advertising", {}).get("targeting_song_genre", "")

        
    # --- ENRICHMENT FIELDS ---
    import re
    from collections import Counter
    
    content = data.get("content", "")
    strumming = data.get("strumming", [])
    
    # 1. Main Key (Tonality)
    row["main_key"] = data.get("tonality_name", "")
    
    # 2. Detailed Strumming (Separated by Part)
    bpm = None
    if strumming and isinstance(strumming, list):
        for s in strumming:
            part = s.get("part", "Unknown").lower()
            clean_part = re.sub(r'[^a-zA-Z0-9]', '_', part)
            if not clean_part:
                 clean_part = "unknown"
                 
            part_bpm = s.get("bpm")
            if part_bpm and not bpm:
                bpm = part_bpm # Take first bpm
                
            measures = s.get("measures", [])
            measure_vals = [str(m.get("measure", "")) for m in measures]
            
            # Dynamic strumming variables
            row[f"strumming_{clean_part}_details"] = "-".join(measure_vals)
            
    row["bpm"] = bpm
    
    # 3. Song Structure Header
    # Split content by [Section] headers, ignoring [ch] and [tab] tags
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
    row["has_pre_chorus"] = check_presence(["pre-chorus", "pre chorus"])
    row["has_bridge"] = check_presence(["bridge"])
    row["has_outro"] = check_presence(["outro"])
    row["has_solo"] = check_presence(["solo"])
    row["has_interlude"] = check_presence(["interlude", "instrumental"])

    
    # 4. Chord Counts (Song Wide)
    chord_matches = re.findall(r'\[ch\](.*?)\[/ch\]', content)
    chord_counts = Counter(chord_matches)
    
    sorted_chords = chord_counts.most_common(3)
    row["chord_1"] = sorted_chords[0][0] if len(sorted_chords) > 0 else ""
    row["chord_1_count"] = sorted_chords[0][1] if len(sorted_chords) > 0 else 0
    row["chord_2"] = sorted_chords[1][0] if len(sorted_chords) > 1 else ""
    row["chord_2_count"] = sorted_chords[1][1] if len(sorted_chords) > 1 else 0
    row["chord_3"] = sorted_chords[2][0] if len(sorted_chords) > 2 else ""
    row["chord_3_count"] = sorted_chords[2][1] if len(sorted_chords) > 2 else 0
    
    chord_summary = ", ".join([f"{k}:{v}" for k, v in chord_counts.items()])
    row["all_chords_summary"] = chord_summary
            
    # --- SYNTHETIC INDICES ---
    if INSTRUMENT == "Bass":
         from music_indices_bass import calculate_bass_indices
         indices = calculate_bass_indices(data)
    else:
         from music_indices import calculate_indices
         indices = calculate_indices(data)
    row.update(indices)


    return row


def merge_versions_aggregated(group):
    """
    Aggregates multiple versions of a song into a single observation:
    1) Sort by Votes (Descending) and take top 5
    2) Sort top 5 by Rating (Descending)
    3) Numerical Columns: Mean()
    4) String/Other Columns: First non-missing value (ordered by rating)
    """
    if len(group) == 1:
        return group.iloc[0]
        
    # 1. Sort by votes, take top 5
    g_sorted = group.sort_values(by='votes', ascending=False)
    top_5 = g_sorted.head(5)
    
    # 2. Sort top 5 by rating descending
    top_5_rated = top_5.sort_values(by='rating', ascending=False)
    
    synthetic_indices = [
        'complexity', 'repetition', 'melodicness', 'energy', 
        'finger_movement', 'disruption', 'root_stability', 
        'intra_root_variation', 'harmonic_palette', 'loop_strength', 
        'structure_variation', 'playability', 'harmonic_softness'
    ]
    
    res_row = {}
    for col in top_5_rated.columns:
        if col in ['song_name', 'artist_name']:
             continue # Managed by groupby
        
        if col == 'id':
             # Retain ID of the highest-rated version for reference
             res_row[col] = top_5_rated.iloc[0][col]
             continue
             
        # RULE 1: Synthetic Indices -> Mean
        if col in synthetic_indices:
             if pd.api.types.is_numeric_dtype(top_5_rated[col]):
                  res_row[col] = top_5_rated[col].mean()
             else:
                  res_row[col] = top_5_rated.iloc[0][col] # fallback
        else:
             # RULE 2: Other Variables -> First Non-Missing (ordered by rating)
             chosen_val = ""
             for _, r in top_5_rated.iterrows():
                  val = r[col]
                  is_missing = False
                  if pd.isna(val) or val is None:
                      is_missing = True
                  elif isinstance(val, str) and (val.strip() == "" or val.lower() == "nan"):
                      is_missing = True
                  elif col in ['bpm', 'votes', 'rating'] and (val == 0 or val == 0.0):
                      is_missing = True
                      
                  if not is_missing:
                       chosen_val = val
                       break
             res_row[col] = chosen_val
             
    return pd.Series(res_row)


def main():
    import sys
    global INSTRUMENT
    
    instrument_arg = sys.argv[1] if len(sys.argv) > 1 else "Chords"
    INSTRUMENT = instrument_arg.capitalize()
    
    input_dir = f"data/raw_tabs_{INSTRUMENT.lower()}"
    output_dir = OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all JSON files
    if not os.path.exists(input_dir):
        print(f"Input directory {input_dir} not found.")
        return
        
    json_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.json')]
    print(f"[{INSTRUMENT}] Found {len(json_files)} JSON files in {input_dir}")

    
    rows = []
    for f in json_files:
        try:
            row = flatten_tab_data(f)
            if row:
                rows.append(row)
        except Exception as e:
            print(f"Error processing {f}: {e}")
            
    if not rows:
        print("No data to save.")
        return
        
    df = pd.DataFrame(rows)
    
    # --- BEST VERSION SELECTION (Advanced Aggregate Merge) ---
    if 'rating' in df.columns and 'votes' in df.columns:
        # Ensure correct types before sorting
        df['votes'] = pd.to_numeric(df['votes'], errors='coerce').fillna(0)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0.0)
        
        df_best = df.groupby(['song_name', 'artist_name'], group_keys=False).apply(merge_versions_aggregated).reset_index()
        print(f"Aggregated versions: {len(df_best)} groups from {len(df)} initial files.")
    else:
        df_best = df
        
    # Save CSV
    dataset_name = OUTPUT_NAME_MAP.get(INSTRUMENT, f"dataset_{INSTRUMENT.lower()}_top100yearend")
    csv_path = os.path.join(output_dir, f"{dataset_name}.csv")
    df_best.to_csv(csv_path, index=False)
    print(f"Saved CSV Dataset: {csv_path}")
    
    # Save Stata DTA
    dta_path = os.path.join(output_dir, f"{dataset_name}.dta")

    try:
        # Standard cleaning for Stata
        for col in df_best.columns:
            # Stata doesn't support mixed or pure null in string Columns.
            # Convert everything that isn't explicitly numeric to string
            if not pd.api.types.is_numeric_dtype(df_best[col]):
                df_best[col] = df_best[col].fillna('').astype(str)
                
        df_best.to_stata(dta_path, write_index=False, version=118)
        print(f"Saved Stata DTA Dataset: {dta_path}")
    except Exception as e:
        print(f"Error saving Stata DTA: {e}")

    if INSTRUMENT == "Chords":
        alias_csv_path = os.path.join(output_dir, "dataset_top100yearend.csv")
        alias_dta_path = os.path.join(output_dir, "dataset_top100yearend.dta")
        df_best.to_csv(alias_csv_path, index=False)
        print(f"Saved CSV Alias Dataset: {alias_csv_path}")
        try:
            df_best.to_stata(alias_dta_path, write_index=False, version=118)
            print(f"Saved Stata DTA Alias Dataset: {alias_dta_path}")
        except Exception as e:
            print(f"Error saving Stata DTA alias: {e}")

if __name__ == "__main__":
    main()
