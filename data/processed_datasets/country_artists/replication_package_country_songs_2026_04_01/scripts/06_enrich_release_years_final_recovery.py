import pandas as pd
import json
import os
import sys

# Paths relative to replication root (script is in scripts/)
script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(script_dir, "../output/Sound_of_Culture_Country_Full_Enriched_v2.csv") # Output from Discogs
OUTPUT_CSV = os.path.join(script_dir, "../output/Sound_of_Culture_Country_Full_Final.csv")
CACHE_FILE = os.path.join(script_dir, "../data/release_years_cache_internet.json")

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except:
             return {}
    return {}

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Input file {INPUT_CSV} not found.")
        return

    df = pd.read_csv(INPUT_CSV)
    cache = load_cache()
    
    # Target missing years (NaN or 0)
    df['release_year'] = pd.to_numeric(df.get('release_year'), errors='coerce')
    missing_mask = df['release_year'].isna() | (df['release_year'] == 0)
    
    print(f"Starting Phase 4 Recovery (Internet/Manual Cache) for {missing_mask.sum()} missing songs...")

    def find_year(row):
        if pd.isna(row['release_year']) or row['release_year'] == 0:
            key = f"{row['artist_name']} - {row['song_name']}"
            val = cache.get(key)
            if val and val != "ERROR_RETRY":
                 return val
        return row['release_year']

    df['release_year'] = df.apply(find_year, axis=1)
    
    # Final cleanup: ensure numeric
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
    
    # Save the final consolidated dataset
    df.to_csv(OUTPUT_CSV, index=False)
    
    # Save final Stata DTA
    dta_path = OUTPUT_CSV.replace(".csv", ".dta")
    try:
        df_stata = df.copy()
        for col in df_stata.columns:
            if not pd.api.types.is_numeric_dtype(df_stata[col]):
                df_stata[col] = df_stata[col].fillna('').astype(str).apply(lambda x: x[:244])
        df_stata.to_stata(dta_path, write_index=False, version=118)
    except Exception as e:
         print(f"Error saving DTA: {e}")

    print(f"Final Enrichment complete. Saved to {OUTPUT_CSV} and {dta_path}")

if __name__ == "__main__":
    main()
