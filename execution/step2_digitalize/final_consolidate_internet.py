import pandas as pd
import json
import os
import re

# Paths
base_dir = "data/processed_datasets/country_artists"
input_csv = os.path.join(base_dir, "Sound_of_Culture_Country_Full_Enriched_v3.csv")
cache_internet = os.path.join(base_dir, "release_years_cache_internet.json")
output_csv = os.path.join(base_dir, "Sound_of_Culture_Country_Full_Enriched_v5.csv")
output_dta = os.path.join(base_dir, "Sound_of_Culture_Country_Full_Enriched_v5.dta")

def normalize(text):
    if not isinstance(text, str):
        return ""
    # Convert to lowercase
    text = text.lower()
    # Remove everything except alphanumeric
    text = re.sub(r'[^a-zA-Z0-9]', '', text)
    return text

def main():
    print(f"Loading {input_csv}...")
    df = pd.read_csv(input_csv, encoding='latin-1')
    
    print(f"Initial missing release years: {df['release_year'].isna().sum()}")
    
    if os.path.exists(cache_internet):
        with open(cache_internet, 'r') as f:
            cache = json.load(f)
        print(f"Loaded {len(cache)} entries from cache.")
        
        # Create a normalized version of the cache
        norm_cache = {}
        for key, year in cache.items():
            norm_key = normalize(key)
            if norm_key:
                norm_cache[norm_key] = year
        
        # Merge logic
        count = 0
        missing_mask = df['release_year'].isna()
        for idx, row in df[missing_mask].iterrows():
            # Try exact match first
            key = f"{row['artist_name']} - {row['song_name']}"
            if key in cache:
                df.at[idx, 'release_year'] = cache[key]
                count += 1
                continue
            
            # Try normalized match
            norm_key = normalize(key)
            if norm_key in norm_cache:
                df.at[idx, 'release_year'] = norm_cache[norm_key]
                count += 1
                continue
            
            # Special case for Dixie Chicks / The Chicks
            if "dixie chicks" in row['artist_name'].lower():
                alt_key = normalize(f"The Chicks - {row['song_name']}")
                if alt_key in norm_cache:
                    df.at[idx, 'release_year'] = norm_cache[alt_key]
                    count += 1
                    continue
        
        print(f"Inserted {count} new years from internet cache (with normalization).")
    
    print(f"Final missing release years: {df['release_year'].isna().sum()}")
    
    # Save CSV
    df.to_csv(output_csv, index=False)
    print(f"Saved {output_csv}")
    
    # Save Stata DTA
    try:
        # Final cleanup for Stata
        df_stata = df.copy()
        for col in df_stata.select_dtypes(include=['object']).columns:
             df_stata[col] = df_stata[col].astype(str).str[:244]
        df_stata.to_stata(output_dta, write_index=False, version=118)
        print(f"Saved {output_dta}")
    except Exception as e:
        print(f"Error saving DTA: {e}")

if __name__ == "__main__":
    main()
