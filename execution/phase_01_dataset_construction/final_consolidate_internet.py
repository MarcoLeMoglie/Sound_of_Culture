import pandas as pd
import json
import os
import re
import difflib

def is_valid_year(year):
    try:
        y = float(year)
        # Permissible range for songs in this dataset
        return not pd.isna(y) and 1900 <= y <= 2030
    except (ValueError, TypeError):
        return False

def super_normalize(text):
    if not isinstance(text, str): return ""
    # Standard cleaning: lowercase, strip, remove junk words
    text = text.lower().strip()
    for word in ["chords", "tab", "acoustic", "live", "remix", "cover", "official", "ver", "version"]:
        text = text.replace(word, "")
    # Remove all non-alphanumeric characters for a tight comparison key
    return re.sub(r'[^a-zA-Z0-9]', '', text)

def main():
    base_dir = "data/phase_01_dataset_construction/processed/country_artists"
    input_csv = os.path.join(base_dir, "Sound_of_Culture_Country_Full_Enriched_v3.csv")
    output_v5_csv = os.path.join(base_dir, "Sound_of_Culture_Country_Full_Enriched_v5.csv")
    output_v5_dta = os.path.join(base_dir, "Sound_of_Culture_Country_Full_Enriched_v5.dta")

    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} not found.")
        return

    print(f"Loading {input_csv}...")
    df = pd.read_csv(input_csv, encoding='latin-1')

    initial_missing = df['release_year'].isna().sum()
    print(f"Initial missing release years: {initial_missing}")

    # Aggregazione di tutte le cache generate nelle varie fasi
    cache_files = [
        "release_years_cache.json",           # MusicBrainz Phase 1
        "release_years_cache_wiki.json",      # Wikipedia Phase 3
        "release_years_cache_discogs.json",   # Discogs Phase 3
        "release_years_cache_fuzzy.json",     # MusicBrainz Fuzzy Phase 2
        "release_years_cache_internet.json"    # Web Batch Phase 4 (Current)
    ]

    master_cache = {}
    for cf in cache_files:
        path = os.path.join(base_dir, cf)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                    # Filtriamo solo anni validi per evitare l'iniezione di null/stringhe d'errore
                    valid_years = {k: v for k, v in data.items() if is_valid_year(v)}
                    master_cache.update(valid_years)
                    print(f"Loaded {len(valid_years)} valid years from {cf}")
            except Exception as e:
                print(f"Error loading {cf}: {e}")

    # Preparazione mappature per normalizzazione e fuzzy search
    norm_cache = {} # NormalizedKey -> Year
    artist_cache_map = {} # NormalizedArtist -> List of full keys

    for k, v in master_cache.items():
        nk = super_normalize(k)
        if nk:
            norm_cache[nk] = v
        
        # Mappatura per fuzzy matching circoscritto allo stesso artista
        if " - " in k:
            artist_part = k.split(" - ")[0]
            na = super_normalize(artist_part)
            if na not in artist_cache_map:
                artist_cache_map[na] = []
            artist_cache_map[na].append(k)

    print(f"Combined Master Cache ready with {len(master_cache)} unique entries.")
    
    count_exact = 0
    count_norm = 0
    count_fuzzy = 0

    for idx, row in df.iterrows():
        # Saltiamo se l'anno è già presente
        if not pd.isna(row['release_year']):
            continue

        artist = str(row['artist_name'])
        song = str(row['song_name'])
        key = f"{artist} - {song}"
        
        # LIVELLO 1: MATCH ESATTO
        if key in master_cache:
            df.at[idx, 'release_year'] = master_cache[key]
            count_exact += 1
            continue

        # LIVELLO 2: SUPER-NORMALIZZAZIONE (cattura Dan + Shay vs Dan & Shay)
        nk = super_normalize(key)
        if nk in norm_cache:
            df.at[idx, 'release_year'] = norm_cache[nk]
            count_norm += 1
            continue

        # LIVELLO 3: FUZZY MATCHING (Soglia 90% + Vincolo sullo stesso artista)
        na = super_normalize(artist)
        if na in artist_cache_map:
            # Ricerchiamo solo tra le canzoni dello STESSO artista normalizzato
            possible_keys = artist_cache_map[na]
            matches = difflib.get_close_matches(key, possible_keys, n=1, cutoff=0.90)
            if matches:
                match_key = matches[0]
                df.at[idx, 'release_year'] = master_cache[match_key]
                count_fuzzy += 1

    print(f"Successfully matched: {count_exact} exact, {count_norm} normalized, {count_fuzzy} fuzzy.")
    final_missing = df['release_year'].isna().sum()
    print(f"Final missing release years: {final_missing}")

    # Pulizia finale per esportazione Stata
    df['release_year'] = pd.to_numeric(df['release_year'], errors='coerce')
    
    # Troncamento stringhe a 244 caratteri per compatibilità DTA
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.slice(0, 244)

    df.to_csv(output_v5_csv, index=False)
    df.to_stata(output_v5_dta, write_index=False, version=118)
    print(f"Saved {output_v5_csv} and {output_v5_dta}")

if __name__ == "__main__":
    main()
