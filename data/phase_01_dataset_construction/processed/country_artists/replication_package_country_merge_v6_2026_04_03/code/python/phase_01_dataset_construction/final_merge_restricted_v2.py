import pandas as pd
import os
import json

def main():
    base_dir = "data/processed_datasets/country_artists"
    songs_csv = os.path.join(base_dir, "Sound_of_Culture_Country_Full_Enriched_v5.csv")
    artists_csv = os.path.join(base_dir, "country_artists_restricted.csv")
    dictionary_csv = os.path.join(base_dir, "country_artists_data_dictionary.csv")
    
    output_csv = os.path.join(base_dir, "Sound_of_Culture_Country_Restricted_Final_v6.csv")
    output_dta = os.path.join(base_dir, "Sound_of_Culture_Country_Restricted_Final_v6.dta")

    print("Loading datasets...")
    df_songs = pd.read_csv(songs_csv, encoding='latin-1')
    df_artists = pd.read_csv(artists_csv)
    df_dict = pd.read_csv(dictionary_csv)

    print(f"Initial songs: {len(df_songs)}")

    # 1. Perform Left Join
    # artist_name (songs) <-> name_primary (artists)
    merged_df = pd.merge(
        df_songs, 
        df_artists, 
        left_on='artist_name', 
        right_on='name_primary', 
        how='left'
    )

    print(f"Merged songs (Left Join): {len(merged_df)}")
    if len(merged_df) != len(df_songs):
        print("Warning: Row count changed during merge! Check for duplicates in artist file.")

    # 2. Variable Labeling Logic
    # Start with labels from the data dictionary
    variable_labels = {}
    for _, row in df_dict.iterrows():
        col = row['column_name']
        desc = row['description']
        if pd.notna(col) and pd.notna(desc):
            # Stata labels are capped at 80 chars
            variable_labels[col] = str(desc)[:80]

    # Add labels for Song-Level Metrics from v5
    song_metrics_labels = {
        "song_name": "Song Title",
        "artist_name": "Artist Name (Primary)",
        "id": "Ultimate Guitar Tab ID",
        "type": "Tab Type (Chords/Bass)",
        "version": "Tab Version Number",
        "votes": "Number of UG votes",
        "rating": "Average UG Rating (1-5)",
        "difficulty": "Manual Difficulty Level (UG)",
        "tuning": "Instrument Tuning",
        "capo": "Capo Position",
        "url_web": "Ultimate Guitar URL",
        "upload_date": "Date of Tab Upload",
        "upload_year": "Year of Tab Upload",
        "genre": "Genre tag from UG",
        "main_key": "Estimated Musical Key",
        "bpm": "Estimated Tempo (BPM)",
        "song_structure": "Structural blocks (Verse, Chorus, etc)",
        "has_intro": "Binary: Contains Intro section",
        "has_verse": "Binary: Contains Verse section",
        "has_chorus": "Binary: Contains Chorus section",
        "has_bridge": "Binary: Contains Bridge section",
        "has_outro": "Binary: Contains Outro section",
        "complexity": "Chord Complexity Index (Extensions ratio + Unique ratio)",
        "repetition": "Chord Repetition Index (Inverse of Unique ratio)",
        "melodicness": "Stable Consonances Ratio (Ratio of soft/major chords)",
        "energy": "Density of Chord Switches and Difficulty",
        "finger_movement": "Distance between chord fingerings",
        "disruption": "Abrupt harmonic changes index",
        "root_stability": "Consecutive Same Root Count",
        "intra_root_variation": "Variations of the same root",
        "harmonic_palette": "Richness of unique chords over total expected",
        "loop_strength": "Repeating 4-chord sublists strength",
        "structure_variation": "Variation in song section structure",
        "playability": "Ease of playing with standard shapes",
        "harmonic_softness": "Ratio of soft/major chords",
        "release_year": "Song Release Year (Consolidated Phase 4 Recovery)"
    }
    
    # Update master labels with song metrics
    variable_labels.update(song_metrics_labels)

    # 3. Clean for Stata compatibility
    # Ensure release_year is numeric
    merged_df['release_year'] = pd.to_numeric(merged_df['release_year'], errors='coerce')
    
    # Truncate string columns to 244 chars (Stata limit)
    for col in merged_df.select_dtypes(include=['object']).columns:
        merged_df[col] = merged_df[col].astype(str).str.slice(0, 244)
        # Handle nan strings
        merged_df.loc[merged_df[col] == 'nan', col] = ""

    # Sort columns for tidier DTA (Song info first, then Artist info)
    song_cols = list(df_songs.columns)
    artist_cols = [c for c in merged_df.columns if c not in song_cols]
    final_order = song_cols + artist_cols
    merged_df = merged_df[final_order]

    # Save CSV
    merged_df.to_csv(output_csv, index=False)
    print(f"Saved CSV: {output_csv}")

    # Save DTA with labels
    # Use to_stata directly from pandas, passing variable_labels
    # We must filter variable_labels to only include columns present in merged_df
    final_labels = {k: v for k, v in variable_labels.items() if k in merged_df.columns}
    
    try:
        merged_df.to_stata(
            output_dta, 
            write_index=False, 
            version=118, 
            variable_labels=final_labels
        )
        print(f"Saved DTA: {output_dta} (with {len(final_labels)} labels)")
    except Exception as e:
        print(f"Error saving DTA Labeling: {e}. Trying fallback without labels.")
        merged_df.to_stata(output_dta, write_index=False, version=118)

if __name__ == "__main__":
    main()
