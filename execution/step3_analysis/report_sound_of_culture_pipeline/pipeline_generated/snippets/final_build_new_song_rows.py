def build_new_song_rows() -> pd.DataFrame:
    discovery_rows = load_discovery()
    v6_df = pd.read_csv(V6_CSV, low_memory=False)
    artist_df = load_artist_universe()
    output_columns = build_output_columns(list(v6_df.columns))
    by_id, by_name = build_artist_metadata_maps(v6_df, artist_df)
    existing_keys = {canonical_song_key(row.artist_name, row.song_name) for row in v6_df.itertuples()}

    grouped = defaultdict(list)
    for row in discovery_rows:
        key = canonical_song_key(row.get("target_artist_name") or row.get("artist_name"), row.get("song_name"))
        grouped[key].append(row)

    new_rows = []
    skipped_existing = 0
    missing_raw = 0
    for idx, (key, versions) in enumerate(grouped.items(), start=1):
        if key in existing_keys:
            skipped_existing += 1
            continue
        try:
            base_row, payload, num_json_used = build_enriched_payload(versions)
        except FileNotFoundError:
            missing_raw += 1
            continue
        song_row = flatten_chord_payload(base_row, payload, num_json_used)
        full_row = attach_artist_metadata(song_row, base_row, by_id, by_name, output_columns)
        new_rows.append(full_row)
        if idx % 5000 == 0 or idx == len(grouped):
            log(f"Processed song groups: {idx}/{len(grouped)}; new rows accumulated: {len(new_rows)}")

    new_df = pd.DataFrame(new_rows, columns=output_columns)
    pre_dedup_count = len(new_df)
    if not new_df.empty:
        new_df["_artist_norm"] = new_df["artist_name"].map(norm_text)
        new_df["_song_norm"] = new_df["song_name"].map(norm_text)
        new_df.sort_values(["rating", "votes", "id"], ascending=[False, False, False], inplace=True)
        new_df.drop_duplicates(subset=["id"], keep="first", inplace=True)
        new_df.drop_duplicates(subset=["_artist_norm", "_song_norm"], keep="first", inplace=True)
        new_df.drop(columns=["_artist_norm", "_song_norm"], inplace=True)
        new_df = new_df.reindex(columns=output_columns)

    log(f"New song groups discovered: {len(grouped)}")
    log(f"Skipped because already in v6: {skipped_existing}")
    log(f"Skipped because no raw JSON survived: {missing_raw}")
    log(f"Deduplicated new song rows removed: {pre_dedup_count - len(new_df)}")
    log(f"New songs ready to append: {len(new_df)}")
    new_df.to_csv(NEW_SONGS_CSV, index=False)
    return new_df
