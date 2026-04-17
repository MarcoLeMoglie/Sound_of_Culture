def build_chart_cache() -> pd.DataFrame:
    if CHART_CACHE_CSV.exists():
        df = pd.read_csv(CHART_CACHE_CSV)
        if not df.empty and int(df["chart_year"].max()) >= 2025:
            return augment_chart_cache_with_year_end_supplement(df)
        log("Existing Billboard chart cache is incomplete; rebuilding it")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    all_rows = []
    for year in range(1946, 2026):
        rows = scrape_chart_rows_for_year(session, year)
        if rows:
            all_rows.extend(rows)
        if year % 10 == 0 or year == 2025:
            log(f"Billboard year-end chart scrape progress through {year}: {len(all_rows)} rows")
        time.sleep(0.1)

    chart_df = pd.DataFrame(all_rows)
    if chart_df.empty:
        raise RuntimeError("Could not build Billboard country year-end chart cache")

    chart_df = augment_chart_cache_with_year_end_supplement(chart_df)
    chart_df["song_key"] = chart_df["song_name"].map(norm_text)
    chart_df["artist_key"] = chart_df["artist_name"].map(norm_text)
    chart_df.sort_values(["artist_key", "song_key", "chart_year", "position"], inplace=True)
    chart_df.to_csv(CHART_CACHE_CSV, index=False)
    chart_df.to_json(CHART_CACHE_JSON, orient="records", force_ascii=False, indent=2)
    return chart_df


def augment_chart_cache_with_year_end_supplement(chart_df: pd.DataFrame) -> pd.DataFrame:
    if not SUPPLEMENTAL_CHART_CSV.exists():
        return chart_df
    try:
        supplemental_df = pd.read_csv(SUPPLEMENTAL_CHART_CSV, low_memory=False)
    except Exception:
        return chart_df
    if supplemental_df.empty or "source_kind" not in supplemental_df.columns:
        return chart_df
    supplemental_df = supplemental_df[supplemental_df["source_kind"].eq("year_end_wikipedia")].copy()
    if supplemental_df.empty:
        return chart_df
    combined = pd.concat([chart_df, supplemental_df], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(subset=["chart_year", "position", "song_name", "artist_name"])
    return combined


def apply_chart_variables(df: pd.DataFrame, chart_df: pd.DataFrame) -> pd.DataFrame:
    chart_lookup = {}
    for row in chart_df.itertuples(index=False):
        candidate = (int(row.chart_year), int(row.position))
        keys = {
            canonical_song_key(row.artist_name, row.song_name),
            canonical_song_key(simplify_chart_artist(row.artist_name), clean_chart_song_title(row.song_name)),
        }
        for key in keys:
            current = chart_lookup.get(key)
            if current is None or candidate < current:
                chart_lookup[key] = candidate

    df["billboard_country_year_end_flag"] = 0
    df["billboard_country_year_end_year"] = pd.NA
    df["billboard_country_year_end_pos"] = pd.NA
    df["_artist_chart_key"] = df["artist_name"].map(norm_text)
    df["_song_chart_key"] = df["song_name"].map(norm_text)
    df["_artist_chart_key_simple"] = df["artist_name"].map(simplify_chart_artist).map(norm_text)
    df["_song_chart_key_clean"] = df["song_name"].map(clean_chart_song_title).map(norm_text)

    for idx in df.index:
        keys = [
            (df.at[idx, "_artist_chart_key"], df.at[idx, "_song_chart_key"]),
            (df.at[idx, "_artist_chart_key_simple"], df.at[idx, "_song_chart_key_clean"]),
        ]
        for key in keys:
            if key in chart_lookup:
                chart_year, position = chart_lookup[key]
                df.at[idx, "billboard_country_year_end_flag"] = 1
                df.at[idx, "billboard_country_year_end_year"] = chart_year
                df.at[idx, "billboard_country_year_end_pos"] = position
                break
    df.drop(columns=["_artist_chart_key", "_song_chart_key", "_artist_chart_key_simple", "_song_chart_key_clean"], inplace=True, errors="ignore")
    return df


def build_final_dataset() -> pd.DataFrame:
    base_df = pd.read_csv(V6_CSV, low_memory=False)
    output_columns = build_output_columns(list(base_df.columns))
    base_df = base_df.reindex(columns=output_columns)
    discovery_rows = load_discovery()
    if NEW_SONGS_CSV.exists():
        new_df = pd.read_csv(NEW_SONGS_CSV, low_memory=False)
        log(f"Loaded cached new-song rows from {NEW_SONGS_CSV.name}: {len(new_df)} rows")
        new_df = deduplicate_new_song_cache(new_df, output_columns)
        new_df.to_csv(NEW_SONGS_CSV, index=False)
    else:
        new_df = build_new_song_rows()
    existing_ids = set(pd.to_numeric(base_df["id"], errors="coerce").dropna().astype(int))
    before_overlap_drop = len(new_df)
    new_df = new_df[~pd.to_numeric(new_df["id"], errors="coerce").fillna(-1).astype(int).isin(existing_ids)].copy()
    overlap_removed = before_overlap_drop - len(new_df)
    if overlap_removed:
        log(f"Removed new-song rows whose UG id already exists in v6: {overlap_removed}")
    combined = pd.concat([base_df, new_df], ignore_index=True)
    artist_df = load_artist_universe()
    bridge_df = build_artist_bridge_rows(discovery_rows, combined, base_df, artist_df)
    if not bridge_df.empty:
        combined = pd.concat([combined, bridge_df], ignore_index=True)
    combined = apply_num_json_used(combined, discovery_rows)
    combined = apply_chart_variables(combined, build_chart_cache())
    combined = enrich_release_years(combined)
    combined.to_csv(OUTPUT_CSV, index=False)
    return combined

def build_final_dataset() -> pd.DataFrame:
    base_df = pd.read_csv(V6_CSV, low_memory=False)
    output_columns = build_output_columns(list(base_df.columns))
    base_df = base_df.reindex(columns=output_columns)
    discovery_rows = load_discovery()
    if NEW_SONGS_CSV.exists():
        new_df = pd.read_csv(NEW_SONGS_CSV, low_memory=False)
        log(f"Loaded cached new-song rows from {NEW_SONGS_CSV.name}: {len(new_df)} rows")
        new_df = deduplicate_new_song_cache(new_df, output_columns)
        new_df.to_csv(NEW_SONGS_CSV, index=False)
    else:
        new_df = build_new_song_rows()
    existing_ids = set(pd.to_numeric(base_df["id"], errors="coerce").dropna().astype(int))
    before_overlap_drop = len(new_df)
    new_df = new_df[~pd.to_numeric(new_df["id"], errors="coerce").fillna(-1).astype(int).isin(existing_ids)].copy()
    overlap_removed = before_overlap_drop - len(new_df)
    if overlap_removed:
        log(f"Removed new-song rows whose UG id already exists in v6: {overlap_removed}")
    combined = pd.concat([base_df, new_df], ignore_index=True)
    artist_df = load_artist_universe()
    bridge_df = build_artist_bridge_rows(discovery_rows, combined, base_df, artist_df)
    if not bridge_df.empty:
        combined = pd.concat([combined, bridge_df], ignore_index=True)
    combined = apply_num_json_used(combined, discovery_rows)
    combined = apply_chart_variables(combined, build_chart_cache())
    combined = enrich_release_years(combined)
    combined.to_csv(OUTPUT_CSV, index=False)
    return combined
