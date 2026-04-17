def enrich_release_years(df: pd.DataFrame) -> pd.DataFrame:
    cache_only = os.environ.get("SOC_RELEASE_YEAR_CACHE_ONLY") == "1"
    dedicated_cache = load_json(RELEASE_CACHE_FILE)
    artist_track_cache = load_json(ARTIST_RELEASE_TRACK_CACHE_FILE)
    master_cache = build_master_release_cache(pd.read_csv(V6_CSV, low_memory=False))
    normalized_master = {normalize_key(key): value for key, value in master_cache.items()}

    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    df["_artist_norm"] = df["artist_name"].map(normalize_space)
    df["_song_norm"] = df["song_name"].map(norm_text)

    manual_fills = 0
    for idx in df.index[df["release_year"].isna()]:
        key = (normalize_space(df.at[idx, "artist_name"]), normalize_space(df.at[idx, "song_name"]))
        year = WEB_CONFIRMED_RELEASE_YEAR_OVERRIDES.get(key)
        if is_valid_year(year):
            df.at[idx, "release_year"] = int(year)
            dedicated_cache[canonical_release_key(*key)] = int(year)
            manual_fills += 1

    cache_fills = 0
    for idx in df.index[df["release_year"].isna()]:
        key = canonical_release_key(df.at[idx, "artist_name"], df.at[idx, "song_name"])
        if key in master_cache:
            df.at[idx, "release_year"] = master_cache[key]
            cache_fills += 1
            continue
        nkey = normalize_key(key)
        if nkey in normalized_master:
            df.at[idx, "release_year"] = normalized_master[nkey]
            cache_fills += 1
            continue
        cached_value = dedicated_cache.get(key)
        if is_valid_year(cached_value):
            df.at[idx, "release_year"] = int(float(cached_value))
            cache_fills += 1
    log(f"Rows filled from historical release caches: {cache_fills}")
    if manual_fills:
        log(f"Rows filled from web-confirmed manual release overrides: {manual_fills}")

    remaining = df[df["release_year"].isna()].copy()
    if remaining.empty or cache_only:
        save_release_caches(dedicated_cache, artist_track_cache)
        if cache_only:
            log(f"Cache-only release-year mode enabled; rows still missing: {int(df['release_year'].isna().sum())}")
        df.drop(columns=["_artist_norm", "_song_norm"], inplace=True, errors="ignore")
        return df

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    artist_order = (
        remaining.groupby("_artist_norm")
        .size()
        .sort_values(ascending=False)
        .index.tolist()
    )
    processed_artists = 0
    for artist_name in artist_order:
        group = remaining[remaining["_artist_norm"] == artist_name]
        songs_by_norm = {norm_text(song): song for song in group["song_name"].tolist()}
        artist_mbids = group.get("musicbrainz_mbid", pd.Series(dtype="object")).dropna().astype(str).str.strip()
        artist_mbids = artist_mbids[artist_mbids.ne("")]
        artist_mbid = artist_mbids.iloc[0] if not artist_mbids.empty else ""
        artist_missing_start = int(df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)]["song_name"].nunique())
        log(f"Release-year artist start: {artist_name} with {artist_missing_start} missing songs")
        artist_query_count = 0

        artist_cache_key = artist_mbid or artist_name
        cached_artist_tracks = artist_track_cache.get(artist_cache_key, {})
        if isinstance(cached_artist_tracks, dict):
            for song_norm, year in cached_artist_tracks.items():
                if song_norm not in songs_by_norm or not is_valid_year(year):
                    continue
                mask = df["release_year"].isna() & df["_artist_norm"].eq(artist_name) & df["_song_norm"].eq(song_norm)
                df.loc[mask, "release_year"] = int(year)
                dedicated_cache[canonical_release_key(artist_name, songs_by_norm[song_norm])] = int(year)

        itunes_found = year_from_artist_itunes(session, artist_name, songs_by_norm)
        for song_norm, year in itunes_found.items():
            mask = df["release_year"].isna() & df["_artist_norm"].eq(artist_name) & df["_song_norm"].eq(song_norm)
            df.loc[mask, "release_year"] = year
            song_title = songs_by_norm[song_norm]
            dedicated_cache[canonical_release_key(artist_name, song_title)] = year

        mb_artist_candidates = df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)]
        if not mb_artist_candidates.empty:
            mb_artist_found = year_from_artist_musicbrainz(session, artist_name, songs_by_norm, artist_mbid=artist_mbid)
            for song_norm, year in mb_artist_found.items():
                mask = df["release_year"].isna() & df["_artist_norm"].eq(artist_name) & df["_song_norm"].eq(song_norm)
                df.loc[mask, "release_year"] = year
                song_title = songs_by_norm[song_norm]
                dedicated_cache[canonical_release_key(artist_name, song_title)] = year
            if mb_artist_found:
                artist_track_cache.setdefault(artist_cache_key, {}).update({k: int(v) for k, v in mb_artist_found.items() if is_valid_year(v)})

        mb_release_candidates = df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)]
        if not mb_release_candidates.empty:
            mb_release_found = year_from_artist_musicbrainz_releases(session, artist_name, songs_by_norm, artist_mbid=artist_mbid)
            for song_norm, year in mb_release_found.items():
                mask = df["release_year"].isna() & df["_artist_norm"].eq(artist_name) & df["_song_norm"].eq(song_norm)
                df.loc[mask, "release_year"] = year
                song_title = songs_by_norm[song_norm]
                dedicated_cache[canonical_release_key(artist_name, song_title)] = year
            if mb_release_found:
                artist_track_cache.setdefault(artist_cache_key, {}).update({k: int(v) for k, v in mb_release_found.items() if is_valid_year(v)})

        deezer_candidates = df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)]
        if not deezer_candidates.empty:
            deezer_found = year_from_artist_deezer(session, artist_name, songs_by_norm)
            for song_norm, year in deezer_found.items():
                mask = df["release_year"].isna() & df["_artist_norm"].eq(artist_name) & df["_song_norm"].eq(song_norm)
                df.loc[mask, "release_year"] = year
                song_title = songs_by_norm[song_norm]
                dedicated_cache[canonical_release_key(artist_name, song_title)] = year

        still_missing = df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)][["artist_name", "song_name"]].drop_duplicates()
        still_missing_norms = {norm_text(song): song for song in still_missing["song_name"].tolist()}
        for row in still_missing.itertuples(index=False):
            artist_query_count += 1
            bundle_found = year_from_song_itunes_bundle(session, row.artist_name, row.song_name, still_missing_norms)
            if not bundle_found:
                if artist_query_count % 50 == 0:
                    save_release_caches(dedicated_cache, artist_track_cache)
                    missing_now = int(df["release_year"].isna().sum())
                    log(f"Release-year artist progress: {artist_name}; song queries {artist_query_count}; missing rows now: {missing_now}")
                continue
            for song_norm, year in bundle_found.items():
                mask = df["release_year"].isna() & df["_artist_norm"].eq(artist_name) & df["_song_norm"].eq(song_norm)
                df.loc[mask, "release_year"] = year
                song_title = still_missing_norms[song_norm]
                dedicated_cache[canonical_release_key(artist_name, song_title)] = year
            if artist_query_count % 25 == 0:
                save_release_caches(dedicated_cache, artist_track_cache)
                missing_now = int(df["release_year"].isna().sum())
                log(f"Release-year artist progress: {artist_name}; song queries {artist_query_count}; missing rows now: {missing_now}")
            still_missing = df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)][["artist_name", "song_name"]].drop_duplicates()
            still_missing_norms = {norm_text(song): song for song in still_missing["song_name"].tolist()}
            if still_missing.empty:
                break

        still_missing = df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)][["artist_name", "song_name"]].drop_duplicates()
        for row in still_missing.itertuples(index=False):
            artist_query_count += 1
            key = canonical_release_key(row.artist_name, row.song_name)
            year = year_from_song_discogs(session, row.artist_name, row.song_name)
            if year is None:
                year = year_from_song_musicbrainz(session, row.artist_name, row.song_name, artist_mbid=artist_mbid)
            if year is None:
                year = year_from_song_wikidata(session, row.artist_name, row.song_name)
            if year is None:
                year = year_from_song_wikipedia(session, row.artist_name, row.song_name)
            dedicated_cache[key] = year
            if is_valid_year(year):
                mask = df["release_year"].isna() & df["_artist_norm"].eq(normalize_space(row.artist_name)) & df["_song_norm"].eq(norm_text(row.song_name))
                df.loc[mask, "release_year"] = int(year)
            if artist_query_count % 25 == 0:
                save_release_caches(dedicated_cache, artist_track_cache)
                missing_now = int(df["release_year"].isna().sum())
                log(f"Release-year artist progress: {artist_name}; song queries {artist_query_count}; missing rows now: {missing_now}")

        processed_artists += 1
        artist_missing_end = int(df[df["release_year"].isna() & df["_artist_norm"].eq(artist_name)]["song_name"].nunique())
        log(f"Release-year artist done: {artist_name}; remaining missing songs for artist: {artist_missing_end}")
        if processed_artists % 10 == 0 or processed_artists == len(artist_order):
            save_release_caches(dedicated_cache, artist_track_cache)
            missing_now = int(df["release_year"].isna().sum())
            log(f"Release-year enrichment artists processed: {processed_artists}/{len(artist_order)}; missing rows now: {missing_now}")

    save_release_caches(dedicated_cache, artist_track_cache)
    df.drop(columns=["_artist_norm", "_song_norm"], inplace=True, errors="ignore")
    return df
