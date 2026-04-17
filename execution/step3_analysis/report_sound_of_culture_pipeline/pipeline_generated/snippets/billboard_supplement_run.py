def run(
    max_pages_artist: int,
    max_pages_song: int,
    workers: int,
    min_delay: float,
    max_delay: float,
    skip_broad: bool,
    download_added_only: bool,
    target_csv: Path,
) -> None:
    universe_df = load_universe()
    artist_rows = build_artist_rows(universe_df)
    existing_rows = load_json_rows(DISCOVERY_JSON)
    existing_ids = {int(row["id"]) for row in existing_rows if row.get("id")}
    attempted_target_keys = load_progress_keys()

    added_artists_df = pd.read_csv(ADDED_ARTISTS_CSV, low_memory=False) if ADDED_ARTISTS_CSV.exists() else pd.DataFrame()
    targets_df = pd.read_csv(target_csv, low_memory=False)
    added_artist_ids = set(added_artists_df["artist_id"].fillna("").astype(str)) if not added_artists_df.empty else set()

    if download_added_only:
        rows = [row for row in existing_rows if str(row.get("artist_id", "")).strip() in added_artist_ids]
        log(f"Downloading raw JSONs for {len(rows)} discovery rows tied to added Billboard artists")
        base.download_rows(
            rows,
            RAW_CHORDS_DIR,
            workers=workers,
            min_delay=min_delay,
            max_delay=max_delay,
            shard_mod=None,
            shard_rem=None,
        )
        return

    supplement_rows: List[dict] = []
    existing_artist_ids = {str(row.get("artist_id", "")).strip() for row in existing_rows if str(row.get("artist_id", "")).strip()}

    if not added_artists_df.empty and not skip_broad:
        pending_added_artists_df = added_artists_df[
            ~added_artists_df["artist_id"].fillna("").astype(str).isin(existing_artist_ids)
        ].copy()
        log(f"Broad discovery for {len(pending_added_artists_df)} newly added artists still missing from discovery")
        client = UltimateGuitarClient()
        for idx, row in enumerate(pending_added_artists_df.to_dict(orient="records"), start=1):
            artist_id = str(row.get("artist_id", "")).strip()
            artist_row = artist_rows.get(artist_id)
            if not artist_row:
                continue
            discovered = base.discover_kind_for_artist(client, artist_row, "Chords", max_pages=max_pages_artist)
            artist_rows_flat = []
            for versions in discovered.values():
                for version in versions:
                    version["discovery_source"] = "new_artist_full"
                    artist_rows_flat.append(version)
            supplement_rows.extend(artist_rows_flat)
            log(
                f"[artists] {idx}/{len(pending_added_artists_df)} {artist_row['name_primary']} -> "
                f"{len(discovered)} songs / {len(artist_rows_flat)} json targets"
            )
            if idx % 20 == 0 or idx == len(pending_added_artists_df):
                checkpoint(existing_rows, supplement_rows)
            time.sleep(0.5)

    merged_rows = merge_discovery_rows(existing_rows, supplement_rows)
    known_song_keys = current_song_keys(merged_rows)

    client = UltimateGuitarClient()
    target_new_rows: List[dict] = []
    log(f"Targeted discovery for {len(targets_df)} Billboard songs still missing")
    for idx, row in enumerate(targets_df.to_dict(orient="records"), start=1):
        artist_id = str(row.get("artist_id", "")).strip()
        song_name = str(row.get("song_name", "")).strip()
        if not artist_id or not song_name or artist_id not in artist_rows:
            continue
        key = (artist_id, base.normalize_text(song_name))
        if key in known_song_keys:
            continue
        progress_key = f"{artist_id}::{key[1]}"
        if progress_key in attempted_target_keys:
            continue
        artist_row = {**artist_rows[artist_id], "chart_artist_name": str(row.get("chart_artist_name", "")).strip()}
        versions = discover_target_song(client, artist_row, song_name, max_pages=max_pages_song)
        attempted_target_keys.add(progress_key)
        if versions:
            target_new_rows.extend(versions)
            known_song_keys.add(key)
        if idx % 25 == 0 or idx == len(targets_df):
            log(f"[targets] progress {idx}/{len(targets_df)} | new song matches {len(target_new_rows)}")
        if idx % 50 == 0 or idx == len(targets_df):
            checkpoint(existing_rows, supplement_rows + target_new_rows)
            save_progress_keys(attempted_target_keys)
        time.sleep(0.25)

    supplement_rows.extend(target_new_rows)
    save_progress_keys(attempted_target_keys)

    final_rows = merge_discovery_rows(existing_rows, supplement_rows)
    save_json_rows(DISCOVERY_JSON, final_rows)
    save_json_rows(SUPPLEMENT_DISCOVERY_JSON, supplement_rows)
    log(
        f"Merged discovery saved: existing={len(existing_rows)}, supplement={len(supplement_rows)}, "
        f"final={len(final_rows)}"
    )

    final_ids = {int(row["id"]) for row in final_rows if row.get("id")}
    new_ids = sorted(final_ids - existing_ids)
    download_rows = [row for row in final_rows if int(row.get("id") or 0) in set(new_ids)]
    log(f"Downloading {len(download_rows)} newly added chord JSON payloads")
    base.download_rows(
        download_rows,
        RAW_CHORDS_DIR,
        workers=workers,
        min_delay=min_delay,
        max_delay=max_delay,
        shard_mod=None,
        shard_rem=None,
    )

    report = {
        "added_artists": int(len(added_artists_df)),
        "target_songs": int(len(targets_df)),
        "existing_discovery_rows": int(len(existing_rows)),
        "supplement_discovery_rows": int(len(supplement_rows)),
        "final_discovery_rows": int(len(final_rows)),
        "new_download_ids": int(len(new_ids)),
        "attempted_target_keys": int(len(attempted_target_keys)),
    }
    REPORT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    log(f"Saved report to {REPORT_JSON.name}")
