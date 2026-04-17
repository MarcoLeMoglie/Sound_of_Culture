def run_discovery(max_pages, checkpoint_every, workers):
    artists = load_artists(ARTIST_CSV)
    progress = load_progress()
    processed_artist_ids = set(progress["processed_artist_ids"])
    chords_map, tabs_map = rebuild_maps(progress)

    total = len(artists)
    log(f"[discover] Loaded {total} artists from {ARTIST_CSV}")
    log(f"[discover] Resume state: {len(processed_artist_ids)} artists already processed")

    pending_artists = [artist for artist in artists if artist["artist_id"] not in processed_artist_ids]
    artist_positions = {artist["artist_id"]: idx for idx, artist in enumerate(artists, start=1)}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_artist = {
            executor.submit(discover_artist_payload, artist_row, max_pages): artist_row
            for artist_row in pending_artists
        }
        for future in as_completed(future_to_artist):
            artist_row = future_to_artist[future]
            artist_id = artist_row["artist_id"]
            position = artist_positions[artist_id]
            log(f"[discover] [{position}/{total}] completed {artist_row['name_primary']}")

            try:
                _, chords_result, tabs_result = future.result()
            except Exception as exc:
                log(f"[discover] artist failed and will be retried on the next run: {artist_row['name_primary']} ({exc})")
                continue

            for key, versions in chords_result.items():
                chords_map[key] = versions
            for key, versions in tabs_result.items():
                tabs_map[key] = versions

            processed_artist_ids.add(artist_id)

            processed_count = len(processed_artist_ids)
            if processed_count % checkpoint_every == 0 or processed_count == total:
                save_progress(processed_artist_ids, chords_map, tabs_map, total)
                log(
                    f"[discover] checkpoint: artists={processed_count}/{total}, "
                    f"chords_json={len(flatten_top_versions(chords_map))}, "
                    f"tabs_json={len(flatten_top_versions(tabs_map))}"
                )

    save_progress(processed_artist_ids, chords_map, tabs_map, total)
    log(
        f"[discover] complete: artists={len(processed_artist_ids)}/{total}, "
        f"chords_json={len(flatten_top_versions(chords_map))}, "
        f"tabs_json={len(flatten_top_versions(tabs_map))}"
    )
