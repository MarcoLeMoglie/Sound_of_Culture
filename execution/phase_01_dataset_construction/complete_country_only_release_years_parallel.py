import concurrent.futures
import json
import sys
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from execution.phase_01_dataset_construction.build_country_only_chords_final import (  # noqa: E402
    OUTPUT_CSV,
    RELEASE_CACHE_FILE,
    USER_AGENT,
    canonical_release_key,
    is_valid_year,
    load_json,
    norm_text,
    save_json,
    year_from_artist_deezer,
    year_from_artist_itunes,
    year_from_artist_musicbrainz,
    year_from_song_discogs,
    year_from_song_itunes_bundle,
    year_from_song_musicbrainz,
    year_from_song_wikidata,
    year_from_song_wikipedia,
)


THREAD_LOCAL = threading.local()
MAX_WORKERS = 4
SAVE_EVERY = 200
ARTIST_PREFILL_MIN_SONGS = 5
ARTIST_PREFILL_MAX_ARTISTS = 80


def log(message: str) -> None:
    print(f"[release-year-parallel] {message}", flush=True)


def get_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        THREAD_LOCAL.session = session
    return session


def lookup_release_year(task: Tuple[str, str, str]) -> Tuple[str, Optional[int], str]:
    artist, song, artist_mbid = task
    key = canonical_release_key(artist, song)
    session = get_session()

    year = year_from_song_musicbrainz(session, artist, song, artist_mbid=artist_mbid)
    if is_valid_year(year):
        return key, int(year), "musicbrainz_song"

    bundle_found = year_from_song_itunes_bundle(session, artist, song, {norm_text(song): song})
    if bundle_found:
        best = min(int(value) for value in bundle_found.values() if is_valid_year(value))
        if is_valid_year(best):
            return key, best, "itunes_song"

    year = year_from_song_wikipedia(session, artist, song)
    if is_valid_year(year):
        return key, int(year), "wikipedia_song"

    year = year_from_song_wikidata(session, artist, song)
    if is_valid_year(year):
        return key, int(year), "wikidata_song"

    deezer_found = year_from_artist_deezer(session, artist, {norm_text(song): song})
    if deezer_found:
        best = min(int(value) for value in deezer_found.values() if is_valid_year(value))
        if is_valid_year(best):
            return key, best, "deezer_song"

    year = year_from_song_discogs(session, artist, song)
    if is_valid_year(year):
        return key, int(year), "discogs_song"

    return key, None, "not_found"


def build_artist_prefill(df: pd.DataFrame, cache: Dict[str, object]) -> tuple[Dict[str, int], Dict[str, str]]:
    missing_unique = (
        df[df["release_year"].isna()][["artist_name", "song_name", "musicbrainz_mbid"]]
        .drop_duplicates(subset=["artist_name", "song_name"])
        .copy()
    )
    songs_by_artist: Dict[str, Dict[str, str]] = {}
    mbid_by_artist: Dict[str, str] = {}
    for row in missing_unique.itertuples(index=False):
        key = canonical_release_key(row.artist_name, row.song_name)
        if is_valid_year(cache.get(key)):
            continue
        artist = str(row.artist_name).strip()
        song = str(row.song_name).strip()
        if not artist or not song:
            continue
        songs_by_artist.setdefault(artist, {})[norm_text(song)] = song
        mbid = "" if pd.isna(row.musicbrainz_mbid) else str(row.musicbrainz_mbid).strip()
        if artist not in mbid_by_artist and mbid:
            mbid_by_artist[artist] = mbid
    artist_order = dict(sorted(songs_by_artist.items(), key=lambda item: (-len(item[1]), item[0])))
    return artist_order, mbid_by_artist


def apply_artist_level_prefill(df: pd.DataFrame, cache: Dict[str, object]) -> Dict[str, int]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    songs_by_artist, mbid_by_artist = build_artist_prefill(df, cache)
    filtered_items = [(artist, songs) for artist, songs in songs_by_artist.items() if len(songs) >= ARTIST_PREFILL_MIN_SONGS]
    filtered_items = filtered_items[:ARTIST_PREFILL_MAX_ARTISTS]
    source_counts = {"itunes_artist": 0, "musicbrainz_artist": 0, "deezer_artist": 0}
    log(f"Artist-level prefill candidates: {len(filtered_items)} artists (min songs {ARTIST_PREFILL_MIN_SONGS}, cap {ARTIST_PREFILL_MAX_ARTISTS})")

    for index, (artist_name, songs_by_norm) in enumerate(filtered_items, start=1):
        artist_mbid = mbid_by_artist.get(artist_name, "")

        itunes_found = year_from_artist_itunes(session, artist_name, songs_by_norm)
        for song_norm, year in itunes_found.items():
            song_title = songs_by_norm.get(song_norm)
            if not song_title:
                continue
            key = canonical_release_key(artist_name, song_title)
            if not is_valid_year(cache.get(key)):
                cache[key] = int(year)
                source_counts["itunes_artist"] += 1

        mb_found = year_from_artist_musicbrainz(session, artist_name, songs_by_norm, artist_mbid=artist_mbid)
        for song_norm, year in mb_found.items():
            song_title = songs_by_norm.get(song_norm)
            if not song_title:
                continue
            key = canonical_release_key(artist_name, song_title)
            if not is_valid_year(cache.get(key)):
                cache[key] = int(year)
                source_counts["musicbrainz_artist"] += 1

        deezer_found = year_from_artist_deezer(session, artist_name, songs_by_norm)
        for song_norm, year in deezer_found.items():
            song_title = songs_by_norm.get(song_norm)
            if not song_title:
                continue
            key = canonical_release_key(artist_name, song_title)
            if not is_valid_year(cache.get(key)):
                cache[key] = int(year)
                source_counts["deezer_artist"] += 1

        if index % 10 == 0 or index == len(filtered_items):
            save_json(RELEASE_CACHE_FILE, cache)
            valid_cache = sum(1 for value in cache.values() if is_valid_year(value))
            log(
                f"Artist prefill {index}/{len(filtered_items)}; "
                f"valid cache years: {valid_cache}; sources: {json.dumps(source_counts, ensure_ascii=False)}"
            )
            time.sleep(0.2)

    save_json(RELEASE_CACHE_FILE, cache)
    return source_counts


def main() -> None:
    df = pd.read_csv(OUTPUT_CSV, low_memory=False)
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    dedicated_cache: Dict[str, object] = load_json(RELEASE_CACHE_FILE)
    prefill_counts = apply_artist_level_prefill(df, dedicated_cache)
    missing_unique = (
        df[df["release_year"].isna()][["artist_name", "song_name", "votes", "rating", "musicbrainz_mbid"]]
        .copy()
        .sort_values(["votes", "rating"], ascending=[False, False])
        .drop_duplicates(subset=["artist_name", "song_name"])
    )
    artist_priority = missing_unique["artist_name"].value_counts().to_dict()
    missing_unique = missing_unique.copy()
    missing_unique["_artist_missing_count"] = missing_unique["artist_name"].map(artist_priority)
    missing_unique["votes"] = pd.to_numeric(missing_unique["votes"], errors="coerce").fillna(0)
    missing_unique["rating"] = pd.to_numeric(missing_unique["rating"], errors="coerce").fillna(0)
    missing_unique.sort_values(
        ["votes", "rating", "_artist_missing_count", "artist_name", "song_name"],
        ascending=[False, False, False, True, True],
        inplace=True,
    )

    tasks = []
    for row in missing_unique.itertuples(index=False):
        key = canonical_release_key(row.artist_name, row.song_name)
        if is_valid_year(dedicated_cache.get(key)):
            continue
        tasks.append((row.artist_name, row.song_name, "" if pd.isna(row.musicbrainz_mbid) else str(row.musicbrainz_mbid).strip()))

    log(f"Unique missing songs queued for parallel enrichment: {len(tasks)}")

    source_counts = {"discogs_song": 0, "musicbrainz_song": 0, "wikipedia_song": 0, "wikidata_song": 0, "itunes_song": 0, "deezer_song": 0, "not_found": 0}
    source_counts.update(prefill_counts)
    completed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {executor.submit(lookup_release_year, task): task for task in tasks}
        for future in concurrent.futures.as_completed(future_to_task):
            completed += 1
            key, year, source = future.result()
            dedicated_cache[key] = year
            source_counts[source] = source_counts.get(source, 0) + 1

            if completed % SAVE_EVERY == 0 or completed == len(tasks):
                save_json(RELEASE_CACHE_FILE, dedicated_cache)
                valid_cache = sum(1 for value in dedicated_cache.values() if is_valid_year(value))
                log(
                    f"Completed {completed}/{len(tasks)} lookups; "
                    f"valid cache years: {valid_cache}; sources: {json.dumps(source_counts, ensure_ascii=False)}"
                )

    save_json(RELEASE_CACHE_FILE, dedicated_cache)
    log("Parallel release-year enrichment finished")


if __name__ == "__main__":
    main()
