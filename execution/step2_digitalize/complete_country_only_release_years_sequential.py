import json
import sys
import time
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from execution.step2_digitalize.build_country_only_chords_final import (  # noqa: E402
    OUTPUT_CSV,
    RELEASE_CACHE_FILE,
    USER_AGENT,
    ITUNES_SEARCH_URL,
    DISCOGS_SEARCH_URL,
    artist_match,
    canonical_release_key,
    generate_candidate_pairs,
    is_valid_year,
    load_json,
    normalize_space,
    norm_text,
    save_json,
    year_from_song_musicbrainz,
    year_from_song_wikidata,
    year_from_song_wikipedia,
)


SAVE_EVERY = 50
ITUNES_PAUSE = 0.4
DISCOGS_PAUSE = 0.4
MB_PAUSE = 1.0
RATE_LIMIT_SLEEP = 90
ITUNES_DISABLED = False


def log(message: str) -> None:
    print(f"[release-year-sequential] {message}", flush=True)


def build_priority_missing(df: pd.DataFrame) -> pd.DataFrame:
    missing_unique = (
        df[df["release_year"].isna()][["artist_name", "song_name", "votes", "rating"]]
        .copy()
        .sort_values(["votes", "rating"], ascending=[False, False])
        .drop_duplicates(subset=["artist_name", "song_name"])
    )
    artist_priority = missing_unique["artist_name"].value_counts().to_dict()
    missing_unique["_artist_missing_count"] = missing_unique["artist_name"].map(artist_priority)
    missing_unique["votes"] = pd.to_numeric(missing_unique["votes"], errors="coerce").fillna(0)
    missing_unique["rating"] = pd.to_numeric(missing_unique["rating"], errors="coerce").fillna(0)
    missing_unique.sort_values(
        ["votes", "rating", "_artist_missing_count", "artist_name", "song_name"],
        ascending=[False, False, False, True, True],
        inplace=True,
    )
    return missing_unique


def year_from_song_itunes_exact(session: requests.Session, artist: str, song: str) -> Optional[int]:
    global ITUNES_DISABLED
    if ITUNES_DISABLED:
        return None
    queries = []
    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        query = normalize_space(f"{candidate_artist} {candidate_song}")
        if query and query not in queries:
            queries.append(query)

    song_keys = {norm_text(song)}
    song_keys.update(norm_text(candidate_song) for _, candidate_song in generate_candidate_pairs(artist, song))

    for query in queries[:4]:
        try:
            response = session.get(
                ITUNES_SEARCH_URL,
                params={"term": query, "entity": "song", "limit": 25},
                timeout=20,
            )
        except Exception:
            continue

        if response.status_code in {403, 429}:
            log(f"iTunes rate limit/status {response.status_code} on query '{query}'; sleeping {RATE_LIMIT_SLEEP}s")
            ITUNES_DISABLED = True
            time.sleep(RATE_LIMIT_SLEEP)
            return None
        if response.status_code != 200:
            continue

        try:
            results = response.json().get("results", [])
        except Exception:
            continue

        years = []
        for item in results:
            item_artist = normalize_space(item.get("artistName", ""))
            if not artist_match(artist, item_artist):
                continue
            item_song_norm = norm_text(item.get("trackName", ""))
            if item_song_norm not in song_keys:
                continue
            release_date = normalize_space(item.get("releaseDate", ""))
            if is_valid_year(release_date[:4]):
                years.append(int(release_date[:4]))
        if years:
            return min(years)
        time.sleep(ITUNES_PAUSE)
    return None


def year_from_song_discogs_safe(session: requests.Session, artist: str, song: str) -> Optional[int]:
    try:
        response = session.get(
            DISCOGS_SEARCH_URL,
            params={"q": f"{artist} {song}", "type": "release"},
            timeout=20,
        )
    except Exception:
        return None

    if response.status_code == 429:
        log(f"Discogs rate limit on '{artist} - {song}'; sleeping {RATE_LIMIT_SLEEP}s")
        time.sleep(RATE_LIMIT_SLEEP)
        return None
    if response.status_code != 200:
        return None

    try:
        results = response.json().get("results", [])
    except Exception:
        return None

    song_key = norm_text(song)
    artist_key = norm_text(artist)
    candidates = []
    for item in results[:15]:
        title_text = normalize_space(item.get("title", ""))
        title_norm = norm_text(title_text)
        year = item.get("year")
        if not is_valid_year(year):
            continue
        if song_key and song_key not in title_norm and artist_key and artist_key not in title_norm:
            continue
        candidates.append(int(float(year)))
    time.sleep(DISCOGS_PAUSE)
    return min(candidates) if candidates else None


def main() -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    df = pd.read_csv(OUTPUT_CSV, low_memory=False)
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    cache: Dict[str, object] = load_json(RELEASE_CACHE_FILE)
    missing_unique = build_priority_missing(df)

    tasks = []
    for row in missing_unique.itertuples(index=False):
        key = canonical_release_key(row.artist_name, row.song_name)
        if is_valid_year(cache.get(key)):
            continue
        tasks.append((row.artist_name, row.song_name))

    log(f"Missing songs queued for sequential enrichment: {len(tasks)}")

    source_counts = {"itunes_song": 0, "discogs_song": 0, "musicbrainz_song": 0, "wikipedia_song": 0, "wikidata_song": 0, "not_found": 0}
    for idx, (artist, song) in enumerate(tasks, start=1):
        key = canonical_release_key(artist, song)

        year = year_from_song_discogs_safe(session, artist, song)
        source = "discogs_song"

        if not is_valid_year(year):
            year = year_from_song_itunes_exact(session, artist, song)
            source = "itunes_song" if is_valid_year(year) else "discogs_song"

        if not is_valid_year(year):
            year = year_from_song_musicbrainz(session, artist, song)
            time.sleep(MB_PAUSE)
            source = "musicbrainz_song" if is_valid_year(year) else "not_found"

        if not is_valid_year(year):
            year = year_from_song_wikipedia(session, artist, song)
            source = "wikipedia_song" if is_valid_year(year) else "not_found"

        if not is_valid_year(year):
            year = year_from_song_wikidata(session, artist, song)
            source = "wikidata_song" if is_valid_year(year) else "not_found"

        cache[key] = int(year) if is_valid_year(year) else None
        source_counts[source] = source_counts.get(source, 0) + 1

        if idx % SAVE_EVERY == 0 or idx == len(tasks):
            save_json(RELEASE_CACHE_FILE, cache)
            valid_cache = sum(1 for value in cache.values() if is_valid_year(value))
            log(
                f"Completed {idx}/{len(tasks)} lookups; valid cache years: {valid_cache}; "
                f"sources: {json.dumps(source_counts, ensure_ascii=False)}"
            )

    save_json(RELEASE_CACHE_FILE, cache)
    log("Sequential release-year enrichment finished")


if __name__ == "__main__":
    main()
