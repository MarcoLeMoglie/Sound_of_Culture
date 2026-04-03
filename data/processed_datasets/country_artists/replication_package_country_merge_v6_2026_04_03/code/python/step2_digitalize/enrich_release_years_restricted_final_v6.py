import json
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
import requests
from difflib import SequenceMatcher

BASE_DIR = Path("data/processed_datasets/country_artists")
TARGET_CSV = BASE_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
TARGET_DTA = BASE_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.dta"

CACHE_DIR = BASE_DIR / "intermediate" / "json_caches"
DEDICATED_CACHE_FILE = CACHE_DIR / "release_years_cache_restricted_final_v6.json"
MANUAL_YEAR_OVERRIDES = {
    "Bellamy Brothers - Normal Aint Coming Back Jesus Is": 2024,
    "Boxcar Willie - Dont Pretend": 1998,
    "Lorrie Morgan - He Drinks Tequila She Talks Dirty In Spanish": 2001,
}
HISTORICAL_CACHE_FILES = [
    CACHE_DIR / "release_years_cache.json",
    CACHE_DIR / "release_years_cache_wiki.json",
    CACHE_DIR / "release_years_cache_discogs.json",
    CACHE_DIR / "release_years_cache_fuzzy.json",
    CACHE_DIR / "release_years_cache_internet.json",
]

USER_AGENT = "SoundOfCultureReleaseYear/1.0 (https://github.com/MarcoLeMoglie/Sound_of_Culture)"
MB_SEARCH_URL = "https://musicbrainz.org/ws/2/recording/"
ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
DEEZER_TRACK_SEARCH_URL = "https://api.deezer.com/search/track"
DEEZER_ALBUM_URL = "https://api.deezer.com/album"
DISCOGS_SEARCH_URL = "https://api.discogs.com/database/search"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
ITUNES_PAUSE_SECONDS = 0.35
DEEZER_PAUSE_SECONDS = 0.25
DISCOGS_PAUSE_SECONDS = 0.35


def log(message: str) -> None:
    print(f"[release-year-v6] {message}", flush=True)


def normalize_space(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    return re.sub(r"\s+", " ", text).strip()


def is_missing_year(value: object) -> bool:
    if pd.isna(value):
        return True
    text = normalize_space(value).lower()
    return text in {"", "nan", "none", "0", "0.0"}


def is_valid_year(value: object) -> bool:
    try:
        year = int(float(value))
    except (TypeError, ValueError):
        return False
    return 1900 <= year <= 2030


def canonical_key(artist: object, song: object) -> str:
    return f"{normalize_space(artist)} - {normalize_space(song)}"


def clean_for_search(text: str) -> str:
    text = normalize_space(text)
    text = text.replace("&", " and ")
    text = text.replace("+", " and ")
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\[[^]]*\]", "", text)
    text = re.sub(r"\b(chords?|tabs?|acoustic|live|remix|cover|official|version|ver)\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^A-Za-z0-9' -]+", " ", text)
    return normalize_space(text)


def maybe_fix_mojibake(text: str) -> str:
    text = normalize_space(text)
    if not text:
        return text
    replacements = {
        "ÃÂ­": "í",
        "ÃÂ¡": "á",
        "ÃÂ©": "é",
        "ÃÂ³": "ó",
        "ÃÂº": "ú",
        "Ã": "",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return normalize_space(text)


def add_contraction_variants(text: str) -> list[str]:
    variants = [normalize_space(text)]
    rules = [
        (r"\bAint\b", "Ain't"),
        (r"\bCant\b", "Can't"),
        (r"\bDont\b", "Don't"),
        (r"\bDoesnt\b", "Doesn't"),
        (r"\bDidnt\b", "Didn't"),
        (r"\bIm\b", "I'm"),
        (r"\bIve\b", "I've"),
        (r"\bIll\b", "I'll"),
        (r"\bId\b", "I'd"),
        (r"\bIts\b", "It's"),
        (r"\bThats\b", "That's"),
        (r"\bTheres\b", "There's"),
        (r"\bWhats\b", "What's"),
        (r"\bWont\b", "Won't"),
        (r"\bYoure\b", "You're"),
        (r"\bYouve\b", "You've"),
        (r"\bShell\b", "She'll"),
        (r"\bHeres\b", "Here's"),
        (r"\bCouldnt\b", "Couldn't"),
        (r"\bWouldnt\b", "Wouldn't"),
        (r"\bShouldnt\b", "Shouldn't"),
    ]
    base = normalize_space(text)
    for pattern, replacement in rules:
        updated = re.sub(pattern, replacement, base)
        updated = normalize_space(updated)
        if updated and updated not in variants:
            variants.append(updated)
    return variants


def normalize_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", clean_for_search(text).lower())


def token_signature(text: str) -> str:
    return " ".join(sorted(token for token in clean_for_search(text).lower().split() if token))


def generate_candidate_pairs(artist: str, song: str) -> Iterable[Tuple[str, str]]:
    artist_clean = clean_for_search(maybe_fix_mojibake(artist))
    song_clean = clean_for_search(maybe_fix_mojibake(song))
    pairs = []

    def add_pair(a: str, s: str) -> None:
        a = normalize_space(a)
        s = normalize_space(s)
        if a and s and (a, s) not in pairs:
            pairs.append((a, s))

    artist_variants = []
    song_variants = []
    for value in add_contraction_variants(artist):
        cleaned = clean_for_search(maybe_fix_mojibake(value))
        if cleaned and cleaned not in artist_variants:
            artist_variants.append(cleaned)
    for value in add_contraction_variants(song):
        cleaned = clean_for_search(maybe_fix_mojibake(value))
        if cleaned and cleaned not in song_variants:
            song_variants.append(cleaned)

    artist_variants = artist_variants or [artist_clean]
    song_variants = song_variants or [song_clean]

    for a in artist_variants[:4]:
        for s in song_variants[:6]:
            add_pair(a, s)

    if artist_clean.lower().startswith("the "):
        add_pair(artist_clean[4:], song_clean)

    for pattern in [r"\s+and\s+his\s+.+$", r"\s+and\s+the\s+.+$", r"\s*&\s+.+$", r"\s+band$"]:
        reduced_artist = re.sub(pattern, "", artist_clean, flags=re.IGNORECASE).strip()
        add_pair(reduced_artist, song_clean)

    for prefix in [
        "20Th Century Masters The Millennium Collection Best Of ",
        "The Definitive Collection ",
        "Greatest Hits ",
        "Best Of ",
    ]:
        if song_clean.lower().startswith(prefix.lower()):
            add_pair(artist_clean, song_clean[len(prefix):].strip())

    return pairs


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        with path.open() as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
    except Exception as exc:
        log(f"Could not load cache {path.name}: {exc}")
    return {}


def save_json(path: Path, payload: Dict[str, object]) -> None:
    with path.open("w") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def build_master_cache() -> Dict[str, int]:
    master: Dict[str, int] = {}
    for path in HISTORICAL_CACHE_FILES + [DEDICATED_CACHE_FILE]:
        payload = load_json(path)
        for key, value in payload.items():
            if is_valid_year(value):
                master[key] = int(float(value))
    return master


def build_normalized_cache(master_cache: Dict[str, int]) -> Dict[str, int]:
    normalized: Dict[str, int] = {}
    for key, value in master_cache.items():
        normalized[normalize_key(key)] = value
    return normalized


def year_from_musicbrainz(session: requests.Session, artist: str, song: str) -> Optional[int]:
    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        params = {
            "query": f'artist:"{candidate_artist}" AND recording:"{candidate_song}"',
            "fmt": "json",
            "limit": 10,
        }
        try:
            response = session.get(MB_SEARCH_URL, params=params, timeout=25)
            if response.status_code == 503:
                time.sleep(3)
                continue
            response.raise_for_status()
            data = response.json()
        except Exception as exc:
            log(f"MusicBrainz failed for {candidate_artist} / {candidate_song}: {exc}")
            continue

        years = []
        for recording in data.get("recordings", []):
            artist_credit = normalize_space(recording.get("artist-credit-phrase", "")).lower()
            if normalize_key(candidate_artist) not in normalize_key(artist_credit) and normalize_key(artist_credit) not in normalize_key(candidate_artist):
                continue
            score = int(recording.get("score", 0) or 0)
            if score < 70:
                continue
            for release in recording.get("releases", []):
                date = normalize_space(release.get("date", ""))
                year = date.split("-")[0]
                if year.isdigit() and is_valid_year(year):
                    years.append(int(year))

        if years:
            return min(years)
        time.sleep(1.05)
    return None


def year_from_itunes(session: requests.Session, artist: str, song: str) -> Optional[int]:
    artist_key = normalize_key(artist)
    song_key = normalize_key(song)
    song_tokens = token_signature(song)
    candidates = []

    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        results = None
        for attempt in range(3):
            try:
                response = session.get(
                    ITUNES_SEARCH_URL,
                    params={"term": f"{candidate_artist} {candidate_song}", "entity": "song", "limit": 20},
                    timeout=20,
                )
                if response.status_code == 429:
                    time.sleep(2.5 * (attempt + 1))
                    continue
                response.raise_for_status()
                results = response.json().get("results", [])
                break
            except Exception as exc:
                if attempt == 2:
                    log(f"iTunes failed for {candidate_artist} / {candidate_song}: {exc}")
                time.sleep(1.0 * (attempt + 1))
        if results is None:
            continue

        for item in results:
            release_date = normalize_space(item.get("releaseDate", ""))
            release_year = release_date[:4]
            if not is_valid_year(release_year):
                continue

            item_artist = normalize_space(item.get("artistName", ""))
            item_track = normalize_space(item.get("trackName", ""))
            item_artist_key = normalize_key(item_artist)
            item_track_key = normalize_key(item_track)
            item_song_tokens = token_signature(item_track)

            artist_ratio = SequenceMatcher(None, artist_key, item_artist_key).ratio() if artist_key and item_artist_key else 0
            track_ratio = SequenceMatcher(None, song_key, item_track_key).ratio() if song_key and item_track_key else 0
            token_bonus = 0.15 if song_tokens and song_tokens == item_song_tokens else 0
            exact_bonus = 0.25 if song_key and song_key == item_track_key else 0
            score = (artist_ratio * 0.55) + (track_ratio * 0.45) + token_bonus + exact_bonus

            if artist_ratio < 0.72:
                continue
            if track_ratio < 0.60 and token_bonus == 0 and exact_bonus == 0:
                continue

            candidates.append((score, int(release_year), item_artist, item_track))

        time.sleep(ITUNES_PAUSE_SECONDS)

    if not candidates:
        return None

    candidates.sort(key=lambda row: (-row[0], row[1]))
    best_score = candidates[0][0]
    viable_years = [year for score, year, _, _ in candidates if score >= max(0.82, best_score - 0.08)]
    return min(viable_years) if viable_years else candidates[0][1]


def year_from_deezer(session: requests.Session, artist: str, song: str) -> Optional[int]:
    artist_key = normalize_key(artist)
    song_key = normalize_key(song)
    candidates = []

    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        try:
            response = session.get(
                DEEZER_TRACK_SEARCH_URL,
                params={"q": f'artist:"{candidate_artist}" track:"{candidate_song}"'},
                timeout=20,
            )
            response.raise_for_status()
            results = response.json().get("data", [])
        except Exception as exc:
            log(f"Deezer failed for {candidate_artist} / {candidate_song}: {exc}")
            continue

        for item in results[:10]:
            item_artist = normalize_space(item.get("artist", {}).get("name", ""))
            item_track = normalize_space(item.get("title", ""))
            artist_ratio = SequenceMatcher(None, artist_key, normalize_key(item_artist)).ratio() if artist_key else 0
            track_ratio = SequenceMatcher(None, song_key, normalize_key(item_track)).ratio() if song_key else 0
            if artist_ratio < 0.70 or track_ratio < 0.60:
                continue

            release_year = None
            release_date = normalize_space(item.get("release_date", ""))
            if is_valid_year(release_date[:4]):
                release_year = int(release_date[:4])
            else:
                album_id = item.get("album", {}).get("id")
                if album_id:
                    try:
                        album_response = session.get(f"{DEEZER_ALBUM_URL}/{album_id}", timeout=20)
                        album_response.raise_for_status()
                        album_date = normalize_space(album_response.json().get("release_date", ""))
                        if is_valid_year(album_date[:4]):
                            release_year = int(album_date[:4])
                    except Exception:
                        release_year = None

            if release_year is not None:
                score = (artist_ratio * 0.55) + (track_ratio * 0.45)
                candidates.append((score, release_year))

        time.sleep(DEEZER_PAUSE_SECONDS)

    if not candidates:
        return None
    candidates.sort(key=lambda row: (-row[0], row[1]))
    return candidates[0][1]


def year_from_discogs(session: requests.Session, artist: str, song: str) -> Optional[int]:
    artist_key = normalize_key(artist)
    song_key = normalize_key(song)
    candidates = []

    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        try:
            response = session.get(
                DISCOGS_SEARCH_URL,
                params={"q": f"{candidate_artist} {candidate_song}", "type": "release"},
                timeout=20,
            )
            response.raise_for_status()
            results = response.json().get("results", [])
        except Exception as exc:
            log(f"Discogs failed for {candidate_artist} / {candidate_song}: {exc}")
            continue

        for item in results[:12]:
            title = normalize_space(item.get("title", ""))
            year = item.get("year")
            if not is_valid_year(year):
                continue
            title_key = normalize_key(title)
            artist_ratio = SequenceMatcher(None, artist_key, title_key).ratio() if artist_key else 0
            track_ratio = SequenceMatcher(None, song_key, title_key).ratio() if song_key else 0
            artist_contains = 0.25 if artist_key and artist_key in title_key else 0
            track_contains = 0.35 if song_key and song_key in title_key else 0
            artist_score = artist_ratio + artist_contains
            track_score = track_ratio + track_contains
            if artist_score < 0.45 or track_score < 0.35:
                continue
            candidates.append((((artist_score * 0.45) + (track_score * 0.55)), int(float(year))))

        time.sleep(DISCOGS_PAUSE_SECONDS)

    if not candidates:
        return None
    candidates.sort(key=lambda row: (-row[0], row[1]))
    return candidates[0][1]


def year_from_wikipedia(session: requests.Session, artist: str, song: str) -> Optional[int]:
    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        try:
            search_response = session.get(
                WIKIPEDIA_API_URL,
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": f'"{candidate_song}" "{candidate_artist}" song',
                    "format": "json",
                    "srlimit": 3,
                },
                timeout=20,
            )
            search_response.raise_for_status()
            search_results = search_response.json().get("query", {}).get("search", [])
        except Exception as exc:
            log(f"Wikipedia search failed for {candidate_artist} / {candidate_song}: {exc}")
            continue

        for item in search_results:
            title = item.get("title", "")
            try:
                content_response = session.get(
                    WIKIPEDIA_API_URL,
                    params={
                        "action": "query",
                        "prop": "revisions",
                        "rvprop": "content",
                        "rvslots": "main",
                        "titles": title,
                        "format": "json",
                    },
                    timeout=20,
                )
                content_response.raise_for_status()
                pages = content_response.json().get("query", {}).get("pages", {})
            except Exception as exc:
                log(f"Wikipedia content failed for {title}: {exc}")
                continue

            for page in pages.values():
                revisions = page.get("revisions", [])
                if not revisions:
                    continue
                content = revisions[0].get("slots", {}).get("main", {}).get("*", "")
                match = re.search(r"Released\s*=\s*.*?(\d{4})", content, re.IGNORECASE | re.DOTALL)
                if match and is_valid_year(match.group(1)):
                    return int(match.group(1))
        time.sleep(0.5)
    return None


def save_outputs(df: pd.DataFrame) -> None:
    """Save CSV and DTA with variable labels applied."""
    df.to_csv(TARGET_CSV, index=False, encoding="utf-8")
    
    # 1. Variable Labeling
    # Start with labels from the data dictionary if available
    variable_labels = {}
    dict_path = BASE_DIR / "country_artists_data_dictionary.csv"
    if dict_path.exists():
        try:
            df_dict = pd.read_csv(dict_path)
            for _, row in df_dict.iterrows():
                col = row["column_name"]
                desc = row["description"]
                if pd.notna(col) and pd.notna(desc):
                    # Stata labels are capped at 80 chars
                    variable_labels[col] = str(desc)[:80]
        except Exception as exc:
            log(f"Warning: Could not read data dictionary for labels: {exc}")

    # 2. Add/Override labels for Song-Level Metrics
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
        "release_year": "Song Release Year (Consolidated Phase 4 Recovery)",
        "chord_1": "Most frequent chord in the song",
        "chord_1_count": "Usage count of the most frequent chord",
        "chord_2": "Second most frequent chord in the song",
        "chord_2_count": "Usage count of the second most frequent chord",
        "chord_3": "Third most frequent chord in the song",
        "chord_3_count": "Usage count of the third most frequent chord",
    }
    variable_labels.update(song_metrics_labels)

    # 3. Clean for Stata compatibility
    stata_df = df.copy()
    
    # Filter labels to only include columns present in the dataframe
    final_labels = {k: v for k, v in variable_labels.items() if k in stata_df.columns}

    # Truncate string columns to 244 chars (Stata limit)
    for col in stata_df.select_dtypes(include=["object"]).columns:
        stata_df[col] = stata_df[col].fillna("").astype(str).str.slice(0, 244)
        stata_df.loc[stata_df[col] == "nan", col] = ""

    # Ensure release_year is correctly typed
    if "release_year" in stata_df.columns:
        stata_df["release_year"] = pd.to_numeric(stata_df["release_year"], errors="coerce")

    try:
        stata_df.to_stata(
            TARGET_DTA, 
            write_index=False, 
            version=118, 
            variable_labels=final_labels
        )
        log(f"Saved {TARGET_CSV.name} and {TARGET_DTA.name} (with {len(final_labels)} labels)")
    except Exception as exc:
        log(f"Error saving labeled DTA: {exc}. Trying fallback without labels.")
        stata_df.to_stata(TARGET_DTA, write_index=False, version=118)


def main() -> None:
    if not TARGET_CSV.exists():
        raise FileNotFoundError(f"{TARGET_CSV} not found")

    df = pd.read_csv(TARGET_CSV, low_memory=False)
    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")

    dedicated_cache = load_json(DEDICATED_CACHE_FILE)
    master_cache = build_master_cache()
    normalized_cache = build_normalized_cache(master_cache)

    missing_mask = df["release_year"].isna()
    unique_missing = df.loc[missing_mask, ["artist_name", "song_name"]].drop_duplicates()
    log(f"Missing release-year rows before enrichment: {int(missing_mask.sum())}")
    log(f"Unique missing song pairs before enrichment: {len(unique_missing)}")

    cache_fills = 0
    for idx in df.index[missing_mask]:
        key = canonical_key(df.at[idx, "artist_name"], df.at[idx, "song_name"])
        if key in MANUAL_YEAR_OVERRIDES:
            df.at[idx, "release_year"] = MANUAL_YEAR_OVERRIDES[key]
            dedicated_cache[key] = MANUAL_YEAR_OVERRIDES[key]
            cache_fills += 1
            continue
        if key in master_cache:
            df.at[idx, "release_year"] = master_cache[key]
            cache_fills += 1
            continue
        nkey = normalize_key(key)
        if nkey in normalized_cache:
            df.at[idx, "release_year"] = normalized_cache[nkey]
            cache_fills += 1
    log(f"Rows filled from historical caches: {cache_fills}")

    remaining_pairs = (
        df.loc[df["release_year"].isna(), ["artist_name", "song_name"]]
        .drop_duplicates()
        .sort_values(["artist_name", "song_name"])
    )
    log(f"Unique pairs still missing after cache pass: {len(remaining_pairs)}")

    if os.environ.get("SOC_RELEASE_YEAR_CACHE_ONLY") == "1":
        final_missing = int(df["release_year"].isna().sum())
        log("Cache-only mode enabled; skipping live release-year lookup.")
        log(f"Missing release-year rows after enrichment: {final_missing}")
        save_json(DEDICATED_CACHE_FILE, dedicated_cache)
        save_outputs(df)
        return

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    live_found = 0
    for index, row in enumerate(remaining_pairs.itertuples(index=False), start=1):
        artist = normalize_space(row.artist_name)
        song = normalize_space(row.song_name)
        key = canonical_key(artist, song)

        cached_value = dedicated_cache.get(key)
        if is_valid_year(cached_value):
            year = int(float(cached_value))
        else:
            year = year_from_itunes(session, artist, song)
            if year is None:
                year = year_from_deezer(session, artist, song)
            if year is None:
                year = year_from_discogs(session, artist, song)
            if year is None:
                year = year_from_musicbrainz(session, artist, song)
            if year is None:
                year = year_from_wikipedia(session, artist, song)
            dedicated_cache[key] = year

        if is_valid_year(year):
            year = int(year)
            pair_mask = (
                df["release_year"].isna()
                & df["artist_name"].map(normalize_space).eq(artist)
                & df["song_name"].map(normalize_space).eq(song)
            )
            df.loc[pair_mask, "release_year"] = year
            live_found += int(pair_mask.sum())

        if index % 20 == 0 or index == len(remaining_pairs):
            save_json(DEDICATED_CACHE_FILE, dedicated_cache)
            log(f"Processed {index}/{len(remaining_pairs)} pairs; live rows filled so far: {live_found}")

    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    final_missing = int(df["release_year"].isna().sum())
    log(f"Missing release-year rows after enrichment: {final_missing}")

    save_json(DEDICATED_CACHE_FILE, dedicated_cache)
    save_outputs(df)


if __name__ == "__main__":
    main()
