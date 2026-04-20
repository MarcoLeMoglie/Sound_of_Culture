#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
import warnings
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Tuple

import librosa
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.phase_01_dataset_construction import build_country_only_chords_final as final_builder

warnings.filterwarnings("ignore", message="PySoundFile failed. Trying audioread instead.")
warnings.filterwarnings("ignore", message="librosa.core.audio.__audioread_load", category=FutureWarning)


BASE_DIR = Path("data/processed_datasets/country_artists")
TARGET_CSV = BASE_DIR / "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv"
TARGET_DTA = BASE_DIR / "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.dta"
RAW_TABS_DIR = Path("data/raw_tabs_country")
INTERMEDIATE_DIR = BASE_DIR / "intermediate" / "final_song_metadata_backfill"
SUMMARY_JSON = INTERMEDIATE_DIR / "final_song_metadata_backfill_summary_2026_04_19.json"
ITUNES_CACHE = INTERMEDIATE_DIR / "itunes_track_cache.json"
ITUNES_EXACT_CACHE = INTERMEDIATE_DIR / "itunes_exact_track_cache.json"
DEEZER_SEARCH_CACHE = INTERMEDIATE_DIR / "deezer_search_cache.json"
DEEZER_TRACK_CACHE = INTERMEDIATE_DIR / "deezer_track_cache.json"
DEEZER_ALBUM_CACHE = INTERMEDIATE_DIR / "deezer_album_cache.json"
DISCOGS_CACHE = INTERMEDIATE_DIR / "discogs_genre_cache.json"
LOCAL_UG_CACHE = INTERMEDIATE_DIR / "local_ug_song_metadata_cache.json"
PREVIEW_BPM_CACHE = INTERMEDIATE_DIR / "preview_bpm_cache.json"

ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
DEEZER_TRACK_SEARCH_URL = "https://api.deezer.com/search/track"
DEEZER_TRACK_URL = "https://api.deezer.com/track"
DEEZER_ALBUM_URL = "https://api.deezer.com/album"
DISCOGS_SEARCH_URL = "https://api.discogs.com/database/search"

NUMERIC_LABELS = {
    "bpm": "Estimated tempo in BPM",
}

EXTRA_LABELS = {
    "genre_ug_original": "Original Ultimate Guitar genre tag",
    "genre_official_raw": "Raw official song genre",
    "genre_source": "Genre source used in final field",
    "genre_is_official": "Final genre comes from official external source",
    "bpm_source": "Source used for BPM field",
}

CACHE_ONLY = False
ENABLE_PREVIEW_ESTIMATION = True
DISABLE_ITUNES_EXACT = False
DISABLE_DISCOGS = False


def log(message: str) -> None:
    print(f"[final-song-backfill] {message}", flush=True)


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return default


def save_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def canonical_key(artist: object, song: object) -> str:
    return f"{final_builder.normalize_key(artist)}||{final_builder.normalize_key(song)}"


def nonempty(value: object) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    return "" if text.lower() == "nan" else text


def is_missing_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().eq("")


def normalize_official_genre(raw: object) -> str:
    text = nonempty(raw).lower()
    if not text:
        return ""
    compact = text.replace("&", "and")
    if "country" in compact:
        return "country"
    if "bluegrass" in compact:
        return "bluegrass"
    if "americana" in compact:
        return "americana"
    if "folk" in compact:
        return "folk"
    if "singer/songwriter" in compact or "singer songwriter" in compact:
        return "singer-songwriter"
    if "hip hop" in compact or "rap" in compact:
        return "hip hop"
    if "r&b" in text or "rnb" in compact or "soul" in compact or "funk" in compact:
        return "r&b funk & soul"
    if "rock" in compact:
        return "rock"
    if "pop" in compact:
        return "pop"
    if "electronic" in compact or "dance" in compact:
        return "electronic"
    if "soundtrack" in compact:
        return "soundtrack"
    return text


def best_discogs_genre(genres: Iterable[str], styles: Iterable[str]) -> str:
    style_values = [nonempty(value) for value in styles if nonempty(value)]
    genre_values = [nonempty(value) for value in genres if nonempty(value)]
    if style_values:
        return style_values[0]
    if genre_values:
        return genre_values[0]
    return ""


def initialize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "genre_ug_original" not in out.columns:
        out["genre_ug_original"] = out["genre"].fillna("").astype(str)
    else:
        mask = out["genre_ug_original"].fillna("").astype(str).str.strip().eq("")
        out.loc[mask, "genre_ug_original"] = out.loc[mask, "genre"].fillna("").astype(str)
    for col, default in [
        ("genre_official_raw", ""),
        ("genre_source", ""),
        ("genre_is_official", 0),
        ("bpm_source", ""),
    ]:
        if col not in out.columns:
            out[col] = default
    return out


def set_existing_sources(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    genre_present = out["genre"].fillna("").astype(str).str.strip().ne("")
    source_missing = out["genre_source"].fillna("").astype(str).str.strip().eq("")
    out.loc[genre_present & source_missing, "genre_source"] = "ug_selected"

    bpm_numeric = pd.to_numeric(out["bpm"], errors="coerce")
    bpm_present = bpm_numeric.notna() & bpm_numeric.ne(0)
    bpm_source_missing = out["bpm_source"].fillna("").astype(str).str.strip().eq("")
    out.loc[bpm_present & bpm_source_missing, "bpm_source"] = "ug_selected"
    return out


def fill_genre_from_artist_metadata(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    missing_genre = out["genre"].fillna("").astype(str).str.strip().eq("")
    artist_genres = out["genres_normalized"].fillna("").astype(str)
    candidate = artist_genres.str.split("|").str[0].fillna("").map(normalize_official_genre)
    fill_mask = missing_genre & candidate.ne("")
    out.loc[fill_mask, "genre"] = candidate[fill_mask]
    out.loc[fill_mask, "genre_source"] = "artist_metadata_fallback"
    out.loc[fill_mask, "genre_is_official"] = 0
    return out


def build_local_ug_lookup(target_pairs: set[str]) -> Dict[str, dict]:
    cache = load_json(LOCAL_UG_CACHE, {})
    if cache:
        return cache

    lookup: Dict[str, dict] = {}
    total = 0
    for path in RAW_TABS_DIR.glob("*.json"):
        total += 1
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        key = canonical_key(payload.get("artist_name", ""), payload.get("song_name", ""))
        if key not in target_pairs:
            continue

        current = lookup.get(key, {})
        genre = nonempty(payload.get("advertising", {}).get("targeting_song_genre", ""))
        bpm = None
        tab_view = payload.get("tab_view", {})
        meta = tab_view.get("meta", []) if isinstance(tab_view, dict) else []
        if isinstance(meta, list):
            for item in meta:
                if isinstance(item, dict):
                    candidate = pd.to_numeric(item.get("bpm"), errors="coerce")
                    if pd.notna(candidate) and float(candidate) > 0:
                        bpm = int(float(candidate))
                        break
        if genre and not current.get("genre"):
            current["genre"] = genre
        if bpm and not current.get("bpm"):
            current["bpm"] = bpm
        if current:
            lookup[key] = current

        if total % 10000 == 0:
            log(f"Scanned {total} local UG JSON files for alternate metadata")
    save_json(LOCAL_UG_CACHE, lookup)
    return lookup


def itunes_artist_query(session: requests.Session, artist: str, cache: Dict[str, list]) -> List[dict]:
    cache_key = final_builder.normalize_key(artist)
    if cache_key in cache:
        return cache[cache_key]
    if CACHE_ONLY:
        return []
    try:
        response = session.get(
            ITUNES_SEARCH_URL,
            params={"term": artist, "entity": "song", "attribute": "artistTerm", "limit": 200},
            timeout=30,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
    except Exception as exc:
        log(f"iTunes artist query failed for {artist}: {exc}")
        results = []
    cache[cache_key] = results
    return results


def itunes_song_query(session: requests.Session, artist: str, song: str, cache: Dict[str, list]) -> List[dict]:
    global DISABLE_ITUNES_EXACT
    cache_key = canonical_key(artist, song)
    if cache_key in cache:
        return cache[cache_key]
    if CACHE_ONLY or DISABLE_ITUNES_EXACT:
        return []
    try:
        response = session.get(
            ITUNES_SEARCH_URL,
            params={"term": f"{artist} {song}", "entity": "song", "limit": 50},
            timeout=30,
        )
        if response.status_code in {403, 429}:
            DISABLE_ITUNES_EXACT = True
            log(f"Disabling exact iTunes song queries for the rest of the run after status {response.status_code}")
            cache[cache_key] = []
            return []
        response.raise_for_status()
        results = response.json().get("results", [])
    except Exception as exc:
        log(f"iTunes targeted song query failed for {artist} / {song}: {exc}")
        results = []
    cache[cache_key] = results
    return results


def deezer_artist_query(session: requests.Session, artist: str, cache: Dict[str, list]) -> List[dict]:
    cache_key = final_builder.normalize_key(artist)
    if cache_key in cache:
        return cache[cache_key]
    if CACHE_ONLY:
        return []

    results: List[dict] = []
    index = 0
    while True:
        try:
            response = session.get(
                DEEZER_TRACK_SEARCH_URL,
                params={"q": f'artist:"{artist}"', "limit": 100, "index": index},
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            log(f"Deezer artist query failed for {artist}: {exc}")
            break

        page_data = payload.get("data", [])
        if not page_data:
            break
        results.extend(page_data)
        total = int(payload.get("total", len(results)) or len(results))
        index += len(page_data)
        if index >= total or len(page_data) < 100 or index >= 500:
            break
        time.sleep(0.2)

    cache[cache_key] = results
    return results


def deezer_track_payload(session: requests.Session, track_id: int, cache: Dict[str, dict]) -> dict:
    cache_key = str(track_id)
    if cache_key in cache:
        return cache[cache_key]
    if CACHE_ONLY:
        return {}
    try:
        response = session.get(f"{DEEZER_TRACK_URL}/{track_id}", timeout=20)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        log(f"Deezer track lookup failed for {track_id}: {exc}")
        payload = {}
    cache[cache_key] = payload
    return payload


def deezer_album_payload(session: requests.Session, album_id: int, cache: Dict[str, dict]) -> dict:
    cache_key = str(album_id)
    if cache_key in cache:
        return cache[cache_key]
    if CACHE_ONLY:
        return {}
    try:
        response = session.get(f"{DEEZER_ALBUM_URL}/{album_id}", timeout=20)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        log(f"Deezer album lookup failed for {album_id}: {exc}")
        payload = {}
    cache[cache_key] = payload
    return payload


def estimate_bpm_from_preview(session: requests.Session, preview_url: str, cache: Dict[str, object]) -> Optional[int]:
    if not ENABLE_PREVIEW_ESTIMATION:
        return None
    if not preview_url:
        return None
    if preview_url in cache:
        cached = pd.to_numeric(cache[preview_url], errors="coerce")
        if pd.notna(cached) and float(cached) > 0:
            return int(float(cached))
        return None
    if CACHE_ONLY:
        return None

    estimated: Optional[int] = None
    temp_path: Optional[str] = None
    try:
        response = session.get(preview_url, timeout=30)
        response.raise_for_status()
        suffix = ".m4a" if ".m4a" in preview_url else ".mp3"
        with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(response.content)
            temp_path = tmp.name
        y, sr = librosa.load(temp_path, sr=22050, mono=True, duration=30)
        if len(y):
            tempo, _ = librosa.beat.beat_track(y=y, sr=sr, trim=False)
            tempo_value = float(tempo[0]) if hasattr(tempo, "__len__") else float(tempo)
            if 40 <= tempo_value <= 240:
                estimated = int(round(tempo_value))
    except Exception:
        estimated = None
    finally:
        if temp_path:
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass

    cache[preview_url] = estimated
    return estimated


def discogs_song_query(session: requests.Session, artist: str, song: str, cache: Dict[str, dict]) -> dict:
    global DISABLE_DISCOGS
    key = canonical_key(artist, song)
    if key in cache:
        return cache[key]
    if CACHE_ONLY or DISABLE_DISCOGS:
        return {}
    try:
        response = session.get(
            DISCOGS_SEARCH_URL,
            params={"q": f"{artist} {song}", "type": "release"},
            timeout=25,
        )
        if response.status_code == 429:
            DISABLE_DISCOGS = True
            log("Disabling Discogs release queries for the rest of the run after a 429 rate-limit response")
            cache[key] = {}
            return {}
        response.raise_for_status()
        results = response.json().get("results", [])
    except Exception as exc:
        log(f"Discogs song query failed for {artist} / {song}: {exc}")
        results = []

    song_key = final_builder.normalize_key(song)
    artist_key = final_builder.normalize_key(artist)
    best = {}
    for item in results[:15]:
        title_key = final_builder.normalize_key(item.get("title", ""))
        if song_key and song_key not in title_key and artist_key and artist_key not in title_key:
            continue
        genre = best_discogs_genre(item.get("genre", []), item.get("style", []))
        if genre:
            best = {"genre": genre}
            break
    cache[key] = best
    return best


def exact_title_match(candidate: str, observed: str) -> bool:
    return final_builder.norm_text(candidate) == final_builder.norm_text(observed)


def artist_search_fill(
    df: pd.DataFrame,
    session: requests.Session,
    itunes_cache: Dict[str, list],
    deezer_search_cache: Dict[str, list],
    deezer_track_cache: Dict[str, dict],
    deezer_album_cache: Dict[str, dict],
    preview_bpm_cache: Dict[str, object],
) -> pd.DataFrame:
    out = df.copy()
    bpm_missing_mask = pd.to_numeric(out["bpm"], errors="coerce").isna() | pd.to_numeric(out["bpm"], errors="coerce").eq(0)
    genre_needs_upgrade_mask = (
        out["genre"].fillna("").astype(str).str.strip().eq("")
        | out["genre_source"].fillna("").astype(str).str.strip().isin(["", "ug_selected", "ug_alternative_local", "artist_metadata_fallback"])
    )

    target_mask = bpm_missing_mask | genre_needs_upgrade_mask
    artist_groups = (
        out.loc[target_mask, ["artist_name", "song_name"]]
        .drop_duplicates()
        .groupby("artist_name")["song_name"]
        .apply(list)
        .to_dict()
    )

    for idx, (artist, songs) in enumerate(artist_groups.items(), start=1):
        song_lookup = {final_builder.norm_text(song): song for song in songs}
        artist_mask = out["artist_name"].eq(artist)
        need_genre_for_artist = bool(genre_needs_upgrade_mask[artist_mask].any())
        need_bpm_for_artist = bool(bpm_missing_mask[artist_mask].any())

        if need_genre_for_artist:
            itunes_results = itunes_artist_query(session, artist, itunes_cache)
            for item in itunes_results:
                item_artist = final_builder.normalize_space(item.get("artistName", ""))
                if not final_builder.artist_match(artist, item_artist):
                    continue
                song_norm = final_builder.norm_text(item.get("trackName", ""))
                if song_norm not in song_lookup:
                    continue
                official_raw = nonempty(item.get("primaryGenreName", ""))
                if not official_raw:
                    continue
                mask = out["artist_name"].eq(artist) & out["song_name"].eq(song_lookup[song_norm])
                out.loc[mask, "genre_official_raw"] = official_raw
                out.loc[mask, "genre"] = normalize_official_genre(official_raw)
                out.loc[mask, "genre_source"] = "apple_music_track"
                out.loc[mask, "genre_is_official"] = 1
                if pd.to_numeric(out.loc[mask, "bpm"], errors="coerce").isna().any():
                    preview_url = nonempty(item.get("previewUrl", ""))
                    if preview_url:
                        estimated_bpm = estimate_bpm_from_preview(session, preview_url, preview_bpm_cache)
                        if estimated_bpm:
                            out.loc[mask & pd.to_numeric(out["bpm"], errors="coerce").isna(), "bpm"] = estimated_bpm
                            out.loc[mask & out["bpm_source"].fillna("").astype(str).str.strip().eq(""), "bpm_source"] = "apple_preview_estimated"

        if need_bpm_for_artist or need_genre_for_artist:
            deezer_results = deezer_artist_query(session, artist, deezer_search_cache)
            for item in deezer_results:
                item_artist = final_builder.normalize_space(item.get("artist", {}).get("name", ""))
                if not final_builder.artist_match(artist, item_artist):
                    continue
                song_norm = final_builder.norm_text(item.get("title", ""))
                if song_norm not in song_lookup:
                    continue
                mask = out["artist_name"].eq(artist) & out["song_name"].eq(song_lookup[song_norm])

                track_payload = deezer_track_payload(session, int(item.get("id")), deezer_track_cache) if item.get("id") else {}
                bpm_value = pd.to_numeric(track_payload.get("bpm"), errors="coerce")
                if pd.notna(bpm_value) and float(bpm_value) > 0:
                    out.loc[mask & pd.to_numeric(out["bpm"], errors="coerce").isna(), "bpm"] = int(float(bpm_value))
                    out.loc[mask & out["bpm_source"].fillna("").astype(str).str.strip().eq(""), "bpm_source"] = "deezer_track"
                elif pd.to_numeric(out.loc[mask, "bpm"], errors="coerce").isna().any():
                    preview_url = nonempty(track_payload.get("preview", "")) or nonempty(item.get("preview", ""))
                    if preview_url:
                        estimated_bpm = estimate_bpm_from_preview(session, preview_url, preview_bpm_cache)
                        if estimated_bpm:
                            out.loc[mask & pd.to_numeric(out["bpm"], errors="coerce").isna(), "bpm"] = estimated_bpm
                            out.loc[mask & out["bpm_source"].fillna("").astype(str).str.strip().eq(""), "bpm_source"] = "deezer_preview_estimated"

                album_id = track_payload.get("album", {}).get("id") or item.get("album", {}).get("id")
                if album_id and need_genre_for_artist and out.loc[mask, "genre_is_official"].fillna(0).eq(0).any():
                    album_payload = deezer_album_payload(session, int(album_id), deezer_album_cache)
                    genre_data = album_payload.get("genres", {}).get("data", [])
                    official_raw = nonempty(genre_data[0].get("name", "")) if genre_data else ""
                    if official_raw:
                        out.loc[mask, "genre_official_raw"] = official_raw
                        out.loc[mask, "genre"] = normalize_official_genre(official_raw)
                        out.loc[mask, "genre_source"] = "deezer_album_genre"
                        out.loc[mask, "genre_is_official"] = 1

        if idx % 50 == 0 or idx == len(artist_groups):
            save_json(ITUNES_CACHE, itunes_cache)
            save_json(DEEZER_SEARCH_CACHE, deezer_search_cache)
            save_json(DEEZER_TRACK_CACHE, deezer_track_cache)
            save_json(DEEZER_ALBUM_CACHE, deezer_album_cache)
            save_json(PREVIEW_BPM_CACHE, preview_bpm_cache)
            remaining_bpm = int((pd.to_numeric(out["bpm"], errors="coerce").isna() | pd.to_numeric(out["bpm"], errors="coerce").eq(0)).sum())
            remaining_genre = int(out["genre"].fillna("").astype(str).str.strip().eq("").sum())
            log(f"Artist pass progress {idx}/{len(artist_groups)}; remaining bpm rows={remaining_bpm}, remaining empty genre rows={remaining_genre}")

    return out


def targeted_song_fill(
    df: pd.DataFrame,
    session: requests.Session,
    itunes_exact_cache: Dict[str, list],
    deezer_search_cache: Dict[str, list],
    deezer_track_cache: Dict[str, dict],
    deezer_album_cache: Dict[str, dict],
    discogs_cache: Dict[str, dict],
    preview_bpm_cache: Dict[str, object],
    *,
    genre_gap_only: bool = False,
    bpm_gap_only: bool = False,
    chunk_start: int = 0,
    chunk_size: int = 0,
) -> pd.DataFrame:
    out = df.copy()
    if genre_gap_only and bpm_gap_only:
        raise ValueError("genre_gap_only and bpm_gap_only cannot both be true")
    if genre_gap_only:
        target_mask = out["genre"].fillna("").astype(str).str.strip().eq("")
    elif bpm_gap_only:
        target_mask = pd.to_numeric(out["bpm"], errors="coerce").isna() | pd.to_numeric(out["bpm"], errors="coerce").eq(0)
    else:
        target_mask = (
            (pd.to_numeric(out["bpm"], errors="coerce").isna() | pd.to_numeric(out["bpm"], errors="coerce").eq(0))
            | out["genre"].fillna("").astype(str).str.strip().eq("")
            | out["genre_source"].fillna("").astype(str).str.strip().isin(["ug_selected", "ug_alternative_local", "artist_metadata_fallback"])
        )
    target_rows = out.loc[target_mask, ["artist_name", "song_name"]].drop_duplicates()
    total_targets = len(target_rows)
    if chunk_start or chunk_size:
        start = max(0, int(chunk_start))
        stop = len(target_rows) if not chunk_size or int(chunk_size) <= 0 else min(len(target_rows), start + int(chunk_size))
        target_rows = target_rows.iloc[start:stop].copy()
        log(f"Targeted song pass chunk selected: start={start}, stop={stop}, size={len(target_rows)}, total_eligible={total_targets}")
    else:
        start = 0

    for idx, row in enumerate(target_rows.itertuples(index=False), start=1):
        artist = row.artist_name
        song = row.song_name
        mask = out["artist_name"].eq(artist) & out["song_name"].eq(song)

        song_bpm_missing = pd.to_numeric(out.loc[mask, "bpm"], errors="coerce").isna().any() or pd.to_numeric(out.loc[mask, "bpm"], errors="coerce").eq(0).any()
        song_genre_missing = out.loc[mask, "genre"].fillna("").astype(str).str.strip().eq("").any()
        song_needs_official_genre = out.loc[mask, "genre_source"].fillna("").astype(str).str.strip().isin(["", "ug_selected", "ug_alternative_local", "artist_metadata_fallback"]).any()
        if not song_bpm_missing and not song_genre_missing and not song_needs_official_genre:
            continue

        itunes_results = itunes_song_query(session, artist, song, itunes_exact_cache)
        for item in itunes_results:
            if not final_builder.artist_match(artist, item.get("artistName", "")):
                continue
            if not exact_title_match(item.get("trackName", ""), song):
                continue
            official_raw = nonempty(item.get("primaryGenreName", ""))
            if official_raw and song_needs_official_genre:
                out.loc[mask, "genre_official_raw"] = official_raw
                out.loc[mask, "genre"] = normalize_official_genre(official_raw)
                out.loc[mask, "genre_source"] = "apple_music_track"
                out.loc[mask, "genre_is_official"] = 1
                song_genre_missing = False
                song_needs_official_genre = False
            if song_bpm_missing:
                preview_url = nonempty(item.get("previewUrl", ""))
                if preview_url:
                    estimated_bpm = estimate_bpm_from_preview(session, preview_url, preview_bpm_cache)
                    if estimated_bpm:
                        out.loc[mask & pd.to_numeric(out["bpm"], errors="coerce").isna(), "bpm"] = estimated_bpm
                        out.loc[mask & out["bpm_source"].fillna("").astype(str).str.strip().eq(""), "bpm_source"] = "apple_preview_estimated"
                        song_bpm_missing = False
            if not song_bpm_missing and not song_genre_missing and not song_needs_official_genre:
                break

        deezer_found = False
        cache_key = canonical_key(artist, song)
        if cache_key in deezer_search_cache:
            search_results = deezer_search_cache[cache_key]
        else:
            results: List[dict] = []
            try:
                response = session.get(
                    DEEZER_TRACK_SEARCH_URL,
                    params={"q": f'artist:"{artist}" track:"{song}"', "limit": 10},
                    timeout=20,
                )
                response.raise_for_status()
                results = response.json().get("data", [])
            except Exception as exc:
                log(f"Deezer targeted song query failed for {artist} / {song}: {exc}")
            deezer_search_cache[cache_key] = results
            search_results = results

        for item in search_results:
            if not final_builder.artist_match(artist, item.get("artist", {}).get("name", "")):
                continue
            if not exact_title_match(item.get("title", ""), song):
                continue
            track_payload = deezer_track_payload(session, int(item.get("id")), deezer_track_cache) if item.get("id") else {}
            bpm_value = pd.to_numeric(track_payload.get("bpm"), errors="coerce")
            if pd.notna(bpm_value) and float(bpm_value) > 0:
                out.loc[mask & pd.to_numeric(out["bpm"], errors="coerce").isna(), "bpm"] = int(float(bpm_value))
                out.loc[mask & out["bpm_source"].fillna("").astype(str).str.strip().eq(""), "bpm_source"] = "deezer_track"
                deezer_found = True
                song_bpm_missing = False
            elif song_bpm_missing:
                preview_url = nonempty(track_payload.get("preview", "")) or nonempty(item.get("preview", ""))
                if preview_url:
                    estimated_bpm = estimate_bpm_from_preview(session, preview_url, preview_bpm_cache)
                    if estimated_bpm:
                        out.loc[mask & pd.to_numeric(out["bpm"], errors="coerce").isna(), "bpm"] = estimated_bpm
                        out.loc[mask & out["bpm_source"].fillna("").astype(str).str.strip().eq(""), "bpm_source"] = "deezer_preview_estimated"
                        deezer_found = True
                        song_bpm_missing = False
            album_id = track_payload.get("album", {}).get("id") or item.get("album", {}).get("id")
            if album_id and (song_genre_missing or song_needs_official_genre):
                album_payload = deezer_album_payload(session, int(album_id), deezer_album_cache)
                genre_data = album_payload.get("genres", {}).get("data", [])
                official_raw = nonempty(genre_data[0].get("name", "")) if genre_data else ""
                if official_raw:
                    out.loc[mask, "genre_official_raw"] = official_raw
                    out.loc[mask, "genre"] = normalize_official_genre(official_raw)
                    out.loc[mask, "genre_source"] = "deezer_album_genre"
                    out.loc[mask, "genre_is_official"] = 1
                    song_genre_missing = False
                    song_needs_official_genre = False
            if deezer_found and not song_genre_missing and not song_needs_official_genre:
                break

        if song_genre_missing or song_needs_official_genre:
            discogs_payload = discogs_song_query(session, artist, song, discogs_cache)
            official_raw = nonempty(discogs_payload.get("genre", ""))
            if official_raw:
                out.loc[mask, "genre_official_raw"] = official_raw
                out.loc[mask, "genre"] = normalize_official_genre(official_raw)
                out.loc[mask, "genre_source"] = "discogs_release"
                out.loc[mask, "genre_is_official"] = 1

        if idx % 100 == 0 or idx == len(target_rows):
            save_json(ITUNES_EXACT_CACHE, itunes_exact_cache)
            save_json(DEEZER_SEARCH_CACHE, deezer_search_cache)
            save_json(DEEZER_TRACK_CACHE, deezer_track_cache)
            save_json(DEEZER_ALBUM_CACHE, deezer_album_cache)
            save_json(DISCOGS_CACHE, discogs_cache)
            save_json(PREVIEW_BPM_CACHE, preview_bpm_cache)
            if chunk_start or chunk_size:
                log(f"Targeted song pass progress {idx}/{len(target_rows)} within chunk; global position up to {start + idx}/{total_targets}")
            else:
                log(f"Targeted song pass progress {idx}/{len(target_rows)}")

    return out


def local_ug_fill(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    target_rows = out[
        (pd.to_numeric(out["bpm"], errors="coerce").isna() | pd.to_numeric(out["bpm"], errors="coerce").eq(0))
        | out["genre"].fillna("").astype(str).str.strip().eq("")
    ][["artist_name", "song_name"]].drop_duplicates()
    target_pairs = {canonical_key(row.artist_name, row.song_name) for row in target_rows.itertuples(index=False)}
    local_lookup = build_local_ug_lookup(target_pairs)

    for row in target_rows.itertuples(index=False):
        key = canonical_key(row.artist_name, row.song_name)
        if key not in local_lookup:
            continue
        payload = local_lookup[key]
        mask = out["artist_name"].eq(row.artist_name) & out["song_name"].eq(row.song_name)
        bpm_value = pd.to_numeric(payload.get("bpm"), errors="coerce")
        if pd.notna(bpm_value) and float(bpm_value) > 0:
            out.loc[mask & pd.to_numeric(out["bpm"], errors="coerce").isna(), "bpm"] = int(float(bpm_value))
            out.loc[mask & out["bpm_source"].fillna("").astype(str).str.strip().eq(""), "bpm_source"] = "ug_alternative_local"
        genre_raw = nonempty(payload.get("genre", ""))
        if genre_raw and out.loc[mask, "genre"].fillna("").astype(str).str.strip().eq("").any():
            out.loc[mask, "genre"] = normalize_official_genre(genre_raw)
            out.loc[mask, "genre_source"] = "ug_alternative_local"
            out.loc[mask, "genre_is_official"] = 0
    return out


def save_outputs(df: pd.DataFrame) -> None:
    df.to_csv(TARGET_CSV, index=False, encoding="utf-8")

    stata_df = df.copy()
    variable_labels = {}
    if TARGET_DTA.exists():
        try:
            import pyreadstat

            _, meta = pyreadstat.read_dta(TARGET_DTA, metadataonly=True)
            variable_labels = dict(meta.column_labels or {})
        except Exception:
            variable_labels = {}
    variable_labels.update(NUMERIC_LABELS)
    variable_labels.update(EXTRA_LABELS)

    for col in stata_df.columns:
        if pd.api.types.is_numeric_dtype(stata_df[col]):
            continue
        stata_df[col] = stata_df[col].astype("string").fillna("")
    stata_df.to_stata(TARGET_DTA, write_index=False, version=118, variable_labels={k: v[:80] for k, v in variable_labels.items() if k in stata_df.columns})


def summary(df_before: pd.DataFrame, df_after: pd.DataFrame) -> dict:
    bpm_before = int((pd.to_numeric(df_before["bpm"], errors="coerce").isna() | pd.to_numeric(df_before["bpm"], errors="coerce").eq(0)).sum())
    bpm_after = int((pd.to_numeric(df_after["bpm"], errors="coerce").isna() | pd.to_numeric(df_after["bpm"], errors="coerce").eq(0)).sum())
    genre_before = int(df_before["genre"].fillna("").astype(str).str.strip().eq("").sum())
    genre_after = int(df_after["genre"].fillna("").astype(str).str.strip().eq("").sum())
    return {
        "rows": int(len(df_after)),
        "bpm_missing_before": bpm_before,
        "bpm_missing_after": bpm_after,
        "genre_missing_before": genre_before,
        "genre_missing_after": genre_after,
        "genre_source_counts": df_after["genre_source"].fillna("").astype(str).value_counts(dropna=False).to_dict(),
        "bpm_source_counts": df_after["bpm_source"].fillna("").astype(str).value_counts(dropna=False).to_dict(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill BPM and official genre metadata in the country-only final song dataset.")
    parser.add_argument("--skip-targeted-song-pass", action="store_true", help="Stop after local and artist-level backfills.")
    parser.add_argument("--cache-only", action="store_true", help="Apply only what is already available in local caches.")
    parser.add_argument("--skip-artist-pass", action="store_true", help="Skip the artist-level online pass and run only local plus targeted song matching.")
    parser.add_argument("--skip-preview-estimation", action="store_true", help="Disable BPM estimation from preview audio and use only directly observed BPM metadata.")
    parser.add_argument("--genre-gap-only", action="store_true", help="Run the targeted pass only on songs whose final genre is still empty.")
    parser.add_argument("--bpm-gap-only", action="store_true", help="Run the targeted pass only on songs whose BPM is still missing or zero.")
    parser.add_argument("--chunk-start", type=int, default=0, help="Zero-based start index for the targeted song pass.")
    parser.add_argument("--chunk-size", type=int, default=0, help="Maximum number of targeted songs to process in this run.")
    args = parser.parse_args()

    global CACHE_ONLY, ENABLE_PREVIEW_ESTIMATION
    CACHE_ONLY = args.cache_only
    ENABLE_PREVIEW_ESTIMATION = not args.skip_preview_estimation

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(TARGET_CSV, low_memory=False)
    before = df.copy()
    df = initialize_columns(df)
    df = set_existing_sources(df)
    df = local_ug_fill(df)
    df = fill_genre_from_artist_metadata(df)

    session = requests.Session()
    session.headers.update({"User-Agent": final_builder.USER_AGENT})

    itunes_cache = load_json(ITUNES_CACHE, {})
    itunes_exact_cache = load_json(ITUNES_EXACT_CACHE, {})
    deezer_search_cache = load_json(DEEZER_SEARCH_CACHE, {})
    deezer_track_cache = load_json(DEEZER_TRACK_CACHE, {})
    deezer_album_cache = load_json(DEEZER_ALBUM_CACHE, {})
    discogs_cache = load_json(DISCOGS_CACHE, {})
    preview_bpm_cache = load_json(PREVIEW_BPM_CACHE, {})

    if not args.skip_artist_pass:
        df = artist_search_fill(df, session, itunes_cache, deezer_search_cache, deezer_track_cache, deezer_album_cache, preview_bpm_cache)
    if not args.skip_targeted_song_pass:
        df = targeted_song_fill(
            df,
            session,
            itunes_exact_cache,
            deezer_search_cache,
            deezer_track_cache,
            deezer_album_cache,
            discogs_cache,
            preview_bpm_cache,
            genre_gap_only=args.genre_gap_only,
            bpm_gap_only=args.bpm_gap_only,
            chunk_start=args.chunk_start,
            chunk_size=args.chunk_size,
        )

    save_json(ITUNES_CACHE, itunes_cache)
    save_json(ITUNES_EXACT_CACHE, itunes_exact_cache)
    save_json(DEEZER_SEARCH_CACHE, deezer_search_cache)
    save_json(DEEZER_TRACK_CACHE, deezer_track_cache)
    save_json(DEEZER_ALBUM_CACHE, deezer_album_cache)
    save_json(DISCOGS_CACHE, discogs_cache)
    save_json(PREVIEW_BPM_CACHE, preview_bpm_cache)

    save_outputs(df)
    payload = summary(before, df)
    save_json(SUMMARY_JSON, payload)
    log(f"Summary: {payload}")


if __name__ == "__main__":
    main()
