import argparse
import json
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from execution.phase_01_dataset_construction.music_indices import calculate_indices


BASE_DIR = Path("data/phase_01_dataset_construction/processed/country_artists")
INTERMEDIATE_DIR = BASE_DIR / "intermediate"
JSON_CACHE_DIR = INTERMEDIATE_DIR / "json_caches"

V6_CSV = BASE_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
V6_DTA = BASE_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.dta"
ARTIST_UNIVERSE_CSV = BASE_DIR / "artist_universe_country_only.csv"
DISCOVERY_JSON = Path("data/phase_01_dataset_construction/intermediate/json/ug_country_only_chords_discovery.json")
RAW_CHORDS_DIR = Path("data/phase_01_dataset_construction/raw/ultimate_guitar_country_chords")
RAW_CHORD_INDEX: Optional[Dict[int, Path]] = None
DEEZER_DISABLED = False
DEEZER_FAILURES = 0
ITUNES_DISABLED = False
ITUNES_FAILURES = 0

OUTPUT_STEM = "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08"
OUTPUT_CSV = BASE_DIR / f"{OUTPUT_STEM}.csv"
OUTPUT_DTA = BASE_DIR / f"{OUTPUT_STEM}.dta"
NEW_SONGS_CSV = INTERMEDIATE_DIR / "country_only_chords_new_songs_2026_04_08.csv"
CHART_CACHE_CSV = INTERMEDIATE_DIR / "billboard_country_year_end_songs_1946_2025.csv"
CHART_CACHE_JSON = INTERMEDIATE_DIR / "billboard_country_year_end_songs_1946_2025.json"
SUPPLEMENTAL_CHART_CSV = INTERMEDIATE_DIR / "billboard_country_number_ones_wikipedia_1946_2025.csv"
RELEASE_CACHE_FILE = JSON_CACHE_DIR / "release_years_cache_country_only_chords_final_2026_04_08.json"
ARTIST_RELEASE_TRACK_CACHE_FILE = JSON_CACHE_DIR / "artist_release_track_cache_country_only_2026_04_08.json"

HISTORICAL_CACHE_FILES = [
    JSON_CACHE_DIR / "release_years_cache.json",
    JSON_CACHE_DIR / "release_years_cache_wiki.json",
    JSON_CACHE_DIR / "release_years_cache_discogs.json",
    JSON_CACHE_DIR / "release_years_cache_fuzzy.json",
    JSON_CACHE_DIR / "release_years_cache_internet.json",
    JSON_CACHE_DIR / "release_years_cache_restricted_final_v6.json",
]

USER_AGENT = "SoundOfCultureCountryOnlyChords/1.0 (research pipeline; local run)"
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
DEEZER_TRACK_SEARCH_URL = "https://api.deezer.com/search/track"
DEEZER_ALBUM_URL = "https://api.deezer.com/album"
DISCOGS_SEARCH_URL = "https://api.discogs.com/database/search"
MB_SEARCH_URL = "https://musicbrainz.org/ws/2/recording/"
MB_RELEASE_SEARCH_URL = "https://musicbrainz.org/ws/2/release/"

SONG_COLUMNS = [
    "song_name",
    "artist_name",
    "id",
    "type",
    "version",
    "votes",
    "rating",
    "difficulty",
    "tuning",
    "capo",
    "url_web",
    "upload_date",
    "upload_year",
    "genre",
    "main_key",
    "bpm",
    "song_structure",
    "has_intro",
    "has_verse",
    "has_chorus",
    "has_bridge",
    "has_outro",
    "chord_1",
    "chord_1_count",
    "chord_2",
    "chord_2_count",
    "chord_3",
    "chord_3_count",
    "complexity",
    "repetition",
    "melodicness",
    "energy",
    "finger_movement",
    "disruption",
    "root_stability",
    "intra_root_variation",
    "harmonic_palette",
    "loop_strength",
    "structure_variation",
    "playability",
    "harmonic_softness",
    "release_year",
    "bpm_sections",
    "strumming_patterns",
    "num_chord_json_used",
]

CHART_COLUMNS = [
    "billboard_country_year_end_flag",
    "billboard_country_year_end_year",
    "billboard_country_year_end_pos",
]

FINAL_EXTRA_COLUMNS = ["num_chord_json_used"]
ALL_SONG_LEVEL_COLUMNS = set(SONG_COLUMNS + CHART_COLUMNS + FINAL_EXTRA_COLUMNS)

STRING_LABELS = {
    "song_name": "Song title",
    "artist_name": "Artist name",
    "id": "Ultimate Guitar tab ID",
    "type": "Tab type",
    "version": "UG tab version",
    "votes": "Number of UG votes",
    "rating": "Average UG rating",
    "difficulty": "UG difficulty tag",
    "tuning": "Instrument tuning",
    "capo": "Capo position",
    "url_web": "Ultimate Guitar URL",
    "upload_date": "UG upload date",
    "upload_year": "UG upload year",
    "genre": "UG genre tag",
    "main_key": "Estimated musical key",
    "bpm": "Estimated tempo in BPM",
    "song_structure": "Parsed section structure",
    "has_intro": "Has intro section",
    "has_verse": "Has verse section",
    "has_chorus": "Has chorus section",
    "has_bridge": "Has bridge section",
    "has_outro": "Has outro section",
    "chord_1": "Most frequent chord",
    "chord_1_count": "Count of most frequent chord",
    "chord_2": "Second most frequent chord",
    "chord_2_count": "Count of second chord",
    "chord_3": "Third most frequent chord",
    "chord_3_count": "Count of third chord",
    "complexity": "Chord complexity index",
    "repetition": "Chord repetition index",
    "melodicness": "Soft consonance ratio",
    "energy": "Chord-switch energy index",
    "finger_movement": "Chord movement proxy",
    "disruption": "Harmonic disruption index",
    "root_stability": "Consecutive same-root share",
    "intra_root_variation": "Variation within same root",
    "harmonic_palette": "Unique chord palette",
    "loop_strength": "Repeating four-chord strength",
    "structure_variation": "Variation across sections",
    "playability": "Ease of playing proxy",
    "harmonic_softness": "Soft chord ratio",
    "release_year": "Song release year",
    "bpm_sections": "BPM values by song section from UG strumming JSON",
    "strumming_patterns": "UG strumming measures and metadata by section",
    "num_chord_json_used": "Number of chord JSON files used to build song row",
    "billboard_country_year_end_flag": "Song in Billboard country year-end chart",
    "billboard_country_year_end_year": "Year of Billboard country year-end entry",
    "billboard_country_year_end_pos": "Position in Billboard country year-end chart",
}


def log(message: str) -> None:
    print(f"[country-only-chords] {message}", flush=True)


def norm_text(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = text.lower()
    text = "".join(ch if ch.isalnum() else " " for ch in text)
    return " ".join(text.split())


def normalize_space(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    return re.sub(r"\s+", " ", text).strip()


def clean_chart_song_title(value: object) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"\([^)]*\)", "", text)
    text = text.replace('"', " ").replace("“", " ").replace("”", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def simplify_chart_artist(value: object) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    for pattern in [
        r"\s+feat(?:\.|uring)?\s+.+$",
        r"\s+duet with\s+.+$",
        r"\s+and his\s+.+$",
        r"\s+with his\s+.+$",
        r"\s+and the\s+.+$",
        r"\s+with the\s+.+$",
    ]:
        text = re.sub(pattern, "", text, flags=re.I).strip(" ,;-")
    return normalize_space(text)


def clean_for_search(text: object) -> str:
    text = normalize_space(text)
    text = text.replace("&", " and ")
    text = text.replace("+", " and ")
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\[[^]]*\]", "", text)
    text = re.sub(r"\b(chords?|tabs?|acoustic|live|remix|cover|official|version|ver)\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^A-Za-z0-9' -]+", " ", text)
    return normalize_space(text)


def maybe_fix_mojibake(text: object) -> str:
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


def add_contraction_variants(text: object) -> List[str]:
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


def normalize_key(text: object) -> str:
    return re.sub(r"[^a-z0-9]", "", clean_for_search(maybe_fix_mojibake(text)).lower())


def canonical_song_key(artist: object, song: object) -> Tuple[str, str]:
    return norm_text(artist), norm_text(song)


def canonical_release_key(artist: object, song: object) -> str:
    return f"{normalize_space(artist)} - {normalize_space(song)}"


def extract_year_candidates(text: object) -> List[int]:
    years = []
    for match in re.finditer(r"\b(19\d{2}|20[0-2]\d|2030)\b", normalize_space(text)):
        year = int(match.group(1))
        if is_valid_year(year):
            years.append(year)
    return years


def build_target_song_lookup(target_songs: Dict[str, str]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for song_norm, song_title in target_songs.items():
        variants = {song_norm, norm_text(song_title), normalize_key(song_title)}
        for variant in add_contraction_variants(song_title):
            variants.add(norm_text(variant))
            variants.add(normalize_key(variant))
        for variant in variants:
            variant = normalize_space(variant)
            if variant and variant not in lookup:
                lookup[variant] = song_norm
    return lookup


def resolve_target_song_norm(title: object, target_lookup: Dict[str, str], target_songs: Dict[str, str]) -> str:
    title_norm = norm_text(title)
    title_key = normalize_key(title)

    for candidate in [title_norm, title_key]:
        if candidate and candidate in target_lookup:
            return target_lookup[candidate]

    for candidate, canonical in target_lookup.items():
        if not candidate:
            continue
        if len(candidate) >= 10 and candidate in title_key:
            return canonical
        if len(title_key) >= 10 and title_key in candidate:
            return canonical

    for canonical_norm, song_title in target_songs.items():
        song_key = normalize_key(song_title)
        if not song_key or not title_key:
            continue
        if song_key == title_key:
            return canonical_norm
        if len(song_key) >= 10 and song_key in title_key:
            return canonical_norm
        if len(title_key) >= 10 and title_key in song_key:
            return canonical_norm
    return ""


def is_valid_year(value: object) -> bool:
    try:
        year = int(float(value))
    except (TypeError, ValueError):
        return False
    return 1900 <= year <= 2030


def is_missing_value(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str):
        return normalize_space(value).lower() in {"", "nan", "none"}
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def is_missing_numeric(value: object) -> bool:
    if is_missing_value(value):
        return True
    if isinstance(value, (int, float)):
        return float(value) == 0.0
    return False


def parse_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    # Skip effectively empty or corrupted 2-byte files
    if path.stat().st_size <= 2:
        return None
    try:
        with path.open() as handle:
            data = json.load(handle)
            if not isinstance(data, dict) or not data:
                return None
            # Basic validation: must have some content or a song_id
            if "content" not in data and "song_id" not in data:
                return None
            return data
    except Exception as exc:
        log(f"Could not read {path.name}: {exc}")
        return None


def load_discovery() -> List[dict]:
    with DISCOVERY_JSON.open() as handle:
        return json.load(handle)


def build_raw_chord_index() -> Dict[int, Path]:
    index: Dict[int, Path] = {}
    for path in RAW_CHORDS_DIR.glob("*.json"):
        match = re.search(r"_(\d+)\.json$", path.name)
        if not match:
            continue
        tab_id = int(match.group(1))
        if tab_id not in index:
            index[tab_id] = path
    return index


def raw_json_path(tab_id: int) -> Optional[Path]:
    global RAW_CHORD_INDEX
    if RAW_CHORD_INDEX is None:
        RAW_CHORD_INDEX = build_raw_chord_index()
        log(f"Indexed {len(RAW_CHORD_INDEX)} raw chord JSON files")
    return RAW_CHORD_INDEX.get(tab_id)


def tab_sort_key(row: dict) -> Tuple[float, int, int]:
    rating = float(row.get("rating") or 0.0)
    votes = int(row.get("votes") or 0)
    return (rating, votes, int(row.get("id") or 0))


def deep_backfill(base: dict, donor: dict) -> dict:
    for key, donor_value in donor.items():
        if key not in base or is_missing_value(base[key]):
            base[key] = donor_value
            continue
        if isinstance(base[key], dict) and isinstance(donor_value, dict):
            deep_backfill(base[key], donor_value)
        elif isinstance(base[key], list) and isinstance(donor_value, list) and len(base[key]) == 0 and len(donor_value) > 0:
            base[key] = donor_value
    return base


def extract_strumming_metadata(strumming_data: object) -> Tuple[Optional[float], str, str]:
    """Extract total BPM and section-level strumming metadata from UG JSON."""
    if not isinstance(strumming_data, list):
        return None, "", ""

    overall_bpm: Optional[float] = None
    bpm_sections: List[dict] = []
    strumming_sections: List[dict] = []

    for section_index, section in enumerate(strumming_data, start=1):
        if not isinstance(section, dict):
            continue
        part = normalize_space(section.get("part")) or "General"
        raw_bpm = pd.to_numeric(section.get("bpm"), errors="coerce")
        bpm_value = float(raw_bpm) if pd.notna(raw_bpm) and float(raw_bpm) > 0 else None
        if bpm_value is not None and overall_bpm is None:
            overall_bpm = bpm_value
        if bpm_value is not None:
            bpm_sections.append(
                {
                    "section_index": section_index,
                    "part": part,
                    "bpm": int(bpm_value) if bpm_value.is_integer() else bpm_value,
                }
            )

        measure_values: List[int] = []
        measures = section.get("measures")
        if isinstance(measures, list):
            for measure in measures:
                if isinstance(measure, dict):
                    value = pd.to_numeric(measure.get("measure"), errors="coerce")
                else:
                    value = pd.to_numeric(measure, errors="coerce")
                if pd.notna(value):
                    measure_values.append(int(value))

        denominator = pd.to_numeric(section.get("denuminator", section.get("denominator")), errors="coerce")
        strumming_sections.append(
            {
                "section_index": section_index,
                "part": part,
                "bpm": int(bpm_value) if bpm_value is not None and bpm_value.is_integer() else bpm_value,
                "denuminator": int(denominator) if pd.notna(denominator) else None,
                "is_triplet": int(section.get("is_triplet") or 0),
                "measure_count": len(measure_values),
                "measures": measure_values,
            }
        )

    return (
        overall_bpm,
        json.dumps(bpm_sections, ensure_ascii=False, separators=(",", ":")) if bpm_sections else "",
        json.dumps(strumming_sections, ensure_ascii=False, separators=(",", ":")) if strumming_sections else "",
    )


def build_enriched_payload(version_rows: List[dict]) -> Tuple[dict, dict, int]:
    # Preliminary path check
    valid_candidates = []
    for row in version_rows:
        path = raw_json_path(int(row["id"]))
        if path and path.exists():
            valid_candidates.append(row)
    
    if not valid_candidates:
        raise FileNotFoundError(f"No raw JSON found for versions {[row['id'] for row in version_rows]}")

    # Re-sort with a bias towards versions that likely have more data
    def quality_sort_key(row: dict) -> Tuple[int, float, int, int]:
        # We prefer versions that aren't 'ver 1' if higher versions exist,
        # but primarily we use the UG rating and votes.
        # We also check if we've already cached meta about strumming (if we had an index)
        # but here we'll just use the standard sort and then parse.
        rating = float(row.get("rating") or 0.0)
        votes = int(row.get("votes") or 0)
        return (rating >= 4.5, rating, votes, int(row.get("id") or 0))

    ordered_rows = sorted(valid_candidates, key=quality_sort_key, reverse=True)
    
    payloads = []
    for row in ordered_rows:
        path = raw_json_path(int(row["id"]))
        payload = parse_json(path)
        if payload:
            # Check if this payload has the specific 'strumming' or 'tonality' info we want
            has_rich_meta = 1 if (payload.get("strumming") or payload.get("tonality_name")) else 0
            payloads.append((row, payload, has_rich_meta))

    if not payloads:
        raise FileNotFoundError(f"All raw JSON for versions {[row['id'] for row in ordered_rows]} were corrupted or empty")

    # Re-sort payloads to put those with rich metadata FIRST if ratings are similar
    payloads.sort(key=lambda x: (x[2], x[0].get("rating", 0), x[0].get("votes", 0)), reverse=True)

    base_row, base_payload, _ = payloads[0]
    merged_payload = deepcopy(base_payload)
    
    # Merge up to 3 versions to backfill missing fields
    payloads_used = payloads[:3]
    for _, donor_payload, _ in payloads_used[1:]:
        merged_payload = deep_backfill(merged_payload, donor_payload)

    # Keep ranking metadata from the top-rated version (the original choice).
    merged_payload["id"] = int(base_row["id"])
    merged_payload["rating"] = base_row.get("rating", merged_payload.get("rating"))
    merged_payload["votes"] = base_row.get("votes", merged_payload.get("votes"))
    merged_payload["version"] = merged_payload.get("version", base_payload.get("version"))
    merged_payload["artist_name"] = normalize_space(base_payload.get("artist_name") or merged_payload.get("artist_name") or base_row.get("target_artist_name") or base_row.get("artist_name"))
    merged_payload["song_name"] = normalize_space(base_row.get("song_name") or merged_payload.get("song_name"))
    merged_payload["type"] = "Chords"

    return base_row, merged_payload, len(payloads_used)


def flatten_chord_payload(base_row: dict, data: dict, num_json_used: int | None = None) -> dict:
    row = {
        "song_name": normalize_space(data.get("song_name") or base_row.get("song_name")),
        "artist_name": normalize_space(data.get("artist_name") or base_row.get("target_artist_name") or base_row.get("artist_name")),
        "id": int(base_row["id"]),
        "type": "Chords",
        "version": data.get("version"),
        "votes": int(base_row.get("votes") or data.get("votes") or 0),
        "rating": float(base_row.get("rating") or data.get("rating") or 0.0),
        "difficulty": data.get("difficulty"),
        "tuning": data.get("tuning"),
        "capo": data.get("capo"),
        "url_web": data.get("urlWeb"),
    }

    timestamp = data.get("date")
    upload_date = ""
    upload_year = None
    if timestamp:
        try:
            date_obj = datetime.fromtimestamp(int(timestamp))
            upload_date = date_obj.strftime("%Y-%m-%d")
            upload_year = date_obj.year
        except Exception:
            pass
    row["upload_date"] = upload_date
    row["upload_year"] = upload_year
    row["genre"] = data.get("advertising", {}).get("targeting_song_genre", "country") or "country"

    content = data.get("content", "") or ""
    strumming_data = data.get("strumming", []) or []
    row["main_key"] = data.get("tonality_name", "")

    overall_bpm, bpm_sections_json, strumming_patterns_json = extract_strumming_metadata(strumming_data)
    row["bpm"] = overall_bpm
    row["bpm_sections"] = bpm_sections_json
    row["strumming_patterns"] = strumming_patterns_json

    # Structural indicators
    content_parts = re.split(r"\[(?!ch|/ch|tab|/tab)([^\]]+)\]", content)
    sections_list = []
    if len(content_parts) > 1:
        for index in range(1, len(content_parts), 2):
            sections_list.append(content_parts[index].strip())
    row["song_structure"] = ", ".join(sections_list)

    sections_lower = [section.lower() for section in sections_list]

    def has_section(keywords: Iterable[str]) -> int:
        return int(any(any(keyword in section for keyword in keywords) for section in sections_lower))

    row["has_intro"] = has_section(["intro"])
    row["has_verse"] = has_section(["verse"])
    row["has_chorus"] = has_section(["chorus"])
    row["has_bridge"] = has_section(["bridge"])
    row["has_outro"] = has_section(["outro"])

    # Chord extraction
    chord_matches = re.findall(r"\[ch\](.*?)\[/ch\]", content)
    chord_counts = Counter(chord_matches)
    sorted_chords = chord_counts.most_common(3)
    row["chord_1"] = sorted_chords[0][0] if len(sorted_chords) > 0 else ""
    row["chord_1_count"] = sorted_chords[0][1] if len(sorted_chords) > 0 else 0
    row["chord_2"] = sorted_chords[1][0] if len(sorted_chords) > 1 else ""
    row["chord_2_count"] = sorted_chords[1][1] if len(sorted_chords) > 1 else 0
    row["chord_3"] = sorted_chords[2][0] if len(sorted_chords) > 2 else ""
    row["chord_3_count"] = sorted_chords[2][1] if len(sorted_chords) > 2 else 0

    # Musical Indices
    row.update(calculate_indices(data))
    
    row["release_year"] = None
    row["num_chord_json_used"] = int(num_json_used or 1)
    return row


def load_artist_universe() -> pd.DataFrame:
    artist_df = pd.read_csv(ARTIST_UNIVERSE_CSV, low_memory=False)
    return artist_df


def build_artist_metadata_maps(v6_df: pd.DataFrame, artist_df: pd.DataFrame) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    by_id = {}
    by_name = {}

    for row in artist_df.to_dict(orient="records"):
        artist_id = normalize_space(row.get("artist_id"))
        if artist_id:
            by_id[artist_id] = row
        name_key = norm_text(row.get("name_primary"))
        if name_key and name_key not in by_name:
            by_name[name_key] = row

    artist_columns = [column for column in v6_df.columns if column not in SONG_COLUMNS]
    for row in v6_df[artist_columns].drop_duplicates(subset=["artist_id", "name_primary"]).to_dict(orient="records"):
        artist_id = normalize_space(row.get("artist_id"))
        if artist_id and artist_id not in by_id:
            by_id[artist_id] = row
        name_key = norm_text(row.get("name_primary"))
        if name_key and name_key not in by_name:
            by_name[name_key] = row

    return by_id, by_name


def attach_artist_metadata(song_row: dict, base_row: dict, by_id: Dict[str, dict], by_name: Dict[str, dict], output_columns: List[str]) -> dict:
    artist_id = normalize_space(base_row.get("artist_id"))
    metadata = by_id.get(artist_id)
    if metadata is None:
        metadata = by_name.get(norm_text(song_row["artist_name"]))
    if metadata is None:
        metadata = by_name.get(norm_text(base_row.get("target_artist_name")))
    if metadata is None:
        metadata = {}

    full_row = {column: None for column in output_columns}
    for column, value in song_row.items():
        if column in full_row:
            full_row[column] = value
    for column in output_columns:
        if column in ALL_SONG_LEVEL_COLUMNS:
            continue
        full_row[column] = metadata.get(column)

    if not full_row.get("artist_id"):
        full_row["artist_id"] = artist_id or metadata.get("artist_id")
    if not full_row.get("name_primary"):
        full_row["name_primary"] = metadata.get("name_primary") or song_row["artist_name"]

    return full_row


def build_output_columns(reference_columns: List[str]) -> List[str]:
    output_columns = list(reference_columns)
    for column in SONG_COLUMNS + CHART_COLUMNS + FINAL_EXTRA_COLUMNS:
        if column not in output_columns:
            output_columns.append(column)
    return output_columns


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


def deduplicate_new_song_cache(new_df: pd.DataFrame, reference_columns: List[str]) -> pd.DataFrame:
    if new_df.empty:
        return new_df.reindex(columns=reference_columns)
    working = new_df.copy()
    working["_artist_norm"] = working["artist_name"].map(norm_text)
    working["_song_norm"] = working["song_name"].map(norm_text)
    working.sort_values(["rating", "votes", "id"], ascending=[False, False, False], inplace=True)
    before_count = len(working)
    working.drop_duplicates(subset=["id"], keep="first", inplace=True)
    working.drop_duplicates(subset=["_artist_norm", "_song_norm"], keep="first", inplace=True)
    removed = before_count - len(working)
    if removed:
        log(f"Deduplicated cached new-song rows removed: {removed}")
    working.drop(columns=["_artist_norm", "_song_norm"], inplace=True)
    return working.reindex(columns=reference_columns)


def cached_strumming_patterns_need_rebuild(new_df: pd.DataFrame) -> bool:
    """Detect caches built before numeric UG strumming measures were parsed."""
    if "strumming_patterns" not in new_df.columns:
        return True
    sample = new_df["strumming_patterns"].fillna("").astype(str).str.strip()
    sample = sample[sample.ne("")].head(200)
    if sample.empty:
        return False
    for value in sample:
        try:
            sections = json.loads(value)
        except Exception:
            continue
        if not isinstance(sections, list):
            continue
        for section in sections:
            if not isinstance(section, dict):
                continue
            measures = section.get("measures")
            measure_count = pd.to_numeric(section.get("measure_count"), errors="coerce")
            if isinstance(measures, list) and measures:
                return False
            if pd.notna(measure_count) and int(measure_count) > 0:
                return False
    return True


def build_song_json_usage_maps(discovery_rows: List[dict]) -> Tuple[Dict[int, int], Dict[Tuple[str, str], int]]:
    id_to_count: Dict[int, int] = {}
    key_to_count: Dict[Tuple[str, str], int] = {}
    grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)

    for row in discovery_rows:
        key = canonical_song_key(row.get("target_artist_name") or row.get("artist_name"), row.get("song_name"))
        grouped[key].append(row)

    for key, versions in grouped.items():
        available_rows = [row for row in sorted(versions, key=tab_sort_key, reverse=True) if raw_json_path(int(row.get("id") or 0))]
        if not available_rows:
            continue
        count = min(3, len(available_rows))
        key_to_count[key] = count
        for row in versions:
            tab_id = int(row.get("id") or 0)
            if tab_id:
                id_to_count[tab_id] = count

    return id_to_count, key_to_count


def apply_num_json_used(df: pd.DataFrame, discovery_rows: List[dict]) -> pd.DataFrame:
    if "num_chord_json_used" not in df.columns:
        df["num_chord_json_used"] = pd.NA
    id_to_count, key_to_count = build_song_json_usage_maps(discovery_rows)

    for idx in df.index:
        count = None
        try:
            tab_id = int(float(df.at[idx, "id"]))
        except Exception:
            tab_id = None
        if tab_id is not None and tab_id > 0:
            count = id_to_count.get(tab_id)
        if count is None:
            song_key = canonical_song_key(df.at[idx, "artist_name"], df.at[idx, "song_name"])
            count = key_to_count.get(song_key)
        if count is None and pd.notna(df.at[idx, "id"]):
            count = 1
        df.at[idx, "num_chord_json_used"] = int(count) if count else pd.NA
    return df


def artist_ids_with_raw_json(discovery_rows: List[dict]) -> set[str]:
    artist_ids: set[str] = set()
    for row in discovery_rows:
        artist_id = normalize_space(row.get("artist_id"))
        tab_id = int(row.get("id") or 0)
        if artist_id and tab_id and raw_json_path(tab_id):
            artist_ids.add(artist_id)
    return artist_ids


def next_synthetic_id(existing_ids: set[int]) -> int:
    candidate = -1
    while candidate in existing_ids:
        candidate -= 1
    return candidate


def build_artist_bridge_rows(
    discovery_rows: List[dict],
    combined_df: pd.DataFrame,
    v6_df: pd.DataFrame,
    artist_df: pd.DataFrame,
) -> pd.DataFrame:
    output_columns = build_output_columns(list(v6_df.columns))
    by_id, by_name = build_artist_metadata_maps(v6_df, artist_df)
    artist_ids_with_raw = artist_ids_with_raw_json(discovery_rows)
    artist_ids_in_combined = set(combined_df["artist_id"].fillna("").astype(str)) - {""}
    missing_artist_ids = sorted(artist_ids_with_raw - artist_ids_in_combined)
    if not missing_artist_ids:
        return pd.DataFrame(columns=output_columns)

    rows_by_artist: Dict[str, List[dict]] = defaultdict(list)
    for row in discovery_rows:
        artist_id = normalize_space(row.get("artist_id"))
        tab_id = int(row.get("id") or 0)
        if artist_id in missing_artist_ids and tab_id and raw_json_path(tab_id):
            rows_by_artist[artist_id].append(row)

    existing_numeric_ids = set(pd.to_numeric(combined_df["id"], errors="coerce").dropna().astype(int))
    bridge_rows = []
    for artist_id in missing_artist_ids:
        candidates = rows_by_artist.get(artist_id, [])
        if not candidates:
            continue
        ordered_candidates = sorted(candidates, key=tab_sort_key, reverse=True)
        base_row = ordered_candidates[0]
        num_json_used = min(3, len(ordered_candidates))
        try:
            _, payload, num_json_used = build_enriched_payload(candidates)
            song_row = flatten_chord_payload(base_row, payload, num_json_used)
        except FileNotFoundError:
            song_row = {
                "song_name": normalize_space(base_row.get("song_name")),
                "artist_name": normalize_space(base_row.get("artist_name") or base_row.get("target_artist_name")),
                "id": int(base_row.get("id") or 0),
                "type": "Chords",
                "version": None,
                "votes": int(base_row.get("votes") or 0),
                "rating": float(base_row.get("rating") or 0.0),
                "difficulty": None,
                "tuning": None,
                "capo": None,
                "url_web": None,
                "upload_date": None,
                "upload_year": None,
                "genre": None,
                "main_key": None,
                "bpm": None,
                "song_structure": None,
                "has_intro": None,
                "has_verse": None,
                "has_chorus": None,
                "has_bridge": None,
                "has_outro": None,
                "chord_1": None,
                "chord_1_count": None,
                "chord_2": None,
                "chord_2_count": None,
                "chord_3": None,
                "chord_3_count": None,
                "complexity": None,
                "repetition": None,
                "melodicness": None,
                "energy": None,
                "finger_movement": None,
                "disruption": None,
                "root_stability": None,
                "intra_root_variation": None,
                "harmonic_palette": None,
                "loop_strength": None,
                "structure_variation": None,
                "playability": None,
                "harmonic_softness": None,
                "release_year": None,
                "num_chord_json_used": num_json_used,
            }
        full_row = attach_artist_metadata(song_row, base_row, by_id, by_name, output_columns)
        full_row["artist_id"] = artist_id
        metadata = by_id.get(artist_id, {})
        if not normalize_space(full_row.get("name_primary")):
            full_row["name_primary"] = metadata.get("name_primary") or base_row.get("target_artist_name") or song_row["artist_name"]

        synthetic_id = next_synthetic_id(existing_numeric_ids)
        existing_numeric_ids.add(synthetic_id)
        full_row["id"] = synthetic_id
        bridge_rows.append(full_row)

    bridge_df = pd.DataFrame(bridge_rows, columns=output_columns)
    if not bridge_df.empty:
        log(f"Added bridge rows to guarantee artist presence for {len(bridge_df)} artists with raw JSON")
    return bridge_df


def load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        with path.open() as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        log(f"Could not load {path.name}: {exc}")
        return {}


def save_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def save_release_caches(release_cache: Dict[str, object], artist_track_cache: Dict[str, object]) -> None:
    save_json(RELEASE_CACHE_FILE, release_cache)
    save_json(ARTIST_RELEASE_TRACK_CACHE_FILE, artist_track_cache)


def build_master_release_cache(v6_df: pd.DataFrame) -> Dict[str, int]:
    master: Dict[str, int] = {}
    for row in v6_df.itertuples():
        if is_valid_year(getattr(row, "release_year")):
            master[canonical_release_key(row.artist_name, row.song_name)] = int(float(row.release_year))
    for path in HISTORICAL_CACHE_FILES + [RELEASE_CACHE_FILE]:
        payload = load_json(path)
        for key, value in payload.items():
            if is_valid_year(value):
                master[key] = int(float(value))
    return master


def token_signature(text: object) -> str:
    return " ".join(sorted(token for token in clean_for_search(maybe_fix_mojibake(text)).lower().split() if token))


def generate_candidate_pairs(artist: str, song: str) -> Iterable[Tuple[str, str]]:
    artist = clean_for_search(maybe_fix_mojibake(artist))
    song = clean_for_search(maybe_fix_mojibake(song))
    variants = []

    def add_pair(a: str, s: str) -> None:
        key = (normalize_space(a), normalize_space(s))
        if key[0] and key[1] and key not in variants:
            variants.append(key)

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

    for candidate_artist in artist_variants[:4] or [artist]:
        for candidate_song in song_variants[:6] or [song]:
            add_pair(candidate_artist, candidate_song)

    if artist.lower().startswith("the "):
        add_pair(artist[4:], song)
    for reduced in [
        re.sub(r"\s+and\s+his\s+.+$", "", artist, flags=re.I),
        re.sub(r"\s+and\s+the\s+.+$", "", artist, flags=re.I),
        re.sub(r"\s*&\s+.+$", "", artist, flags=re.I),
        re.sub(r"\s+band$", "", artist, flags=re.I),
    ]:
        add_pair(reduced, song)
    if "&" in artist:
        add_pair(artist.replace("&", "and"), song)
    return variants


def year_from_artist_itunes(session: requests.Session, artist: str, target_songs: Dict[str, str]) -> Dict[str, int]:
    global ITUNES_DISABLED, ITUNES_FAILURES
    if ITUNES_DISABLED:
        return {}
    results = []
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
        ITUNES_FAILURES += 1
        if ITUNES_FAILURES >= 3:
            ITUNES_DISABLED = True
            log("Disabling iTunes fallback after repeated failures")
        return {}

    found = {}
    for item in results:
        item_artist = normalize_space(item.get("artistName", ""))
        if normalize_key(artist) not in normalize_key(item_artist) and normalize_key(item_artist) not in normalize_key(artist):
            continue
        release_date = normalize_space(item.get("releaseDate", ""))
        if not is_valid_year(release_date[:4]):
            continue
        item_song_norm = norm_text(item.get("trackName", ""))
        if item_song_norm in target_songs:
            found[item_song_norm] = min(found.get(item_song_norm, 9999), int(release_date[:4]))
    return found


def year_from_artist_deezer(session: requests.Session, artist: str, target_songs: Dict[str, str]) -> Dict[str, int]:
    global DEEZER_DISABLED, DEEZER_FAILURES
    if DEEZER_DISABLED:
        return {}
    try:
        response = session.get(
            DEEZER_TRACK_SEARCH_URL,
            params={"q": f'artist:"{artist}"', "limit": 100},
            timeout=10,
        )
        response.raise_for_status()
        results = response.json().get("data", [])
    except Exception as exc:
        DEEZER_FAILURES += 1
        log(f"Deezer artist query failed for {artist}: {exc}")
        if DEEZER_FAILURES >= 5:
            DEEZER_DISABLED = True
            log("Disabling Deezer fallback after repeated connection failures")
        return {}

    found = {}
    for item in results:
        item_artist = normalize_space(item.get("artist", {}).get("name", ""))
        if normalize_key(artist) not in normalize_key(item_artist) and normalize_key(item_artist) not in normalize_key(artist):
            continue
        release_year = None
        release_date = normalize_space(item.get("release_date", ""))
        if is_valid_year(release_date[:4]):
            release_year = int(release_date[:4])
        else:
            album_id = item.get("album", {}).get("id")
            if album_id:
                try:
                    album_response = session.get(f"{DEEZER_ALBUM_URL}/{album_id}", timeout=30)
                    album_response.raise_for_status()
                    album_date = normalize_space(album_response.json().get("release_date", ""))
                    if is_valid_year(album_date[:4]):
                        release_year = int(album_date[:4])
                except Exception:
                    release_year = None
        if release_year is None:
            continue
        item_song_norm = norm_text(item.get("title", ""))
        if item_song_norm in target_songs:
            found[item_song_norm] = min(found.get(item_song_norm, 9999), release_year)
    return found


def artist_match(candidate_artist: str, observed_artist: str) -> bool:
    candidate_key = normalize_key(candidate_artist)
    observed_key = normalize_key(observed_artist)
    if not candidate_key or not observed_key:
        return False
    return candidate_key in observed_key or observed_key in candidate_key


def year_from_song_itunes_bundle(session: requests.Session, artist: str, seed_song: str, target_songs: Dict[str, str]) -> Dict[str, int]:
    global ITUNES_DISABLED, ITUNES_FAILURES
    if ITUNES_DISABLED:
        return {}
    found: Dict[str, int] = {}
    queries = []
    for candidate_artist, candidate_song in generate_candidate_pairs(artist, seed_song):
        query = normalize_space(f"{candidate_artist} {candidate_song}")
        if query and query not in queries:
            queries.append(query)

    for query in queries[:4]:
        try:
            response = session.get(
                ITUNES_SEARCH_URL,
                params={"term": query, "entity": "song", "limit": 25},
                timeout=20,
            )
            response.raise_for_status()
            results = response.json().get("results", [])
        except Exception as exc:
            ITUNES_FAILURES += 1
            if ITUNES_FAILURES >= 3:
                ITUNES_DISABLED = True
                log(f"Disabling iTunes fallback after repeated failures: {exc}")
            continue

        for item in results:
            item_artist = normalize_space(item.get("artistName", ""))
            if not artist_match(artist, item_artist):
                continue
            item_song_norm = norm_text(item.get("trackName", ""))
            if item_song_norm not in target_songs:
                continue
            release_date = normalize_space(item.get("releaseDate", ""))
            if not is_valid_year(release_date[:4]):
                continue
            found[item_song_norm] = min(found.get(item_song_norm, 9999), int(release_date[:4]))

        if found:
            return found
    return found


def year_from_song_discogs(session: requests.Session, artist: str, song: str) -> Optional[int]:
    try:
        response = session.get(
            DISCOGS_SEARCH_URL,
            params={"q": f"{artist} {song}", "type": "release"},
            timeout=25,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
    except Exception:
        return None

    song_key = normalize_key(song)
    artist_key = normalize_key(artist)
    candidates = []
    for item in results[:12]:
        title_key = normalize_key(item.get("title", ""))
        year = item.get("year")
        if not is_valid_year(year):
            continue
        if song_key and song_key not in title_key and artist_key and artist_key not in title_key:
            continue
        candidates.append(int(float(year)))
    return min(candidates) if candidates else None


def year_from_song_musicbrainz(session: requests.Session, artist: str, song: str, artist_mbid: str = "") -> Optional[int]:
    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        query = f'arid:{artist_mbid} AND recording:"{candidate_song}"' if artist_mbid else f'artist:"{candidate_artist}" AND recording:"{candidate_song}"'
        try:
            response = session.get(
                MB_SEARCH_URL,
                params={
                    "query": query,
                    "fmt": "json",
                    "limit": 10,
                },
                timeout=25,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            continue

        years = []
        for recording in payload.get("recordings", []):
            artist_credit = normalize_space(recording.get("artist-credit-phrase", ""))
            if artist_mbid == "" and not artist_match(candidate_artist, artist_credit):
                continue
            first_release_date = normalize_space(recording.get("first-release-date", ""))
            if is_valid_year(first_release_date[:4]):
                years.append(int(first_release_date[:4]))
            for release in recording.get("releases", []):
                date = normalize_space(release.get("date", ""))
                if is_valid_year(date[:4]):
                    years.append(int(date[:4]))
        if years:
            return min(years)
    return None


def wikipedia_page_wikitext(session: requests.Session, title: str) -> str:
    response = session.get(
        WIKIPEDIA_API_URL,
        params={
            "action": "query",
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "main",
            "titles": title,
            "redirects": 1,
            "format": "json",
        },
        timeout=20,
    )
    response.raise_for_status()
    pages = response.json().get("query", {}).get("pages", {})
    for page in pages.values():
        revisions = page.get("revisions", [])
        if not revisions:
            continue
        return revisions[0].get("slots", {}).get("main", {}).get("*", "")
    return ""


def wikipedia_extract_container_titles(content: str) -> List[str]:
    titles: List[str] = []

    def add_title(value: str) -> None:
        cleaned = normalize_space(value).strip("|").strip()
        cleaned = re.sub(r"^''|''$", "", cleaned)
        cleaned = cleaned.strip("[] ")
        if cleaned and cleaned not in titles:
            titles.append(cleaned)

    for pattern in [
        r"\|\s*(?:album|from album|ep)\s*=\s*(.+?)(?:\n\||$)",
        r"\bfrom (?:the |their |his |her )?(?:album|ep)\s+\[\[([^\]|#]+)",
        r"\bfrom (?:the |their |his |her )?(?:album|ep)\s+''([^']+)''",
    ]:
        for match in re.finditer(pattern, content, flags=re.I | re.S):
            raw = match.group(1)
            wikilinks = re.findall(r"\[\[([^\]|#]+)", raw)
            if wikilinks:
                for title in wikilinks:
                    add_title(title)
            else:
                raw = re.sub(r"<ref[^>]*>.*?</ref>", "", raw, flags=re.I | re.S)
                raw = re.sub(r"\{\{[^{}]*\}\}", " ", raw)
                raw = raw.split("\n", 1)[0]
                raw = raw.split("{{", 1)[0]
                raw = raw.split("<", 1)[0]
                add_title(raw)
    return titles[:5]


def wikipedia_extract_release_year(content: str) -> Optional[int]:
    patterns = [
        r"\|\s*released\s*=\s*(.+?)(?:\n\||$)",
        r"\|\s*published\s*=\s*(.+?)(?:\n\||$)",
        r"\|\s*date\s*=\s*(.+?)(?:\n\||$)",
        r"\breleased(?:\s+as\s+a\s+single)?\b[^.\n]{0,120}\b(19\d{2}|20[0-2]\d|2030)\b",
        r"\bfrom (?:the |their |his |her )?(?:album|ep)\b[^.\n]{0,120}\b(19\d{2}|20[0-2]\d|2030)\b",
    ]
    candidates: List[int] = []
    for pattern in patterns:
        for match in re.finditer(pattern, content, flags=re.I | re.S):
            group_text = match.group(1) if match.groups() else match.group(0)
            candidates.extend(extract_year_candidates(group_text))
    return min(candidates) if candidates else None


def wikipedia_search_song_titles(session: requests.Session, artist: str, song: str) -> List[str]:
    titles: List[str] = []
    queries: List[str] = []
    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        for query in [
            f'"{candidate_song}" "{candidate_artist}" song',
            f'"{candidate_song}" "{candidate_artist}"',
            f'"{candidate_song}" {candidate_artist} album',
        ]:
            query = normalize_space(query)
            if query and query not in queries:
                queries.append(query)

    for query in queries[:6]:
        try:
            search_response = session.get(
                WIKIPEDIA_API_URL,
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": query,
                    "format": "json",
                    "srlimit": 5,
                },
                timeout=20,
            )
            search_response.raise_for_status()
            search_results = search_response.json().get("query", {}).get("search", [])
        except Exception:
            continue
        for item in search_results:
            title = normalize_space(item.get("title", ""))
            if title and title not in titles:
                titles.append(title)
        if titles:
            break
    return titles[:8]


def year_from_song_wikipedia(session: requests.Session, artist: str, song: str) -> Optional[int]:
    for title in wikipedia_search_song_titles(session, artist, song):
        try:
            content = wikipedia_page_wikitext(session, title)
        except Exception:
            continue
        year = wikipedia_extract_release_year(content)
        if is_valid_year(year):
            return int(year)
        for container_title in wikipedia_extract_container_titles(content):
            try:
                container_content = wikipedia_page_wikitext(session, container_title)
            except Exception:
                continue
            year = wikipedia_extract_release_year(container_content)
            if is_valid_year(year):
                return int(year)
        time.sleep(0.5)
    return None


def wikidata_claim_entity_ids(entity: dict, prop: str) -> List[str]:
    ids: List[str] = []
    for claim in entity.get("claims", {}).get(prop, []):
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if isinstance(value, dict) and value.get("id"):
            ids.append(value["id"])
    return ids


def wikidata_claim_years(entity: dict, props: Tuple[str, ...] = ("P577", "P571")) -> List[int]:
    years: List[int] = []
    for prop in props:
        for claim in entity.get("claims", {}).get(prop, []):
            value = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
            if isinstance(value, dict) and "time" in value:
                years.extend(extract_year_candidates(value.get("time", "")))
            else:
                years.extend(extract_year_candidates(value))
    return sorted({year for year in years if is_valid_year(year)})


def wikidata_label(entity: dict) -> str:
    return normalize_space(entity.get("labels", {}).get("en", {}).get("value", ""))


def wikidata_get_entities(session: requests.Session, ids: List[str]) -> Dict[str, dict]:
    cleaned = [qid for qid in ids if qid]
    if not cleaned:
        return {}
    response = session.get(
        WIKIDATA_API_URL,
        params={
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(cleaned[:50]),
            "languages": "en",
            "props": "labels|claims",
        },
        timeout=25,
    )
    response.raise_for_status()
    return response.json().get("entities", {})


def year_from_song_wikidata(session: requests.Session, artist: str, song: str) -> Optional[int]:
    candidate_ids: List[str] = []
    song_key = normalize_key(song)
    for candidate_artist, candidate_song in generate_candidate_pairs(artist, song):
        queries = [normalize_space(f"{candidate_song} {candidate_artist}"), normalize_space(candidate_song)]
        for query in queries[:2]:
            if not query:
                continue
            try:
                response = session.get(
                    WIKIDATA_API_URL,
                    params={
                        "action": "wbsearchentities",
                        "format": "json",
                        "language": "en",
                        "type": "item",
                        "limit": 8,
                        "search": query,
                    },
                    timeout=20,
                )
                response.raise_for_status()
                results = response.json().get("search", [])
            except Exception:
                continue
            for item in results:
                qid = item.get("id", "")
                label = normalize_space(item.get("label", ""))
                description = normalize_space(item.get("description", "")).lower()
                label_key = normalize_key(label)
                if song_key and song_key not in label_key and label_key not in song_key:
                    continue
                if description and not any(token in description for token in ["song", "single", "track", "ballad", "musical work"]):
                    continue
                if qid and qid not in candidate_ids:
                    candidate_ids.append(qid)
            if candidate_ids:
                break
        if candidate_ids:
            break

    if not candidate_ids:
        return None

    try:
        entities = wikidata_get_entities(session, candidate_ids)
    except Exception:
        return None

    related_ids: List[str] = []
    for qid in candidate_ids:
        entity = entities.get(qid, {})
        related_ids.extend(wikidata_claim_entity_ids(entity, "P175"))
        related_ids.extend(wikidata_claim_entity_ids(entity, "P361"))
    related_ids = list(dict.fromkeys(related_ids))
    related_entities: Dict[str, dict] = {}
    if related_ids:
        try:
            related_entities = wikidata_get_entities(session, related_ids)
        except Exception:
            related_entities = {}

    candidate_years: List[int] = []
    for qid in candidate_ids:
        entity = entities.get(qid, {})
        performer_ids = wikidata_claim_entity_ids(entity, "P175")
        if performer_ids:
            performer_labels = [wikidata_label(related_entities.get(pid, {})) for pid in performer_ids]
            if not any(artist_match(artist, label) for label in performer_labels if label):
                continue
        direct_years = wikidata_claim_years(entity)
        if direct_years:
            candidate_years.extend(direct_years)
            continue
        for parent_id in wikidata_claim_entity_ids(entity, "P361"):
            candidate_years.extend(wikidata_claim_years(related_entities.get(parent_id, {})))

    valid_years = sorted({year for year in candidate_years if is_valid_year(year)})
    return valid_years[0] if valid_years else None


def year_from_artist_musicbrainz(session: requests.Session, artist: str, target_songs: Dict[str, str], artist_mbid: str = "", max_pages: int = 15) -> Dict[str, int]:
    found: Dict[str, int] = {}
    target_lookup = build_target_song_lookup(target_songs)
    dynamic_max_pages = max_pages
    if len(target_songs) >= 250:
        dynamic_max_pages = max(dynamic_max_pages, 40)
    elif len(target_songs) >= 120:
        dynamic_max_pages = max(dynamic_max_pages, 25)
    if artist_mbid:
        dynamic_max_pages = max(dynamic_max_pages, 20)

    for page in range(dynamic_max_pages):
        query = f"arid:{artist_mbid}" if artist_mbid else f'artist:"{artist}"'
        try:
            response = session.get(
                MB_SEARCH_URL,
                params={
                    "query": query,
                    "fmt": "json",
                    "limit": 100,
                    "offset": page * 100,
                },
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            break

        recordings = payload.get("recordings", [])
        if not recordings:
            break

        page_hits = 0
        for recording in recordings:
            artist_credit = normalize_space(recording.get("artist-credit-phrase", ""))
            if artist_mbid == "" and not artist_match(artist, artist_credit):
                continue

            title_norm = resolve_target_song_norm(recording.get("title", ""), target_lookup, target_songs)
            if not title_norm or title_norm not in target_songs:
                continue

            years = []
            first_release_date = normalize_space(recording.get("first-release-date", ""))
            if is_valid_year(first_release_date[:4]):
                years.append(int(first_release_date[:4]))
            for release in recording.get("releases", []):
                date = normalize_space(release.get("date", ""))
                if is_valid_year(date[:4]):
                    years.append(int(date[:4]))
            if years:
                page_hits += 1
                found[title_norm] = min(found.get(title_norm, 9999), min(years))

        if len(found) >= len(target_songs):
            break
        if page_hits == 0 and page >= 4:
            break
    return found


def year_from_artist_musicbrainz_releases(
    session: requests.Session,
    artist: str,
    target_songs: Dict[str, str],
    artist_mbid: str = "",
    max_pages: int = 6,
    max_release_fetches: int = 80,
) -> Dict[str, int]:
    found: Dict[str, int] = {}
    target_lookup = build_target_song_lookup(target_songs)
    fetched_release_ids: set[str] = set()
    release_fetches = 0

    def harvest_release(release_id: str, fallback_year: int | None = None) -> None:
        nonlocal release_fetches
        if not release_id or release_id in fetched_release_ids or release_fetches >= max_release_fetches:
            return
        fetched_release_ids.add(release_id)
        release_fetches += 1
        try:
            response = session.get(
                f"{MB_RELEASE_SEARCH_URL}{release_id}",
                params={"inc": "recordings", "fmt": "json"},
                timeout=25,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return

        years: List[int] = []
        date = normalize_space(payload.get("date", ""))
        if is_valid_year(date[:4]):
            years.append(int(date[:4]))
        if fallback_year and is_valid_year(fallback_year):
            years.append(int(fallback_year))
        year = min(years) if years else None
        if not is_valid_year(year):
            return

        for medium in payload.get("media", []):
            for track in medium.get("tracks", []):
                title_norm = resolve_target_song_norm(track.get("title", ""), target_lookup, target_songs)
                if not title_norm or title_norm not in target_songs:
                    continue
                found[title_norm] = min(found.get(title_norm, 9999), int(year))

    dynamic_pages = max_pages
    if len(target_songs) >= 250:
        dynamic_pages = max(dynamic_pages, 10)
        max_release_fetches = max(max_release_fetches, 140)
    elif len(target_songs) >= 120:
        dynamic_pages = max(dynamic_pages, 8)
        max_release_fetches = max(max_release_fetches, 100)

    for page in range(dynamic_pages):
        query = f"arid:{artist_mbid}" if artist_mbid else f'artist:"{artist}"'
        try:
            response = session.get(
                MB_RELEASE_SEARCH_URL,
                params={
                    "query": query,
                    "fmt": "json",
                    "limit": 100,
                    "offset": page * 100,
                },
                timeout=25,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            break

        releases = payload.get("releases", [])
        if not releases:
            break

        page_hits = 0
        for release in releases:
            release_id = normalize_space(release.get("id", ""))
            release_artist = normalize_space(release.get("artist-credit-phrase", ""))
            if artist_mbid == "" and release_artist and not artist_match(artist, release_artist):
                continue

            title_text = normalize_space(release.get("title", ""))
            lower_title = title_text.lower()
            if any(token in lower_title for token in ["karaoke", "tribute", "greatest hits", "best of", "live at", "anthology"]):
                continue

            date = normalize_space(release.get("date", ""))
            fallback_year = int(date[:4]) if is_valid_year(date[:4]) else None
            before = len(found)
            harvest_release(release_id, fallback_year=fallback_year)
            if len(found) > before:
                page_hits += 1
            if len(found) >= len(target_songs) or release_fetches >= max_release_fetches:
                break

        if len(found) >= len(target_songs) or release_fetches >= max_release_fetches:
            break
        if page_hits == 0 and page >= 2:
            break
    return found


def enrich_release_years(df: pd.DataFrame) -> pd.DataFrame:
    cache_only = os.environ.get("SOC_RELEASE_YEAR_CACHE_ONLY") == "1"
    dedicated_cache = load_json(RELEASE_CACHE_FILE)
    artist_track_cache = load_json(ARTIST_RELEASE_TRACK_CACHE_FILE)
    master_cache = build_master_release_cache(pd.read_csv(V6_CSV, low_memory=False))
    normalized_master = {normalize_key(key): value for key, value in master_cache.items()}

    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    df["_artist_norm"] = df["artist_name"].map(normalize_space)
    df["_song_norm"] = df["song_name"].map(norm_text)

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


def wikipedia_search_titles(session: requests.Session, year: int) -> List[str]:
    direct_candidates = [
        f"Billboard Top Country & Western Records of {year}",
        f"Billboard Year-End Hot Country Singles of {year}",
        f"Billboard Year-End Hot Country Songs of {year}",
        f"Billboard Year-End Country Singles of {year}",
        f"{year} in country music",
    ]
    seen = []
    for title in direct_candidates:
        if title not in seen:
            seen.append(title)

    queries = [
        f'Billboard Top Country & Western Records of {year}',
        f'Billboard Year-End Hot Country Singles of {year}',
        f'Billboard Year-End Hot Country Songs of {year}',
        f'Billboard year-end country songs of {year}',
        f'{year} in country music',
    ]
    for query in queries:
        try:
            response = session.get(
                WIKIPEDIA_API_URL,
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": query,
                    "format": "json",
                    "srlimit": 5,
                },
                timeout=25,
            )
            response.raise_for_status()
            results = response.json().get("query", {}).get("search", [])
        except Exception:
            continue
        for item in results:
            title = item.get("title", "")
            title_norm = norm_text(title)
            if (
                title
                and str(year) in title
                and (
                    "billboard" in title_norm
                    or "country music" in title_norm
                    or "country songs" in title_norm
                    or "country singles" in title_norm
                )
                and title not in seen
            ):
                seen.append(title)
    return seen


def extract_rank_from_row(row: dict) -> Optional[int]:
    rank_candidates = []
    for key, value in row.items():
        key_norm = norm_text(key)
        if not any(token in key_norm for token in ["rank", "year end", "retail", "juke", "disk", "dj", "position", "peak", "us"]):
            continue
        value_text = normalize_space(value).replace("#", "")
        match = re.search(r"\d+", value_text)
        if match:
            rank = int(match.group(0))
            if 1 <= rank <= 100:
                rank_candidates.append(rank)
    return min(rank_candidates) if rank_candidates else None


def identify_song_artist_columns(columns: List[str]) -> Tuple[Optional[str], Optional[str]]:
    title_col = None
    artist_col = None
    preferred_title_cols = []
    fallback_title_cols = []
    for column in columns:
        col_norm = norm_text(column)
        if any(token in col_norm for token in ["title", "single"]):
            preferred_title_cols.append(column)
        elif "song" in col_norm:
            fallback_title_cols.append(column)
        if artist_col is None and "artist" in col_norm:
            artist_col = column
    if preferred_title_cols:
        title_col = preferred_title_cols[0]
    elif fallback_title_cols:
        title_col = fallback_title_cols[0]
    return title_col, artist_col


def extract_country_music_year_rows(table: pd.DataFrame, year: int, source_title: str) -> List[dict]:
    columns = [str(col) for col in table.columns]
    title_col, artist_col = identify_song_artist_columns(columns)
    if not title_col or not artist_col:
        return []

    songs_rank_col = None
    for column in columns:
        if norm_text(column) == "songs":
            songs_rank_col = column
            break
    if songs_rank_col is None:
        return []

    records = []
    for row in table.to_dict(orient="records"):
        title_value = normalize_space(row.get(title_col)).strip('"')
        artist_value = normalize_space(row.get(artist_col)).strip('"')
        if not title_value or not artist_value:
            continue
        rank_text = normalize_space(row.get(songs_rank_col)).replace("#", "")
        match = re.search(r"\d+", rank_text)
        if not match:
            continue
        rank = int(match.group(0))
        if not (1 <= rank <= 100):
            continue
        records.append(
            {
                "chart_year": year,
                "position": rank,
                "song_name": title_value,
                "artist_name": artist_value,
                "source_title": source_title,
            }
        )
    return records


def scrape_chart_rows_for_year(session: requests.Session, year: int) -> List[dict]:
    records = []
    if year >= 2006:
        titles = [f"{year} in country music"]
    else:
        titles = wikipedia_search_titles(session, year)
    for title in titles:
        try:
            response = session.get(
                WIKIPEDIA_API_URL,
                params={"action": "parse", "page": title, "prop": "text", "format": "json"},
                timeout=30,
            )
            response.raise_for_status()
            html = response.json().get("parse", {}).get("text", {}).get("*", "")
        except Exception:
            continue

        if not html:
            continue
        try:
            tables = pd.read_html(StringIO(html))
        except ValueError:
            continue

        for table in tables:
            if norm_text(title) == norm_text(f"{year} in country music"):
                extracted = extract_country_music_year_rows(table, year, title)
                if extracted:
                    records.extend(extracted)
                    continue

            columns = [str(col) for col in table.columns]
            title_col, artist_col = identify_song_artist_columns(columns)
            if not title_col or not artist_col:
                continue

            for row in table.to_dict(orient="records"):
                rank = extract_rank_from_row(row)
                title_value = normalize_space(row.get(title_col))
                artist_value = normalize_space(row.get(artist_col))
                if not title_value or not artist_value or rank is None:
                    continue
                if norm_text(title_value) in {"artist", "title", "single", "song"}:
                    continue
                records.append(
                    {
                        "chart_year": year,
                        "position": rank,
                        "song_name": title_value.strip('"'),
                        "artist_name": artist_value.strip('"'),
                        "source_title": title,
                    }
                )
        if records:
            break
    return records


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
        if {"bpm_sections", "strumming_patterns"}.difference(new_df.columns):
            log("Cached new-song rows predate section-level BPM/strumming fields; rebuilding from raw JSON")
            new_df = build_new_song_rows()
        elif cached_strumming_patterns_need_rebuild(new_df):
            log("Cached new-song rows predate numeric strumming-measure parsing; rebuilding from raw JSON")
            new_df = build_new_song_rows()
        else:
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


def save_dta_with_labels(df: pd.DataFrame) -> None:
    labels = {}
    dict_path = BASE_DIR / "country_artists_data_dictionary.csv"
    if dict_path.exists():
        try:
            dictionary_df = pd.read_csv(dict_path)
            for row in dictionary_df.itertuples(index=False):
                if pd.notna(row.column_name) and pd.notna(row.description):
                    labels[str(row.column_name)] = str(row.description)[:80]
        except Exception as exc:
            log(f"Could not read country artist dictionary for labels: {exc}")

    labels.update({key: value[:80] for key, value in STRING_LABELS.items()})
    final_labels = {column: label for column, label in labels.items() if column in df.columns}

    stata_df = df.copy()
    for column in stata_df.columns:
        if pd.api.types.is_numeric_dtype(stata_df[column]):
            continue
        stata_df[column] = stata_df[column].fillna("").astype(str).str.slice(0, 244)
        stata_df.loc[stata_df[column] == "nan", column] = ""

    for column in ["release_year", "billboard_country_year_end_year", "billboard_country_year_end_pos"]:
        if column in stata_df.columns:
            stata_df[column] = pd.to_numeric(stata_df[column], errors="coerce")

    stata_df.to_stata(OUTPUT_DTA, write_index=False, version=118, variable_labels=final_labels)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the country-only chords final dataset starting from v6.")
    parser.add_argument("--cache-only-release-years", action="store_true", help="Skip live release-year lookup and use caches only.")
    args = parser.parse_args()

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    JSON_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if args.cache_only_release_years:
        os.environ["SOC_RELEASE_YEAR_CACHE_ONLY"] = "1"

    combined = build_final_dataset()
    save_dta_with_labels(combined)
    log(f"Saved CSV to {OUTPUT_CSV}")
    log(f"Saved DTA to {OUTPUT_DTA}")


if __name__ == "__main__":
    main()
