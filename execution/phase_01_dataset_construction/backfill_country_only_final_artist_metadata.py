#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.phase_01_dataset_construction import enrich_artist_universe_missing_metadata as meta


BASE_DIR = Path("data/processed_datasets/country_artists")
TARGET_CSV = BASE_DIR / "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv"
TARGET_DTA = BASE_DIR / "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.dta"
COUNTRY_ONLY_UNIVERSE = BASE_DIR / "artist_universe_country_only.csv"
BACKFILL_DIR = BASE_DIR / "intermediate" / "final_artist_backfill"
PRECOMPUTED_LOOKUP_FILE = BASE_DIR / "intermediate" / "restricted_final_v6_backfill" / "precomputed_targeted_live_lookup.csv"

REFERENCE_FILES: List[Tuple[Path, str, int]] = [
    (COUNTRY_ONLY_UNIVERSE, "country_only_universe", 1),
    (BASE_DIR / "country_artists_restricted.csv", "restricted", 2),
    (BASE_DIR / "country_artists_master.csv", "master", 3),
    (BASE_DIR / "manual_review_queue.csv", "manual_review", 4),
    (BASE_DIR / "country_artists_excluded_or_non_us.csv", "excluded", 5),
]

ARTIST_COLUMNS = [
    "artist_id",
    "name_primary",
    "birth_name",
    "stage_name",
    "aliases",
    "wikidata_qid",
    "musicbrainz_mbid",
    "isni",
    "viaf_id",
    "wikipedia_url",
    "birth_date",
    "birth_year",
    "birth_place_raw",
    "birth_city",
    "birth_county",
    "birth_state",
    "birth_state_abbr",
    "birth_country",
    "death_date",
    "death_year",
    "death_place_raw",
    "citizenship",
    "occupations",
    "genres_raw",
    "genres_normalized",
    "country_relevance_score",
    "instruments",
    "member_of",
    "record_labels",
    "awards",
    "official_website",
    "birth_decade",
    "us_macro_region",
    "is_deceased",
    "age_or_age_at_death",
    "is_us_born",
    "is_solo_person",
    "is_country_core",
    "is_country_broad",
    "flag_restricted_sample",
    "flag_expanded_sample",
    "sample_membership",
    "inclusion_reason",
    "exclusion_reason",
    "source_primary",
    "source_secondary",
    "source_seed",
    "evidence_urls",
    "source_count",
    "birth_date_confidence",
    "birth_place_confidence",
    "genre_confidence",
    "manual_review_needed",
    "notes",
]

NUMERIC_ARTIST_COLUMNS = {
    "birth_year",
    "death_year",
    "country_relevance_score",
    "birth_decade",
    "is_deceased",
    "age_or_age_at_death",
    "is_us_born",
    "is_solo_person",
    "is_country_core",
    "is_country_broad",
    "flag_restricted_sample",
    "flag_expanded_sample",
    "source_count",
    "manual_review_needed",
}

FORCE_RESET_ARTISTS = {
    "Alee (Canada)",
    "Alee (France)",
    "Alee Kinder",
    "Adam Wakefield",
    "Artists of Then, Now & Forever",
    "Baby Eagle",
    "BeyoncÃÂ©",
    "Black Belt Eagle Scout",
    "Blu & Exile",
    "Bobby Bare Jr.",
    "Chuck Wagon & The Wheels",
    "Conjunto Rio Grande",
    "Connor Smith",
    "EXILE (Japan)",
    "EXILE Atsushi",
    "Eagle And The Men",
    "Eagle Brook Music",
    "Eagle Heights Worship",
    "Eagle Seagull",
    "Eagle's Wings",
    "Eagle-Eye Cherry",
    "Eagles",
    "Eleisha Eagle",
    "Exile (US)",
    "Exile Takahiro",
    "Fly Golden Eagle",
    "Gloriana",
    "Hubert John Richards",
    "J SOUL BROTHERS from EXILE TRIBE",
    "Jim Hurst",
    "John Richards",
    "Jonathan Jackson",
    "LANCO",
    "Laurel & Hardy",
    "Leen & Leahy",
    "Leen Jongewaard",
    "Leen Persijn",
    "Leen Zijlmans",
    "Love and Theft",
    "Mari-Leen",
    "Open Mike Eagle",
    "Ryder The Eagle",
    "Tante Leen",
    "The Eagle Rock Gospel Singers",
    "The Eagle and Child",
    "The Wreckers",
    "Thee Wreckers",
    "West Texas Exile",
    "Wilburn Brothers",
}

FINAL_ARTIST_OVERRIDES = {
    "Adam Wakefield": {
        "birth_city": "Hanover",
        "birth_state": "New Hampshire",
        "birth_country": "United States",
        "notes_append": "web-confirmed hometown/origin: NBC The Voice profile",
    },
    "Alee (Canada)": {
        "name_primary": "Alee",
        "birth_city": "Edmonton",
        "birth_state": "Alberta",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birthplace: Wikipedia",
    },
    "Blu & Exile": {
        "name_primary": "Blu & Exile",
        "birth_city": "Los Angeles",
        "birth_state": "California",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed group origin: Wikipedia",
    },
    "Chuck Wagon & The Wheels": {
        "name_primary": "Chuck Wagon & The Wheels",
        "birth_city": "Tucson",
        "birth_state": "Arizona",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed local origin/base: Tucson scene history",
    },
    "Connor Smith": {
        "name_primary": "Conner Smith",
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace/origin: official site + Wikipedia",
    },
    "Eagle Seagull": {
        "name_primary": "Eagle Seagull",
        "birth_city": "Lincoln",
        "birth_state": "Nebraska",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed group origin: Wikipedia",
    },
    "Eagles": {
        "name_primary": "Eagles",
        "birth_city": "Los Angeles",
        "birth_state": "California",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed group origin: Wikipedia",
    },
    "Exile (US)": {
        "name_primary": "Exile",
        "birth_city": "Richmond",
        "birth_state": "Kentucky",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed group origin: Wikipedia",
    },
    "Gloriana": {
        "name_primary": "Gloriana",
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed group origin: Wikipedia",
    },
    "Jonathan Jackson": {
        "name_primary": "Jonathan Jackson",
        "birth_city": "Orlando",
        "birth_state": "Florida",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace: Wikipedia",
    },
    "Jim Hurst": {
        "name_primary": "Jim Hurst",
        "birth_city": "Middlesboro",
        "birth_state": "Kentucky",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace: BluegrassBios",
    },
    "LANCO": {
        "name_primary": "LANCO",
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed group origin: Wikipedia",
    },
    "Love and Theft": {
        "name_primary": "Love and Theft",
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed duo origin: Wikipedia",
    },
    "The Pinetoppers": {
        "name_primary": "The Pinetoppers",
        "birth_city": "New York City",
        "birth_state": "New York",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed group origin/base: Roy Horton and George Vaughn Horton biographies",
    },
    "The Wreckers": {
        "name_primary": "The Wreckers",
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed duo country: Wikipedia; group origin/base curated as Nashville, Tennessee from LiveOne biography",
    },
    "Wilburn Brothers": {
        "name_primary": "The Wilburn Brothers",
        "birth_city": "Hardy",
        "birth_state": "Arkansas",
        "birth_country": "United States",
        "is_solo_person": 0,
        "notes_append": "web-confirmed duo origin: Wikipedia",
    },
}


def log(message: str) -> None:
    print(f"[final-artist-backfill] {message}", flush=True)


def load_restricted_backfill_module():
    path = ROOT / "execution" / "phase_01_dataset_construction" / "backfill_restricted_final_v6_demographics.py"
    spec = importlib.util.spec_from_file_location("restricted_backfill_module", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load restricted final backfill module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalize_name(value: object) -> str:
    return meta.normalize_name(value)


def normalize_notes(value: object) -> str:
    text = meta.nonempty(value)
    if not text:
        return ""
    pieces = [piece.strip() for piece in text.split("|") if piece.strip() and piece.strip().lower() != "nan"]
    return "|".join(dict.fromkeys(pieces))


def string_or_empty(value: object) -> str:
    text = meta.nonempty(value)
    return "" if text.lower() == "nan" else text


def split_aliases(value: object) -> List[str]:
    text = string_or_empty(value)
    return [piece.strip() for piece in text.split("|") if piece.strip()]


def artist_target_mask(df: pd.DataFrame) -> pd.Series:
    return (
        df["artist_name"].fillna("").astype(str).str.strip().ne("")
        & (
            df["birth_state"].fillna("").astype(str).str.strip().eq("")
            | df["birth_country"].fillna("").astype(str).str.strip().eq("")
            | df["us_macro_region"].fillna("").astype(str).str.strip().str.lower().eq("unknown")
            | df["name_primary"].fillna("").astype(str).str.strip().eq("")
        )
    )


def ensure_target_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ARTIST_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA if col in NUMERIC_ARTIST_COLUMNS else ""
    return out


def reset_suspicious_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    reset_mask = out["artist_name"].fillna("").astype(str).isin(FORCE_RESET_ARTISTS)
    if not reset_mask.any():
        return out
    for col in ARTIST_COLUMNS:
        if col not in out.columns or col == "artist_id":
            continue
        if col in NUMERIC_ARTIST_COLUMNS or pd.api.types.is_numeric_dtype(out[col]):
            out.loc[reset_mask, col] = pd.NA
        else:
            out.loc[reset_mask, col] = ""
    if "notes" in out.columns:
        out.loc[reset_mask, "notes"] = out.loc[reset_mask, "notes"].fillna("").astype(str).map(normalize_notes)
        out.loc[reset_mask, "notes"] = out.loc[reset_mask, "notes"].map(
            lambda value: "|".join(piece for piece in [value, "final_artist_backfill:reset_suspicious_match"] if piece)
        )
    log(f"Reset suspicious artist rows before rematching: {int(reset_mask.sum())}")
    return out


def coerce_value_for_column(df: pd.DataFrame, col: str, value: object):
    if pd.isna(value) or value == "":
        if col in NUMERIC_ARTIST_COLUMNS:
            return pd.NA
        return ""
    return value


def count_missing(df: pd.DataFrame) -> Dict[str, int]:
    artist_df = df[["artist_id", "artist_name", "name_primary", "birth_state", "birth_country", "us_macro_region"]].drop_duplicates()
    return {
        "rows_targeted": int(artist_target_mask(df).sum()),
        "artists_missing_state": int((artist_df["birth_state"].fillna("").astype(str).str.strip().eq("")).sum()),
        "artists_unknown_macro": int((artist_df["us_macro_region"].fillna("").astype(str).str.strip().str.lower().eq("unknown")).sum()),
        "artists_missing_country": int((artist_df["birth_country"].fillna("").astype(str).str.strip().eq("")).sum()),
        "artists_missing_name_primary": int((artist_df["name_primary"].fillna("").astype(str).str.strip().eq("")).sum()),
    }


def best_record(records: List[dict]) -> dict:
    def score(row: dict) -> Tuple[int, int, float, int]:
        country_score = pd.to_numeric(row.get("country_relevance_score", 0), errors="coerce")
        source_count = pd.to_numeric(row.get("source_count", 0), errors="coerce")
        return (
            int(row.get("_source_rank", 99)),
            int(row.get("_match_rank", 99)),
            -float(0 if pd.isna(country_score) else country_score),
            -int(0 if pd.isna(source_count) else source_count),
        )

    ranked = sorted(records, key=score)
    return ranked[0]


def lookup_rows_from_reference(df: pd.DataFrame, source_name: str, source_rank: int) -> Dict[str, dict]:
    lookup: Dict[str, List[dict]] = {}
    for _, row in df.iterrows():
        base = row.to_dict()
        base["_reference_source"] = source_name
        base["_source_rank"] = source_rank
        keys: List[Tuple[str, int]] = []
        for col, match_rank in [("name_primary", 1), ("stage_name", 2), ("birth_name", 3)]:
            key = normalize_name(row.get(col, ""))
            if key:
                keys.append((key, match_rank))
        for alias in split_aliases(row.get("aliases", "")):
            key = normalize_name(alias)
            if key:
                keys.append((key, 4))
        seen = set()
        for key, match_rank in keys:
            if (key, match_rank) in seen:
                continue
            seen.add((key, match_rank))
            record = dict(base)
            record["_match_rank"] = match_rank
            lookup.setdefault(key, []).append(record)
    return {key: best_record(records) for key, records in lookup.items()}


def build_combined_lookup() -> Dict[str, dict]:
    combined: Dict[str, List[dict]] = {}
    for path, source_name, source_rank in REFERENCE_FILES:
        if not path.exists():
            continue
        df = meta.collapse_merge_columns(pd.read_csv(path, low_memory=False))
        file_lookup = lookup_rows_from_reference(df, source_name, source_rank)
        for key, record in file_lookup.items():
            combined.setdefault(key, []).append(record)
    if PRECOMPUTED_LOOKUP_FILE.exists():
        pre_df = pd.read_csv(PRECOMPUTED_LOOKUP_FILE, low_memory=False)
        for _, row in pre_df.iterrows():
            key = normalize_name(row.get("target_artist_name", ""))
            if not key:
                continue
            record = row.to_dict()
            record["_reference_source"] = "precomputed_targeted_live"
            record["_source_rank"] = 1
            record["_match_rank"] = 1
            combined.setdefault(key, []).append(record)
    return {key: best_record(records) for key, records in combined.items()}


def choose_lookup_key(row: pd.Series) -> str:
    for col in ["name_primary", "artist_name", "stage_name", "birth_name"]:
        key = normalize_name(row.get(col, ""))
        if key:
            return key
    for alias in split_aliases(row.get("aliases", "")):
        key = normalize_name(alias)
        if key:
            return key
    return ""


def apply_lookup(df: pd.DataFrame, lookup: Dict[str, dict], *, match_label: str) -> Tuple[pd.DataFrame, int]:
    matched_rows = 0
    for idx in df.index[artist_target_mask(df)]:
        artist_name = string_or_empty(df.at[idx, "artist_name"])
        key = choose_lookup_key(df.loc[idx])
        if not key or key not in lookup:
            continue
        record = lookup[key]
        # Some artist strings are known to collide with unrelated Wikidata aliases.
        # For these, accept only tighter matches than alias-only lookups.
        if artist_name in FORCE_RESET_ARTISTS and int(record.get("_match_rank", 99)) >= 4:
            continue
        if artist_name in FORCE_RESET_ARTISTS:
            record_notes = normalize_notes(record.get("notes", ""))
            if "low country confidence" in record_notes.lower():
                continue
            if (
                string_or_empty(record.get("birth_country", "")) == ""
                and string_or_empty(record.get("birth_state", "")) == ""
                and int(record.get("_match_rank", 99)) > 1
            ):
                continue
        for col in ARTIST_COLUMNS:
            if col not in df.columns:
                continue
            current = df.at[idx, col]
            current_text = string_or_empty(current)
            candidate = record.get(col, "")
            should_fill = False
            if col in NUMERIC_ARTIST_COLUMNS:
                should_fill = pd.isna(current) and not pd.isna(candidate)
            else:
                should_fill = current_text == "" and string_or_empty(candidate) != ""
            if col == "us_macro_region" and current_text.lower() == "unknown" and string_or_empty(candidate).lower() != "unknown":
                should_fill = True
            if should_fill:
                df.at[idx, col] = coerce_value_for_column(df, col, candidate)
        notes = normalize_notes(df.at[idx, "notes"]) if "notes" in df.columns else ""
        tag = f"final_artist_backfill:{match_label}:{record['_reference_source']}"
        if "notes" in df.columns and tag not in notes:
            df.at[idx, "notes"] = "|".join(piece for piece in [notes, tag] if piece)
        matched_rows += 1
    return df, matched_rows


def apply_final_artist_overrides(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for artist_name, payload in FINAL_ARTIST_OVERRIDES.items():
        mask = out["artist_name"].fillna("").astype(str).eq(artist_name)
        if not mask.any():
            continue
        for col, value in payload.items():
            if col == "notes_append" or col not in out.columns:
                continue
            out.loc[mask, col] = coerce_value_for_column(out, col, value)
        if "notes" in out.columns:
            append_text = payload.get("notes_append", "")
            out.loc[mask, "notes"] = out.loc[mask, "notes"].fillna("").astype(str).map(normalize_notes)
            out.loc[mask, "notes"] = out.loc[mask, "notes"].map(
                lambda value: normalize_notes("|".join(piece for piece in [value, append_text] if piece))
            )
    return out


def targeted_live_fill(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    restricted_backfill = load_restricted_backfill_module()
    names = sorted(
        {
            string_or_empty(df.at[idx, "artist_name"])
            for idx in df.index[artist_target_mask(df)]
            if string_or_empty(df.at[idx, "artist_name"])
        }
    )
    if not names:
        return df, 0
    try:
        live_df = restricted_backfill.targeted_live_lookup(names)
    except Exception as exc:
        log(f"Targeted live lookup failed: {exc}")
        return df, 0
    if live_df.empty:
        return df, 0
    live_lookup = lookup_rows_from_reference(live_df, "targeted_live", 1)
    return apply_lookup(df, live_lookup, match_label="live")


def harmonize_and_save(df: pd.DataFrame) -> None:
    df = meta.apply_place_parsing(df)
    df = meta.clean_birth_geography(df)
    df = meta.apply_web_confirmed_overrides(df)
    df = meta.apply_group_and_occupation_annotations(df)
    df = meta.infer_state_from_existing_city_country(df)
    df = meta.refresh_country_only_derived_columns(df)
    if "notes" in df.columns:
        df["notes"] = df["notes"].map(normalize_notes)

    df.to_csv(TARGET_CSV, index=False, encoding="utf-8")

    stata_df = df.copy()
    for col in stata_df.columns:
        if col in NUMERIC_ARTIST_COLUMNS:
            stata_df[col] = pd.to_numeric(stata_df[col], errors="coerce")
            continue
        if pd.api.types.is_numeric_dtype(stata_df[col]):
            continue
        stata_df[col] = stata_df[col].astype("string").fillna("")
    stata_df.to_stata(TARGET_DTA, write_index=False, version=118)
    log(f"Saved {TARGET_CSV.name} and {TARGET_DTA.name}")


def main() -> None:
    BACKFILL_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(TARGET_CSV, low_memory=False)
    df = ensure_target_columns(df)
    df = reset_suspicious_rows(df)

    before = count_missing(df)
    log(f"Before backfill: {before}")

    lookup = build_combined_lookup()
    df, local_matches = apply_lookup(df, lookup, match_label="local")
    log(f"Rows touched by bundled lookups: {local_matches}")

    df, live_matches = targeted_live_fill(df)
    log(f"Rows touched by targeted live lookup: {live_matches}")

    df = apply_final_artist_overrides(df)

    after_before_save = count_missing(df)
    log(f"After lookup stage, before harmonization: {after_before_save}")

    harmonize_and_save(df)

    refreshed = pd.read_csv(TARGET_CSV, low_memory=False)
    after = count_missing(refreshed)
    log(f"After final save: {after}")


if __name__ == "__main__":
    main()
