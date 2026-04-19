#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd


BASE_DIR = Path("data/processed_datasets/country_artists")
INTERMEDIATE_DIR = BASE_DIR / "intermediate"

BASE_CHART_CSV = INTERMEDIATE_DIR / "billboard_country_year_end_songs_1946_2025.csv"
SUPPLEMENTAL_CHART_CSV = INTERMEDIATE_DIR / "billboard_country_number_ones_wikipedia_1946_2025.csv"
FINAL_CSV = BASE_DIR / "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv"
CORE_UNIVERSE_CSV = BASE_DIR / "artist_universe_country_only.csv"
ADJ_UNIVERSE_CSV = BASE_DIR / "artist_universe_adjacent_only.csv"
FULL_UNIVERSE_CSV = BASE_DIR / "artist_universe_country_plus_adjacent.csv"

OUTPUT_CSV = INTERMEDIATE_DIR / "billboard_country_missing_song_targets_augmented_2026_04_15_v4.csv"
OUTPUT_JSON = INTERMEDIATE_DIR / "billboard_country_missing_song_targets_augmented_2026_04_15_v4_summary.json"

ARTIST_SUFFIX_PATTERNS = [
    r"\s+feat(?:\.|uring)?\s+.+$",
    r"\s+with\s+.+$",
    r"\s+duet with\s+.+$",
    r"\s+and his\s+.+$",
    r"\s+and her\s+.+$",
    r"\s+with his\s+.+$",
    r"\s+with her\s+.+$",
    r"\s+and the\s+.+$",
    r"\s+with the\s+.+$",
]


def nonempty(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_name(value: object) -> str:
    text = nonempty(value).lower()
    if not text:
        return ""
    text = text.replace("&", " and ")
    text = text.replace("’", "'")
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_song(value: object) -> str:
    text = nonempty(value)
    if not text:
        return ""
    text = text.replace("’", "'")
    text = text.replace('"', " ")
    text = text.replace("“", " ").replace("”", " ")
    text = re.sub(r"\[[^\]]*\]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def simplify_artist(value: object) -> str:
    text = nonempty(value)
    if not text:
        return ""
    for pattern in ARTIST_SUFFIX_PATTERNS:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip(" ,;-")
    return text


def artist_variants(name: object) -> list[str]:
    raw = nonempty(name)
    if not raw:
        return []
    variants = [raw]
    stripped = re.sub(r"\([^)]*\)", "", raw).strip()
    if stripped and stripped not in variants:
        variants.append(stripped)
    for pattern in ARTIST_SUFFIX_PATTERNS:
        for base in (raw, stripped):
            simplified = re.sub(pattern, "", base, flags=re.IGNORECASE).strip(" ,;-")
            if simplified and simplified not in variants:
                variants.append(simplified)
    return variants


def build_artist_lookup(df: pd.DataFrame) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for _, row in df.iterrows():
        record = row.to_dict()
        candidates: list[str] = []
        for column in ["name_primary", "stage_name", "birth_name"]:
            text = nonempty(row.get(column, ""))
            if text:
                candidates.extend(artist_variants(text))
        aliases = nonempty(row.get("aliases", ""))
        for alias in aliases.split("|"):
            if alias.strip():
                candidates.extend(artist_variants(alias))
        for candidate in candidates:
            key = normalize_name(candidate)
            if key and key not in lookup:
                lookup[key] = record
    return lookup


def build_final_sets(final_df: pd.DataFrame) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    exact = set()
    simple = set()
    for artist_name, song_name in final_df[["artist_name", "song_name"]].drop_duplicates().itertuples(index=False, name=None):
        exact.add((normalize_name(artist_name), normalize_name(song_name)))
        simple.add((normalize_name(simplify_artist(artist_name)), normalize_name(clean_song(song_name))))
    return exact, simple


def lookup_name_set(df: pd.DataFrame) -> set[str]:
    if "name_primary" not in df.columns:
        return set()
    return {normalize_name(value) for value in df["name_primary"].dropna().astype(str)}


def choose_priority(row: dict) -> tuple[int, int, int]:
    source_kind = nonempty(row.get("source_kind", ""))
    priority = 0 if source_kind in {"year_end", "year_end_wikipedia"} else 1
    try:
        position = int(float(row.get("position", 9999)))
    except Exception:
        position = 9999
    try:
        year = int(float(row.get("chart_year", 9999)))
    except Exception:
        year = 9999
    return priority, position, year


def main() -> None:
    base_df = pd.read_csv(BASE_CHART_CSV, low_memory=False)
    supplemental_df = pd.read_csv(SUPPLEMENTAL_CHART_CSV, low_memory=False) if SUPPLEMENTAL_CHART_CSV.exists() else pd.DataFrame()
    final_df = pd.read_csv(FINAL_CSV, low_memory=False)
    core_df = pd.read_csv(CORE_UNIVERSE_CSV, low_memory=False)
    adj_df = pd.read_csv(ADJ_UNIVERSE_CSV, low_memory=False) if ADJ_UNIVERSE_CSV.exists() else pd.DataFrame(columns=["name_primary"])
    full_df = pd.read_csv(FULL_UNIVERSE_CSV, low_memory=False) if FULL_UNIVERSE_CSV.exists() else core_df.copy()

    exact_song_set, simple_song_set = build_final_sets(final_df)
    artist_lookup = build_artist_lookup(core_df)
    adj_set = lookup_name_set(adj_df)
    full_set = lookup_name_set(full_df)

    combined_rows: list[dict] = []
    for row in base_df.to_dict(orient="records"):
        row = dict(row)
        row["song_name_clean"] = clean_song(row.get("song_name", ""))
        row["song_key"] = normalize_name(row["song_name_clean"])
        row["source_kind"] = nonempty(row.get("source_kind", "")) or "year_end"
        combined_rows.append(row)
    for row in supplemental_df.to_dict(orient="records"):
        row = dict(row)
        row["song_name_clean"] = clean_song(row.get("song_name_clean") or row.get("song_name", ""))
        row["song_key"] = normalize_name(row["song_name_clean"])
        combined_rows.append(row)

    target_rows: list[dict] = []
    for row in combined_rows:
        song_name = clean_song(row.get("song_name_clean") or row.get("song_name"))
        artist_name = nonempty(row.get("artist_name", ""))
        if not song_name or not artist_name:
            continue
        exact_key = (normalize_name(artist_name), normalize_name(song_name))
        simple_key = (normalize_name(simplify_artist(artist_name)), normalize_name(song_name))
        if exact_key in exact_song_set or simple_key in simple_song_set:
            continue

        resolved_record = None
        artist_key = normalize_name(artist_name)
        for variant in artist_variants(artist_name):
            variant_key = normalize_name(variant)
            if variant_key in artist_lookup:
                resolved_record = artist_lookup[variant_key]
                artist_key = variant_key
                break

        resolved_artist_id = nonempty(resolved_record.get("artist_id", "")) if resolved_record else ""
        resolved_artist_name = nonempty(resolved_record.get("name_primary", "")) if resolved_record else artist_name
        target_rows.append(
            {
                **row,
                "song_name_clean": song_name,
                "song_key": normalize_name(song_name),
                "resolved_artist_identity": f"id:{resolved_artist_id}" if resolved_artist_id else f"name:{artist_key}",
                "resolved_artist_id": resolved_artist_id,
                "resolved_artist_name": resolved_artist_name,
                "artist_resolution": "existing_artist" if resolved_record else "missing_artist",
                "artist_key_simplified": artist_key,
                "artist_in_adjacent": int(artist_key in adj_set),
                "artist_in_full": int(artist_key in full_set),
            }
        )

    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in target_rows:
        grouped.setdefault((row["resolved_artist_identity"], row["song_key"]), []).append(row)

    summarized: list[dict] = []
    for items in grouped.values():
        items = sorted(items, key=choose_priority)
        top = items[0]
        summarized.append(
            {
                "artist_id": top["resolved_artist_id"],
                "name_primary": top["resolved_artist_name"],
                "chart_artist_name": top["artist_name"],
                "artist_resolution": top["artist_resolution"],
                "song_name": top["song_name_clean"],
                "song_key": top["song_key"],
                "best_chart_year": top["chart_year"],
                "best_chart_pos": top["position"],
                "all_chart_years": "|".join(str(item["chart_year"]) for item in sorted(items, key=lambda item: int(item["chart_year"]))),
                "all_chart_positions": "|".join(str(item["position"]) for item in sorted(items, key=lambda item: (int(item["chart_year"]), int(item["position"])))),
                "source_titles": "|".join(sorted({nonempty(item.get("source_title", "")) for item in items if nonempty(item.get("source_title", ""))})),
                "source_kinds": "|".join(sorted({nonempty(item.get("source_kind", "")) for item in items if nonempty(item.get("source_kind", ""))})),
                "artist_in_adjacent": int(any(int(item.get("artist_in_adjacent", 0)) == 1 for item in items)),
                "artist_in_full": int(any(int(item.get("artist_in_full", 0)) == 1 for item in items)),
            }
        )

    out_df = pd.DataFrame(summarized).sort_values(["artist_resolution", "name_primary", "song_name"]).reset_index(drop=True)
    out_df.to_csv(OUTPUT_CSV, index=False)

    summary = {
        "base_rows": int(len(base_df)),
        "base_unique_songs": int(base_df[["artist_name", "song_name"]].drop_duplicates().shape[0]),
        "supplemental_rows": int(len(supplemental_df)),
        "supplemental_unique_songs": int(supplemental_df[["artist_name", "song_name"]].drop_duplicates().shape[0]) if not supplemental_df.empty else 0,
        "supplemental_years": sorted(pd.to_numeric(supplemental_df["chart_year"], errors="coerce").dropna().astype(int).unique().tolist()) if not supplemental_df.empty else [],
        "augmented_missing_song_targets": int(len(out_df)),
        "targets_existing_artist": int(out_df["artist_resolution"].eq("existing_artist").sum()) if not out_df.empty else 0,
        "targets_missing_artist": int(out_df["artist_resolution"].eq("missing_artist").sum()) if not out_df.empty else 0,
        "targets_artist_in_adjacent": int(out_df["artist_in_adjacent"].sum()) if not out_df.empty else 0,
        "targets_artist_in_full": int(out_df["artist_in_full"].sum()) if not out_df.empty else 0,
    }
    OUTPUT_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
