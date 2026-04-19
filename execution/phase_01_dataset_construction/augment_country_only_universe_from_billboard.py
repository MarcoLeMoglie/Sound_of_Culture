#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import pyreadstat

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.phase_01_dataset_construction import build_extended_artist_universe as ext
from execution.phase_01_dataset_construction.enrich_artist_universe_missing_metadata import VARIABLE_LABELS


OUTPUT_DIR = ROOT / "data" / "processed_datasets" / "country_artists"
INTERMEDIATE_DIR = OUTPUT_DIR / "intermediate"
UNIVERSE_CSV = OUTPUT_DIR / "artist_universe_country_only.csv"
UNIVERSE_DTA = OUTPUT_DIR / "artist_universe_country_only.dta"
FINAL_CSV = OUTPUT_DIR / "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv"
V6_CSV = OUTPUT_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
CHART_CSV = INTERMEDIATE_DIR / "billboard_country_year_end_songs_1946_2025.csv"

MISSING_ROWS_CSV = INTERMEDIATE_DIR / "billboard_country_missing_rows_2026_04_08.csv"
MISSING_SONG_TARGETS_CSV = INTERMEDIATE_DIR / "billboard_country_missing_song_targets_2026_04_08.csv"
MISSING_ARTISTS_RAW_CSV = INTERMEDIATE_DIR / "billboard_country_missing_artists_raw_2026_04_08.csv"
ADDED_ARTISTS_CSV = INTERMEDIATE_DIR / "billboard_country_added_artists_2026_04_08.csv"
UNRESOLVED_ARTISTS_CSV = INTERMEDIATE_DIR / "billboard_country_unresolved_artists_2026_04_08.csv"
SUMMARY_JSON = INTERMEDIATE_DIR / "billboard_country_universe_augmentation_summary_2026_04_08.json"

DATA_LABEL = "Country-only artist universe with Billboard country supplement"

ARTIST_SUFFIX_PATTERNS = [
    r"\s+feat(?:\.|uring)?\s+.+$",
    r"\s+with\s+.+$",
    r"\s+duet with\s+.+$",
    r"\s+and his\s+.+$",
    r"\s+and her\s+.+$",
    r"\s+with his\s+.+$",
    r"\s+with her\s+.+$",
]


def log(message: str) -> None:
    print(f"[billboard-universe] {message}", flush=True)


def normalize_name(value: object) -> str:
    return ext.normalize_name(value)


def nonempty(value: object) -> str:
    return ext.nonempty_text(value)


def clean_chart_song_title(value: object) -> str:
    text = nonempty(value)
    if not text:
        return ""
    text = re.sub(r"\[\d+\]", "", text)
    text = text.replace('"', " ")
    text = text.replace("“", " ").replace("”", " ")
    text = text.replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def chart_song_key(value: object) -> str:
    return normalize_name(clean_chart_song_title(value))


def artist_variants(name: object) -> List[str]:
    raw = nonempty(name)
    if not raw:
        return []
    variants = [raw]
    stripped = re.sub(r"\([^)]*\)", "", raw).strip()
    if stripped and stripped not in variants:
        variants.append(stripped)
    for pattern in ARTIST_SUFFIX_PATTERNS:
        simplified = re.sub(pattern, "", raw, flags=re.IGNORECASE).strip(" ,;-")
        if simplified and simplified not in variants:
            variants.append(simplified)
        simplified = re.sub(pattern, "", stripped, flags=re.IGNORECASE).strip(" ,;-")
        if simplified and simplified not in variants:
            variants.append(simplified)
    return variants


def build_lookup(df: pd.DataFrame) -> Dict[str, dict]:
    lookup: Dict[str, dict] = {}
    for _, row in df.iterrows():
        record = row.to_dict()
        variants: List[str] = []
        for column in ["name_primary", "stage_name", "birth_name"]:
            text = nonempty(row.get(column, ""))
            if text:
                variants.extend(artist_variants(text))
        aliases = nonempty(row.get("aliases", ""))
        for alias in aliases.split("|"):
            if alias.strip():
                variants.extend(artist_variants(alias))
        for variant in variants:
            key = normalize_name(variant)
            if key and key not in lookup:
                lookup[key] = record
    return lookup


def resolve_artist_record(name: object, lookup: Dict[str, dict]) -> Tuple[Optional[dict], str]:
    for variant in artist_variants(name):
        key = normalize_name(variant)
        if key and key in lookup:
            return lookup[key], key
    raw_key = normalize_name(name)
    return None, raw_key


def dataset_artist_identity(row: pd.Series, lookup: Dict[str, dict]) -> str:
    artist_id = nonempty(row.get("artist_id", ""))
    if artist_id:
        return f"id:{artist_id}"
    for field in ["name_primary", "artist_name"]:
        record, key = resolve_artist_record(row.get(field, ""), lookup)
        if record:
            candidate_id = nonempty(record.get("artist_id", ""))
            if candidate_id:
                return f"id:{candidate_id}"
        if key:
            return f"name:{key}"
    return "name:"


def chart_artist_identity(name: object, lookup: Dict[str, dict]) -> Tuple[str, Optional[dict], str]:
    record, key = resolve_artist_record(name, lookup)
    if record:
        return f"id:{nonempty(record.get('artist_id', ''))}", record, key
    return f"name:{key}", None, key


def ensure_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = ""
    return out.reindex(columns=list(columns))


def max_existing_artist_num(df: pd.DataFrame) -> int:
    max_num = 0
    for artist_id in df["artist_id"].fillna("").astype(str):
        match = re.search(r"(\d+)$", artist_id)
        if match:
            max_num = max(max_num, int(match.group(1)))
    if V6_CSV.exists():
        v6_df = pd.read_csv(V6_CSV, usecols=["artist_id"], low_memory=False)
        for artist_id in v6_df["artist_id"].fillna("").astype(str):
            match = re.search(r"(\d+)$", artist_id)
            if match:
                max_num = max(max_num, int(match.group(1)))
    return max_num


def append_billboard_note(value: object) -> str:
    base = nonempty(value)
    extra = "billboard_country_year_end_supplement"
    if not base:
        return extra
    if extra in {piece.strip() for piece in base.split("|")}:
        return base
    return f"{base}|{extra}"


def build_placeholder_rows(
    names: Sequence[str],
    columns: Sequence[str],
) -> pd.DataFrame:
    rows: List[dict] = []
    for name in names:
        rows.append(
            {
                "name_primary": name,
                "stage_name": "",
                "birth_name": "",
                "aliases": "",
                "wikidata_qid": "",
                "musicbrainz_mbid": "",
                "isni": "",
                "viaf_id": "",
                "wikipedia_url": "",
                "birth_year": None,
                "birth_city": "",
                "birth_county": "",
                "birth_state": "",
                "birth_state_abbr": "",
                "birth_country": "",
                "death_date": "",
                "death_year": None,
                "death_place_raw": "",
                "citizenship": "",
                "occupations": "",
                "genres_raw": "",
                "genres_normalized": "",
                "country_relevance_score": 0,
                "instruments": "",
                "member_of": "",
                "record_labels": "",
                "awards": "",
                "official_website": "",
                "birth_decade": None,
                "us_macro_region": "Unknown",
                "is_deceased": 0,
                "age_or_age_at_death": None,
                "is_us_born": 0,
                "is_solo_person": 1,
                "is_country_core": 0,
                "is_country_broad": 1,
                "flag_restricted_sample": 0,
                "flag_expanded_sample": 1,
                "sample_membership": "expanded_only",
                "inclusion_reason": "Billboard country year-end supplement",
                "exclusion_reason": "",
                "source_primary": "Billboard country year-end chart",
                "source_secondary": "",
                "source_seed": "Billboard country year-end supplement",
                "evidence_urls": "",
                "source_count": 1,
                "birth_date_confidence": "low",
                "birth_place_confidence": "low",
                "genre_confidence": "medium",
                "manual_review_needed": 1,
                "notes": "Added from Billboard country year-end chart; structured artist metadata unresolved in current run",
                "name_key": normalize_name(name),
                "in_songfile": 0,
                "in_country_restricted": 0,
                "in_country_master": 0,
                "in_adjacent_seed_pool": 0,
                "source_membership_count": 0,
                "artist_name_songfile_example": "",
                "adjacent_genre_bucket": "",
                "adjacent_source_group": "billboard_country_year_end_supplement",
                "core_union_flag": 0,
                "birth_admin_labels": "",
                "wikipedia_categories": "",
                "wp_birth_place_raw": "",
                "wp_birth_year": "",
                "wp_birth_date": "",
                "birth_date": "",
                "birth_place_raw": "",
                "birth_country_raw": "",
            }
        )
    out = pd.DataFrame(rows)
    return ensure_columns(out, columns)


def save_universe(df: pd.DataFrame) -> None:
    df.to_csv(UNIVERSE_CSV, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
    stata_df = df.copy()
    for column in stata_df.columns:
        if stata_df[column].dtype == "object":
            stata_df[column] = stata_df[column].where(stata_df[column].notna(), None)
            stata_df[column] = stata_df[column].map(lambda value: None if value is None else str(value))
    column_labels = [VARIABLE_LABELS.get(col, col) for col in stata_df.columns]
    pyreadstat.write_dta(stata_df, UNIVERSE_DTA, column_labels=column_labels, file_label=DATA_LABEL, version=15)
    log(f"Saved {UNIVERSE_CSV.name} and {UNIVERSE_DTA.name}")


def summarize_chart_targets(rows_df: pd.DataFrame) -> pd.DataFrame:
    groups: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for row in rows_df.to_dict(orient="records"):
        groups[(row["resolved_artist_identity"], row["song_key"])].append(row)

    targets: List[dict] = []
    for (_, _), items in groups.items():
        items = sorted(items, key=lambda item: (item["position"], item["chart_year"]))
        top = items[0]
        chart_years = "|".join(str(item["chart_year"]) for item in sorted(items, key=lambda item: item["chart_year"]))
        chart_positions = "|".join(str(item["position"]) for item in sorted(items, key=lambda item: (item["chart_year"], item["position"])))
        targets.append(
            {
                "artist_id": top["resolved_artist_id"],
                "name_primary": top["resolved_artist_name"],
                "chart_artist_name": top["artist_name"],
                "artist_resolution": top["artist_resolution"],
                "song_name": top["song_name_clean"],
                "song_key": top["song_key"],
                "best_chart_year": top["chart_year"],
                "best_chart_pos": top["position"],
                "all_chart_years": chart_years,
                "all_chart_positions": chart_positions,
                "source_titles": "|".join(sorted({item["source_title"] for item in items if item["source_title"]})),
            }
        )
    return pd.DataFrame(targets).sort_values(["artist_resolution", "name_primary", "song_name"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Augment artist_universe_country_only with Billboard country year-end artists.")
    parser.add_argument("--skip-live-lookup", action="store_true", help="Do not query live metadata sources; create placeholders only.")
    parser.add_argument("--limit-new-artists", type=int, default=None, help="Optional cap for testing.")
    parser.add_argument("--dry-run", action="store_true", help="Compute outputs without overwriting the artist universe files.")
    args = parser.parse_args()

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    universe_df = pd.read_csv(UNIVERSE_CSV, low_memory=False)
    final_df = pd.read_csv(FINAL_CSV, low_memory=False)
    chart_df = pd.read_csv(CHART_CSV, low_memory=False)
    universe_columns = list(universe_df.columns)

    existing_lookup = build_lookup(universe_df)

    dataset_keys = set()
    for _, row in final_df.iterrows():
        artist_identity = dataset_artist_identity(row, existing_lookup)
        song_key = chart_song_key(row.get("song_name", ""))
        if artist_identity and song_key:
            dataset_keys.add((artist_identity, song_key))

    missing_rows: List[dict] = []
    for row in chart_df.to_dict(orient="records"):
        song_name_clean = clean_chart_song_title(row.get("song_name", ""))
        song_key = chart_song_key(song_name_clean)
        artist_identity, record, artist_key = chart_artist_identity(row.get("artist_name", ""), existing_lookup)
        if not song_key or not artist_identity:
            continue
        if (artist_identity, song_key) in dataset_keys:
            continue
        missing_rows.append(
            {
                **row,
                "song_name_clean": song_name_clean,
                "song_key": song_key,
                "resolved_artist_identity": artist_identity,
                "resolved_artist_id": nonempty(record.get("artist_id", "")) if record else "",
                "resolved_artist_name": nonempty(record.get("name_primary", "")) if record else "",
                "artist_resolution": "existing_artist" if record else "missing_artist",
                "artist_key_simplified": artist_key,
            }
        )

    missing_rows_df = pd.DataFrame(missing_rows).sort_values(["artist_resolution", "artist_name", "song_name_clean", "chart_year", "position"])
    missing_rows_df.to_csv(MISSING_ROWS_CSV, index=False)
    log(f"Saved missing chart rows to {MISSING_ROWS_CSV.name} ({len(missing_rows_df)} rows)")

    missing_artist_groups: Dict[str, List[dict]] = defaultdict(list)
    for row in missing_rows:
        if row["artist_resolution"] == "missing_artist":
            missing_artist_groups[row["artist_key_simplified"]].append(row)

    raw_missing_artists: List[dict] = []
    for key, items in sorted(missing_artist_groups.items()):
        counter = Counter(item["artist_name"] for item in items)
        display_name = counter.most_common(1)[0][0]
        raw_missing_artists.append(
            {
                "artist_key_simplified": key,
                "artist_name": display_name,
                "chart_rows": len(items),
                "unique_songs": len({item["song_key"] for item in items}),
                "sample_song": items[0]["song_name_clean"],
            }
        )
    raw_missing_artists_df = pd.DataFrame(raw_missing_artists).sort_values(["chart_rows", "artist_name"], ascending=[False, True])
    if args.limit_new_artists is not None:
        raw_missing_artists_df = raw_missing_artists_df.head(args.limit_new_artists).copy()
        allowed = set(raw_missing_artists_df["artist_key_simplified"])
        missing_rows_df = missing_rows_df[
            missing_rows_df["artist_resolution"].eq("existing_artist")
            | missing_rows_df["artist_key_simplified"].isin(allowed)
        ].copy()
    raw_missing_artists_df.to_csv(MISSING_ARTISTS_RAW_CSV, index=False)
    log(f"Saved raw missing-artist list to {MISSING_ARTISTS_RAW_CSV.name} ({len(raw_missing_artists_df)} artists)")

    builder = ext.load_builder()
    ext.make_builder_rate_limit_tolerant(builder)

    live_df = pd.DataFrame(columns=universe_columns)
    unresolved_names = raw_missing_artists_df["artist_name"].tolist()
    if unresolved_names and not args.skip_live_lookup:
        log(f"Running live lookup for {len(unresolved_names)} Billboard-missing artists")
        live_df = ext.targeted_live_lookup(
            builder,
            unresolved_names,
            "Billboard country year-end supplement",
            "billboard_country_supplement",
        )
        if not live_df.empty:
            live_df = ensure_columns(live_df, universe_columns)
            log(f"Resolved {len(live_df)} artists with live metadata sources")
        resolved_name_keys = {normalize_name(name) for name in live_df["name_primary"].fillna("").tolist()}
        unresolved_names = [name for name in unresolved_names if normalize_name(name) not in resolved_name_keys]

    placeholder_df = build_placeholder_rows(unresolved_names, universe_columns) if unresolved_names else pd.DataFrame(columns=universe_columns)
    placeholder_df.to_csv(UNRESOLVED_ARTISTS_CSV, index=False)
    if not placeholder_df.empty:
        log(f"Saved unresolved placeholders to {UNRESOLVED_ARTISTS_CSV.name} ({len(placeholder_df)} artists)")

    if live_df.empty and placeholder_df.empty:
        combined_universe_df = universe_df.copy()
        added_artists_df = pd.DataFrame(columns=universe_columns)
    else:
        added_artists_df = pd.concat([live_df, placeholder_df], ignore_index=True, sort=False)
        added_artists_df = ensure_columns(added_artists_df, universe_columns)
        added_artists_df["name_primary"] = added_artists_df["name_primary"].map(nonempty)
        added_artists_df["name_key"] = added_artists_df["name_primary"].map(normalize_name)
        added_artists_df["source_seed"] = added_artists_df["source_seed"].map(append_billboard_note)
        added_artists_df["notes"] = added_artists_df["notes"].map(append_billboard_note)
        added_artists_df["inclusion_reason"] = added_artists_df["inclusion_reason"].map(
            lambda value: nonempty(value) or "Billboard country year-end supplement"
        )
        added_artists_df["adjacent_source_group"] = added_artists_df["adjacent_source_group"].map(
            lambda value: nonempty(value) or "billboard_country_year_end_supplement"
        )
        added_artists_df["flag_expanded_sample"] = pd.to_numeric(added_artists_df["flag_expanded_sample"], errors="coerce").fillna(1).astype(int)

        existing_qids = {nonempty(value) for value in universe_df["wikidata_qid"].fillna("").tolist() if nonempty(value)}
        existing_mbids = {nonempty(value) for value in universe_df["musicbrainz_mbid"].fillna("").tolist() if nonempty(value)}
        existing_names = set(existing_lookup.keys())
        deduped_rows: List[dict] = []
        for row in added_artists_df.to_dict(orient="records"):
            qid = nonempty(row.get("wikidata_qid", ""))
            mbid = nonempty(row.get("musicbrainz_mbid", ""))
            variants = set()
            for value in [row.get("name_primary", ""), row.get("stage_name", ""), row.get("birth_name", ""), row.get("aliases", "")]:
                if "|" in nonempty(value):
                    pieces = [piece for piece in str(value).split("|") if piece.strip()]
                else:
                    pieces = [value]
                for piece in pieces:
                    for variant in artist_variants(piece):
                        key = normalize_name(variant)
                        if key:
                            variants.add(key)
            if qid and qid in existing_qids:
                continue
            if mbid and mbid in existing_mbids:
                continue
            if variants and any(variant in existing_names for variant in variants):
                continue
            deduped_rows.append(row)
            if qid:
                existing_qids.add(qid)
            if mbid:
                existing_mbids.add(mbid)
            existing_names.update(variants)

        added_artists_df = pd.DataFrame(deduped_rows)
        added_artists_df = ensure_columns(added_artists_df, universe_columns)
        next_num = max_existing_artist_num(universe_df)
        artist_ids = []
        for _ in range(len(added_artists_df)):
            next_num += 1
            artist_ids.append(f"artist_{next_num:06d}")
        added_artists_df["artist_id"] = artist_ids
        combined_universe_df = pd.concat([universe_df, added_artists_df], ignore_index=True, sort=False)
        combined_universe_df = ensure_columns(combined_universe_df, universe_columns)
        if not args.dry_run:
            save_universe(combined_universe_df)
        else:
            log("Dry run: artist universe files were not overwritten")

    added_artists_df.to_csv(ADDED_ARTISTS_CSV, index=False)
    if not added_artists_df.empty:
        log(f"Saved added artists to {ADDED_ARTISTS_CSV.name} ({len(added_artists_df)} artists)")

    combined_lookup = build_lookup(combined_universe_df)
    added_name_to_id: Dict[str, str] = {}
    for _, row in added_artists_df.iterrows():
        added_name_to_id[normalize_name(row.get("name_primary", ""))] = nonempty(row.get("artist_id", ""))

    resolved_missing_rows: List[dict] = []
    for row in missing_rows_df.to_dict(orient="records"):
        artist_identity, record, _ = chart_artist_identity(row["artist_name"], combined_lookup)
        resolved_artist_id = nonempty(record.get("artist_id", "")) if record else ""
        resolved_artist_name = nonempty(record.get("name_primary", "")) if record else row["artist_name"]
        resolution = "existing_artist"
        if resolved_artist_id and resolved_artist_id in set(added_name_to_id.values()):
            resolution = "new_artist"
        elif not resolved_artist_id:
            resolution = "unresolved_placeholder"
        resolved_missing_rows.append(
            {
                **row,
                "resolved_artist_identity": artist_identity,
                "resolved_artist_id": resolved_artist_id,
                "resolved_artist_name": resolved_artist_name,
                "artist_resolution": resolution,
            }
        )
    resolved_missing_rows_df = pd.DataFrame(resolved_missing_rows)
    resolved_missing_rows_df.to_csv(MISSING_ROWS_CSV, index=False)
    targets_df = summarize_chart_targets(resolved_missing_rows_df)
    targets_df.to_csv(MISSING_SONG_TARGETS_CSV, index=False)
    log(f"Saved unique missing-song targets to {MISSING_SONG_TARGETS_CSV.name} ({len(targets_df)} songs)")

    summary = pd.DataFrame(
        [
            {"metric": "existing_universe_artists", "value": len(universe_df)},
            {"metric": "chart_missing_rows", "value": len(missing_rows_df)},
            {"metric": "chart_missing_unique_songs", "value": len(targets_df)},
            {"metric": "chart_missing_raw_artists", "value": len(raw_missing_artists_df)},
            {"metric": "added_artists", "value": len(added_artists_df)},
            {"metric": "unresolved_placeholder_artists", "value": len(placeholder_df)},
            {"metric": "updated_universe_artists", "value": len(combined_universe_df)},
        ]
    )
    summary.to_json(SUMMARY_JSON, orient="records", indent=2)
    log(f"Saved summary to {SUMMARY_JSON.name}")


if __name__ == "__main__":
    main()
