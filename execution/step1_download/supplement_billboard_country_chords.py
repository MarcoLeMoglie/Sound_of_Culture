#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.step1_download.scraper_client import UltimateGuitarClient
from execution.step1_download import country_only_ultimate_guitar as base


OUTPUT_DIR = ROOT / "data" / "processed_datasets" / "country_artists"
INTERMEDIATE_DIR = OUTPUT_DIR / "intermediate"
UNIVERSE_CSV = OUTPUT_DIR / "artist_universe_country_only.csv"
ADDED_ARTISTS_CSV = INTERMEDIATE_DIR / "billboard_country_added_artists_2026_04_08.csv"
TARGET_SONGS_CSV = INTERMEDIATE_DIR / "billboard_country_missing_song_targets_2026_04_08.csv"
DISCOVERY_JSON = ROOT / "data" / "intermediate" / "json" / "ug_country_only_chords_discovery.json"
SUPPLEMENT_DISCOVERY_JSON = ROOT / "data" / "intermediate" / "json" / "ug_billboard_country_chords_supplement_2026_04_08.json"
REPORT_JSON = INTERMEDIATE_DIR / "billboard_country_chords_supplement_report_2026_04_08.json"
TARGET_PROGRESS_JSON = INTERMEDIATE_DIR / "billboard_country_target_progress_2026_04_08.json"
RAW_CHORDS_DIR = ROOT / "data" / "raw_tabs_country"


def log(message: str) -> None:
    print(f"[billboard-chords] {message}", flush=True)


def normalize_name(value: object) -> str:
    return base.normalize_text(value)


def load_universe() -> pd.DataFrame:
    return pd.read_csv(UNIVERSE_CSV, low_memory=False)


def build_artist_rows(df: pd.DataFrame) -> Dict[str, dict]:
    rows: Dict[str, dict] = {}
    for _, row in df.iterrows():
        artist_id = str(row.get("artist_id", "")).strip()
        if not artist_id:
            continue
        search_names: List[str] = []
        for field in ["name_primary", "stage_name"]:
            text = str(row.get(field, "")).strip()
            if text and text.lower() != "nan" and text not in search_names:
                search_names.append(text)
        rows[artist_id] = {
            "artist_id": artist_id,
            "name_primary": str(row.get("name_primary", "")).strip(),
            "stage_name": str(row.get("stage_name", "")).strip(),
            "search_names": search_names[:2],
        }
    return rows


def load_json_rows(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open() as handle:
        return json.load(handle)


def save_json_rows(path: Path, rows: List[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)


def load_progress_keys() -> set[str]:
    if not TARGET_PROGRESS_JSON.exists():
        return set()
    try:
        payload = json.loads(TARGET_PROGRESS_JSON.read_text(encoding="utf-8"))
    except Exception:
        return set()
    return {str(item).strip() for item in payload.get("attempted_target_keys", []) if str(item).strip()}


def save_progress_keys(keys: set[str]) -> None:
    TARGET_PROGRESS_JSON.write_text(
        json.dumps({"attempted_target_keys": sorted(keys)}, indent=2),
        encoding="utf-8",
    )


def merge_discovery_rows(existing_rows: List[dict], new_rows: List[dict]) -> List[dict]:
    grouped: Dict[Tuple[str, str], Dict[int, dict]] = defaultdict(dict)
    for row in existing_rows + new_rows:
        song_key = (str(row.get("artist_id", "")).strip(), base.normalize_text(row.get("song_name", "")))
        tab_id = int(row.get("id") or 0)
        if not song_key[0] or not song_key[1] or not tab_id:
            continue
        grouped[song_key][tab_id] = row

    merged: List[dict] = []
    for versions in grouped.values():
        ranked = sorted(versions.values(), key=base.tab_sort_key, reverse=True)[:3]
        merged.extend(ranked)
    merged.sort(
        key=lambda row: (
            str(row.get("artist_id", "")),
            base.normalize_text(row.get("song_name", "")),
            -base.tab_sort_key(row)[0],
            -base.tab_sort_key(row)[1],
            -base.tab_sort_key(row)[2],
        )
    )
    return merged


def song_match(target_song: str, candidate_song: str) -> bool:
    target_key = normalize_name(re.sub(r"\([^)]*\)|\[[^]]*\]", " ", str(target_song)))
    candidate_key = normalize_name(re.sub(r"\([^)]*\)|\[[^]]*\]", " ", str(candidate_song)))
    if not target_key or not candidate_key:
        return False
    if target_key == candidate_key:
        return True
    if len(target_key) >= 10 and target_key in candidate_key:
        return True
    if len(candidate_key) >= 10 and candidate_key in target_key:
        return True
    return False


def artist_query_variants(value: str) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    variants = [raw]
    for pattern in [
        r"\s+feat(?:\.|uring)?\s+.+$",
        r"\s+duet with\s+.+$",
        r"\s+and his\s+.+$",
        r"\s+with his\s+.+$",
        r"\s+and the\s+.+$",
        r"\s+with the\s+.+$",
    ]:
        simplified = re.sub(pattern, "", raw, flags=re.IGNORECASE).strip(" ,;-")
        if simplified and simplified not in variants:
            variants.append(simplified)
    return variants


def discover_target_song(client: UltimateGuitarClient, artist_row: dict, song_name: str, max_pages: int) -> List[dict]:
    queries: List[str] = []
    candidate_names: List[str] = []
    for artist_name in artist_row["search_names"] or [artist_row["name_primary"]]:
        candidate_names.extend(artist_query_variants(artist_name))
    candidate_names.extend(artist_query_variants(str(artist_row.get("chart_artist_name", "")).strip()))

    deduped_names: List[str] = []
    for name in candidate_names:
        if name and name not in deduped_names:
            deduped_names.append(name)

    for artist_name in deduped_names or [artist_row["name_primary"]]:
        query = f"{artist_name} {song_name}".strip()
        if query and query not in queries:
            queries.append(query)
    if not queries:
        queries.append(f"{artist_row['name_primary']} {song_name}".strip())
    if song_name not in queries:
        queries.append(song_name)

    matches: Dict[int, dict] = {}
    for query in queries[:5]:
        for page in range(1, max_pages + 1):
            try:
                payload = client.search(query, page=page, tab_type="Chords")
            except Exception as exc:
                if "404" in str(exc) or "500" in str(exc):
                    break
                log(f"[target] {artist_row['name_primary']} / {song_name} page {page} failed: {exc}")
                break
            if not isinstance(payload, dict):
                break
            tabs = payload.get("tabs", []) or []
            if not tabs:
                break
            for tab in tabs:
                if str(tab.get("type", "")).strip().lower() != "chords":
                    continue
                if not base.artist_matches(artist_row["search_names"], tab.get("artist_name", "")):
                    continue
                if not song_match(song_name, tab.get("song_name", "")):
                    continue
                tab_id = int(tab.get("id") or 0)
                if not tab_id:
                    continue
                matches[tab_id] = {
                    "id": tab_id,
                    "artist_id": artist_row["artist_id"],
                    "artist_name": tab.get("artist_name", ""),
                    "target_artist_name": artist_row["name_primary"],
                    "song_name": tab.get("song_name", ""),
                    "rating": tab.get("rating", 0.0),
                    "votes": tab.get("votes", 0),
                    "type": "Chords",
                    "query": query,
                    "discovery_source": "billboard_target",
                }
            if len(tabs) < 50:
                break
            time.sleep(0.35)
        time.sleep(0.25)
    return sorted(matches.values(), key=base.tab_sort_key, reverse=True)[:3]


def current_song_keys(rows: Iterable[dict]) -> set[Tuple[str, str]]:
    return {
        (str(row.get("artist_id", "")).strip(), base.normalize_text(row.get("song_name", "")))
        for row in rows
        if str(row.get("artist_id", "")).strip() and base.normalize_text(row.get("song_name", ""))
    }


def checkpoint(existing_rows: List[dict], supplement_rows: List[dict]) -> List[dict]:
    final_rows = merge_discovery_rows(existing_rows, supplement_rows)
    save_json_rows(DISCOVERY_JSON, final_rows)
    save_json_rows(SUPPLEMENT_DISCOVERY_JSON, supplement_rows)
    return final_rows


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Supplement country-only chord discovery with Billboard-missing artists and songs.")
    parser.add_argument("--max-pages-artist", type=int, default=20)
    parser.add_argument("--max-pages-song", type=int, default=3)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--min-delay", type=float, default=0.6)
    parser.add_argument("--max-delay", type=float, default=1.6)
    parser.add_argument("--skip-broad", action="store_true", help="Skip full-artist discovery and only run targeted Billboard song search.")
    parser.add_argument("--download-added-only", action="store_true", help="Only download raw JSON for already discovered rows tied to added Billboard artists.")
    parser.add_argument("--target-csv", default=str(TARGET_SONGS_CSV), help="Path to the Billboard target-song CSV to use.")
    args = parser.parse_args()
    run(
        max_pages_artist=args.max_pages_artist,
        max_pages_song=args.max_pages_song,
        workers=args.workers,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        skip_broad=args.skip_broad,
        download_added_only=args.download_added_only,
        target_csv=Path(args.target_csv),
    )


if __name__ == "__main__":
    main()
