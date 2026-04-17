#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.step4_country_artists import augment_country_only_universe_from_billboard as aug


OUTPUT_DIR = aug.OUTPUT_DIR
INTERMEDIATE_DIR = aug.INTERMEDIATE_DIR
FINAL_CSV = aug.FINAL_CSV
CORE_UNIVERSE_CSV = aug.UNIVERSE_CSV
ADJ_UNIVERSE_CSV = OUTPUT_DIR / "artist_universe_adjacent_only.csv"
FULL_UNIVERSE_CSV = OUTPUT_DIR / "artist_universe_country_plus_adjacent.csv"
YEAR_END_CHART_CSV = aug.CHART_CSV

SUPPLEMENTAL_NUMBER_ONES_CSV = INTERMEDIATE_DIR / "billboard_country_number_ones_wikipedia_1946_2025.csv"
SUPPLEMENTAL_TARGETS_CSV = INTERMEDIATE_DIR / "billboard_country_missing_song_targets_augmented_2026_04_15.csv"
SUPPLEMENTAL_REPORT_JSON = INTERMEDIATE_DIR / "billboard_country_missing_song_targets_augmented_2026_04_15.json"

WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "SoundOfCultureBillboard/1.0 (research pipeline)"


def log(message: str) -> None:
    print(f"[billboard-supplement] {message}", flush=True)


def norm(value: object) -> str:
    return aug.normalize_name(value)


def clean_song(value: object) -> str:
    return aug.clean_chart_song_title(value)


def simplify_artist(value: object) -> str:
    text = aug.nonempty(value)
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
        text = pd.Series([text]).replace(pattern, "", regex=True).iloc[0].strip(" ,;-")
    return text


def identify_song_artist_columns(columns: List[str]) -> tuple[Optional[str], Optional[str]]:
    song_col = None
    artist_col = None
    for col in columns:
        low = str(col).strip().lower()
        if song_col is None and any(token in low for token in ["song", "title", "single"]):
            song_col = col
        if artist_col is None and "artist" in low:
            artist_col = col
    return song_col, artist_col


def fetch_wikipedia_title(session: requests.Session, year: int) -> Optional[str]:
    if 1946 <= year <= 1969:
        return f"Billboard year-end top 50 country & western singles of {year}"
    candidates = [
        f"Billboard year-end top 50 country & western singles of {year}",
        f"Billboard year-end top country singles of {year}",
        f"Billboard year-end top 100 country singles of {year}",
        f"Billboard Year-End Hot Country Singles of {year}",
        f"Billboard Year-End Hot Country Songs of {year}",
        f"List of Billboard number-one country singles of {year}",
        f"List of Hot Country Singles number ones of {year}",
        f"List of Hot Country Songs number ones of {year}",
    ]
    for title in candidates:
        try:
            response = session.get(
                WIKIPEDIA_API_URL,
                params={"action": "query", "titles": title, "format": "json", "redirects": 1},
                timeout=30,
            )
            response.raise_for_status()
            pages = response.json().get("query", {}).get("pages", {})
            for page in pages.values():
                if int(page.get("pageid", -1)) > 0:
                    return str(page.get("title", "")).strip() or title
        except Exception:
            continue
    try:
        response = session.get(
            WIKIPEDIA_API_URL,
            params={
                "action": "query",
                "list": "search",
                "format": "json",
                "srlimit": 5,
                "srsearch": f'"{year}" "Billboard" "country" ("year-end" OR "number ones")',
            },
            timeout=30,
        )
        response.raise_for_status()
        for item in response.json().get("query", {}).get("search", []):
            title = str(item.get("title", "")).strip()
            low = title.lower()
            if str(year) in low and "country" in low and ("billboard" in low or "hot country" in low):
                return title
    except Exception:
        return None
    return None


def identify_rank_column(columns: List[str]) -> Optional[str]:
    for col in columns:
        low = str(col).strip().lower()
        if low in {"rank", "no.", "no", "#", "position"} or "rank" in low:
            return col
    return None


def scrape_billboard_page(session: requests.Session, year: int) -> List[dict]:
    title = fetch_wikipedia_title(session, year)
    if not title:
        return []
    try:
        response = session.get(
            WIKIPEDIA_API_URL,
            params={"action": "parse", "page": title, "prop": "text", "format": "json"},
            timeout=30,
        )
        response.raise_for_status()
        html = response.json().get("parse", {}).get("text", {}).get("*", "")
    except Exception:
        return []
    if not html:
        return []
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        return []

    rows: List[dict] = []
    seen = set()
    for table in tables:
        columns = [str(col) for col in table.columns]
        rank_col = identify_rank_column(columns)
        song_col, artist_col = identify_song_artist_columns(columns)
        if not song_col or not artist_col:
            continue
        for item in table.to_dict(orient="records"):
            song_name = clean_song(item.get(song_col, ""))
            artist_name = aug.nonempty(item.get(artist_col, ""))
            if not song_name or not artist_name:
                continue
            key = (norm(simplify_artist(artist_name)), norm(song_name))
            if key in seen:
                continue
            seen.add(key)
            try:
                position = int(float(item.get(rank_col))) if rank_col and str(item.get(rank_col)).strip() not in {"", "nan"} else 1
            except Exception:
                position = 1
            low_title = title.lower()
            source_kind = "year_end_wikipedia" if "year-end" in low_title or "top" in low_title else "weekly_number_one_wikipedia"
            rows.append(
                {
                    "chart_year": year,
                    "position": position,
                    "song_name": song_name,
                    "artist_name": artist_name,
                    "song_name_clean": song_name,
                    "song_key": norm(song_name),
                    "source_title": title,
                    "source_kind": source_kind,
                }
            )
    return rows


def build_supplemental_number_ones(force_rebuild: bool) -> pd.DataFrame:
    if SUPPLEMENTAL_NUMBER_ONES_CSV.exists() and not force_rebuild:
        df = pd.read_csv(SUPPLEMENTAL_NUMBER_ONES_CSV, low_memory=False)
        if not df.empty:
            return df

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    all_rows: List[dict] = []
    for year in range(1946, 2026):
        rows = scrape_billboard_page(session, year)
        if rows:
            all_rows.extend(rows)
        if year % 10 == 0 or year == 2025:
            log(f"Supplemental number-one scrape progress through {year}: {len(all_rows)} rows")

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["chart_year", "song_key", "artist_name"])
    df.to_csv(SUPPLEMENTAL_NUMBER_ONES_CSV, index=False)
    return df


def build_final_song_sets(final_df: pd.DataFrame) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    exact = set()
    simple = set()
    for row in final_df[["artist_name", "song_name"]].drop_duplicates().itertuples(index=False):
        exact.add((norm(row.artist_name), norm(row.song_name)))
        simple.add((norm(simplify_artist(row.artist_name)), norm(clean_song(row.song_name))))
    return exact, simple


def build_lookup_sets(df: pd.DataFrame) -> set[str]:
    return {norm(value) for value in df["name_primary"].dropna().astype(str)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build supplemental Billboard country targets from alternate sources.")
    parser.add_argument("--force-rebuild-number-ones", action="store_true", help="Re-scrape Wikipedia number-one pages even if cached.")
    args = parser.parse_args()

    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    year_end_df = pd.read_csv(YEAR_END_CHART_CSV, low_memory=False)
    supplemental_df = build_supplemental_number_ones(args.force_rebuild_number_ones)
    final_df = pd.read_csv(FINAL_CSV, low_memory=False)
    core_df = pd.read_csv(CORE_UNIVERSE_CSV, low_memory=False)
    adj_df = pd.read_csv(ADJ_UNIVERSE_CSV, low_memory=False)
    full_df = pd.read_csv(FULL_UNIVERSE_CSV, low_memory=False)

    exact_song_set, simple_song_set = build_final_song_sets(final_df)
    core_lookup = aug.build_lookup(core_df)
    adj_set = build_lookup_sets(adj_df)
    full_set = build_lookup_sets(full_df)

    year_end_rows = year_end_df.to_dict(orient="records")
    supplemental_rows = supplemental_df.to_dict(orient="records")
    combined_rows: List[dict] = []

    for row in year_end_rows:
        combined_rows.append(
            {
                **row,
                "song_name_clean": clean_song(row.get("song_name", "")),
                "song_key": norm(clean_song(row.get("song_name", ""))),
                "source_kind": "year_end",
            }
        )
    for row in supplemental_rows:
        combined_rows.append(row)

    target_rows: List[dict] = []
    for row in combined_rows:
        song_name = clean_song(row.get("song_name_clean") or row.get("song_name"))
        artist_name = aug.nonempty(row.get("artist_name", ""))
        if not song_name or not artist_name:
            continue
        exact_key = (norm(artist_name), norm(song_name))
        simple_key = (norm(simplify_artist(artist_name)), norm(song_name))
        if exact_key in exact_song_set or simple_key in simple_song_set:
            continue
        record, artist_key = aug.resolve_artist_record(artist_name, core_lookup)
        resolved_artist_id = aug.nonempty(record.get("artist_id", "")) if record else ""
        resolved_artist_name = aug.nonempty(record.get("name_primary", "")) if record else artist_name
        artist_resolution = "existing_artist" if record else "missing_artist"
        target_rows.append(
            {
                **row,
                "song_name_clean": song_name,
                "song_key": norm(song_name),
                "resolved_artist_identity": f"id:{resolved_artist_id}" if resolved_artist_id else f"name:{artist_key}",
                "resolved_artist_id": resolved_artist_id,
                "resolved_artist_name": resolved_artist_name,
                "artist_resolution": artist_resolution,
                "artist_key_simplified": artist_key,
                "artist_in_adjacent": int(artist_key in adj_set),
                "artist_in_full": int(artist_key in full_set),
            }
        )

    target_df = pd.DataFrame(target_rows)
    if target_df.empty:
        pd.DataFrame().to_csv(SUPPLEMENTAL_TARGETS_CSV, index=False)
        log("No supplemental missing-song targets to save")
        return

    summarized = []
    grouped: Dict[tuple[str, str], List[dict]] = {}
    for row in target_df.to_dict(orient="records"):
        grouped.setdefault((row["resolved_artist_identity"], row["song_key"]), []).append(row)

    for (_, _), items in grouped.items():
        items = sorted(items, key=lambda item: (0 if item["source_kind"] == "year_end" else 1, item["position"], item["chart_year"]))
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
                "all_chart_years": "|".join(str(item["chart_year"]) for item in sorted(items, key=lambda item: item["chart_year"])),
                "all_chart_positions": "|".join(str(item["position"]) for item in sorted(items, key=lambda item: (item["chart_year"], item["position"]))),
                "source_titles": "|".join(sorted({str(item.get("source_title", "")) for item in items if str(item.get("source_title", ""))})),
                "source_kinds": "|".join(sorted({str(item.get("source_kind", "")) for item in items if str(item.get("source_kind", ""))})),
                "artist_in_adjacent": int(any(int(item.get("artist_in_adjacent", 0)) == 1 for item in items)),
                "artist_in_full": int(any(int(item.get("artist_in_full", 0)) == 1 for item in items)),
            }
        )

    summarized_df = pd.DataFrame(summarized).sort_values(["artist_resolution", "name_primary", "song_name"]).reset_index(drop=True)
    summarized_df.to_csv(SUPPLEMENTAL_TARGETS_CSV, index=False)

    report = {
        "year_end_rows": int(len(year_end_df)),
        "year_end_unique_songs": int(year_end_df[["artist_name", "song_name"]].drop_duplicates().shape[0]),
        "supplemental_number_one_rows": int(len(supplemental_df)),
        "supplemental_number_one_years": sorted(pd.to_numeric(supplemental_df["chart_year"], errors="coerce").dropna().astype(int).unique().tolist()) if not supplemental_df.empty else [],
        "augmented_missing_song_targets": int(len(summarized_df)),
        "targets_existing_artist": int(summarized_df["artist_resolution"].eq("existing_artist").sum()),
        "targets_missing_artist": int(summarized_df["artist_resolution"].eq("missing_artist").sum()),
        "targets_artist_in_adjacent": int(summarized_df["artist_in_adjacent"].sum()),
        "targets_artist_in_full": int(summarized_df["artist_in_full"].sum()),
    }
    SUPPLEMENTAL_REPORT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    log(f"Saved supplemental number ones to {SUPPLEMENTAL_NUMBER_ONES_CSV.name} ({len(supplemental_df)} rows)")
    log(f"Saved augmented song targets to {SUPPLEMENTAL_TARGETS_CSV.name} ({len(summarized_df)} rows)")


if __name__ == "__main__":
    main()
