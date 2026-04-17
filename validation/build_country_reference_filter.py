#!/usr/bin/env python3
"""Build reference-list flags for country artist validation.

This script compares the project artist universe against three high-confidence
country reference sources:

1. Artists who reached number one on the U.S. Hot Country chart
2. Country Music Hall of Fame inductees
3. Billboard's best country singers/artists list

Outputs are written under validation/results so the validation workflow stays
separate from the main data pipeline.
"""

from __future__ import annotations

import re
import unicodedata
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


ROOT = Path("/Users/tommaso.colussi/Dropbox/Sound_of_Culture")
DATASET = ROOT / "data/processed_datasets/country_artists/artist_universe_country_only.dta"
VALIDATION_DIR = ROOT / "validation"
RESULTS_DIR = VALIDATION_DIR / "results"
CACHE_DIR = VALIDATION_DIR / "cache"

HEADERS = {"User-Agent": "Mozilla/5.0"}
WIKI_HOT_ROOT = "https://en.wikipedia.org/wiki/List_of_artists_who_reached_number_one_on_the_U.S._Hot_Country_chart"
WIKI_HOF = "https://en.wikipedia.org/wiki/List_of_Country_Music_Hall_of_Fame_inductees"
BILLBOARD = "https://www.billboard.com/lists/best-country-singers/"

HOT_LINK_PATTERNS = (
    "List_of_Hot_Country_Songs_number_ones_of_",
    "List_of_Most_Played_Juke_Box_Folk_Records_number_ones_of_",
    "List_of_Best_Selling_Folk_Records_number_ones_of_",
    "List_of_Country_and_Western_Records_Most_Played_by_Jockeys_number_ones_of_",
    "List_of_Hot_C_and_W_Sides_number_ones_of_",
    "List_of_Hot_Country_Singles_number_ones_of_",
)


def normalize(name: str) -> str:
    if not isinstance(name, str):
        return ""
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii").lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\bthe\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_artist_text(text: str) -> list[str]:
    if not isinstance(text, str):
        return []
    cleaned = re.sub(r"\[[^\]]*\]", "", text).strip()
    if not cleaned:
        return []

    normalized_separators = (
        cleaned.replace(" featuring ", " / ")
        .replace(" feat. ", " / ")
        .replace(" feat ", " / ")
        .replace(" with ", " / ")
        .replace(" and ", " / ")
        .replace(" & ", " / ")
    )
    parts = [part.strip(" -") for part in normalized_separators.split("/") if part.strip(" -")]
    if not parts:
        parts = [cleaned]
    return parts


def fetch_html(url: str, cache_name: str) -> str:
    cache_path = CACHE_DIR / cache_name
    if cache_path.exists():
        return cache_path.read_text()
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    text = response.text
    cache_path.write_text(text)
    return text


def read_tables_from_url(url: str, cache_name: str) -> list[pd.DataFrame]:
    html = fetch_html(url, cache_name)
    return pd.read_html(StringIO(html))


def extract_hot_country_links() -> list[str]:
    html = fetch_html(WIKI_HOT_ROOT, "wiki_hot_country_root.html")
    soup = BeautifulSoup(html, "html.parser")
    urls: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "")
        if not href.startswith("/wiki/"):
            continue
        if any(pattern in href for pattern in HOT_LINK_PATTERNS):
            urls.add(f"https://en.wikipedia.org{href}")
    return sorted(urls)


def extract_hot_country_artists() -> set[str]:
    artists: set[str] = set()
    page_urls = extract_hot_country_links()
    for idx, page_url in enumerate(page_urls):
        cache_name = f"hot_country_{idx:03d}.html"
        try:
            tables = read_tables_from_url(page_url, cache_name)
        except Exception:
            continue
        for table in tables:
            columns = [str(c) for c in table.columns]
            artist_columns = [c for c in columns if "artist" in c.lower()]
            if not artist_columns:
                continue
            for column in artist_columns:
                for value in table[column].dropna().astype(str):
                    for artist in split_artist_text(value):
                        if 1 < len(artist) < 120:
                            artists.add(artist)
    return artists


def extract_hall_of_fame_artists() -> set[str]:
    artists: set[str] = set()
    tables = read_tables_from_url(WIKI_HOF, "wiki_country_hall_of_fame.html")
    for table in tables:
        columns = [str(c) for c in table.columns]
        target = next((c for c in columns if "inductee" in c.lower() or c.lower() == "name"), None)
        if target is None:
            continue
        for value in table[target].dropna().astype(str):
            for artist in split_artist_text(value):
                if 1 < len(artist) < 160:
                    artists.add(artist)
    return artists


def extract_billboard_artists() -> set[str]:
    html = fetch_html(BILLBOARD, "billboard_best_country_singers.html")
    soup = BeautifulSoup(html, "html.parser")
    artists: set[str] = set()
    patterns = [
        re.compile(r"^(?:No\.?\s*)?(\d{1,3})\.?\s+(.+?)$"),
        re.compile(r"^(\d{1,3})\s*[-:.]\s*(.+?)$"),
    ]

    for tag in soup.select("h2, h3, h4, a, span"):
        text = tag.get_text(" ", strip=True)
        if not text or len(text) > 120:
            continue
        for pattern in patterns:
            match = pattern.match(text)
            if not match:
                continue
            rank = int(match.group(1))
            name = match.group(2).strip(" :-")
            if 1 <= rank <= 100 and 1 < len(name) < 100:
                artists.add(name)
            break

    if not artists:
        full_text = soup.get_text("\n", strip=True)
        for match in re.finditer(r"\b(?:No\.?\s*)?(\d{1,3})\.?\s+([A-Z][A-Za-z0-9&'\-., ]{2,80})", full_text):
            rank = int(match.group(1))
            if 1 <= rank <= 100:
                artists.add(match.group(2).strip(" :-"))

    junk = {
        normalize("of all time"),
        normalize("staff list"),
        normalize("the 100 greatest country artists of all time"),
    }
    return {artist for artist in artists if normalize(artist) not in junk and len(artist.split()) <= 10}


def build_reference_mapping(reference_artists: set[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for artist in sorted(reference_artists):
        key = normalize(artist)
        if key and key not in mapping:
            mapping[key] = artist
    return mapping


def main() -> int:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_stata(DATASET, convert_categoricals=False)
    artists = df[
        ["artist_id", "name_primary", "country_relevance_score", "is_country_core", "is_country_broad", "source_seed"]
    ].copy()
    artists["norm_name"] = artists["name_primary"].map(normalize)

    hot_country = extract_hot_country_artists()
    hall_of_fame = extract_hall_of_fame_artists()
    billboard = extract_billboard_artists()

    sources = {
        "hot_country_number_one": build_reference_mapping(hot_country),
        "country_hall_of_fame": build_reference_mapping(hall_of_fame),
        "billboard_best_country_singers": build_reference_mapping(billboard),
    }

    for source_name, mapping in sources.items():
        artists[source_name] = artists["norm_name"].isin(mapping)
        artists[f"{source_name}_matched_name"] = artists["norm_name"].map(mapping)

    source_cols = list(sources.keys())
    artists["in_any_reference_list"] = artists[source_cols].any(axis=1)
    artists["reference_list_count"] = artists[source_cols].sum(axis=1)
    artists["reference_list_hits"] = artists.apply(
        lambda row: "|".join([column for column in source_cols if bool(row[column])]), axis=1
    )

    reference_names = []
    for source_name, mapping in sources.items():
        for normalized_name, original_name in mapping.items():
            reference_names.append(
                {
                    "source": source_name,
                    "reference_name": original_name,
                    "normalized_name": normalized_name,
                }
            )
    reference_names_df = pd.DataFrame(reference_names).sort_values(["source", "reference_name"])

    artists.sort_values("name_primary").to_csv(RESULTS_DIR / "artist_reference_flags.csv", index=False)
    artists[~artists["in_any_reference_list"]].sort_values(
        ["country_relevance_score", "name_primary"]
    ).to_csv(RESULTS_DIR / "artists_not_in_reference_lists.csv", index=False)
    reference_names_df.to_csv(RESULTS_DIR / "reference_names_by_source.csv", index=False)

    summary_rows = []
    for source_name, mapping in sources.items():
        summary_rows.append(
            {
                "source": source_name,
                "reference_name_count": len(mapping),
                "matched_dataset_artists": int(artists[source_name].sum()),
            }
        )
    summary_rows.append(
        {
            "source": "union_any_reference_list",
            "reference_name_count": None,
            "matched_dataset_artists": int(artists["in_any_reference_list"].sum()),
        }
    )
    summary_rows.append(
        {
            "source": "not_in_any_reference_list",
            "reference_name_count": None,
            "matched_dataset_artists": int((~artists["in_any_reference_list"]).sum()),
        }
    )
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(RESULTS_DIR / "reference_filter_summary.csv", index=False)

    print(f"dataset_rows {len(artists)}")
    for row in summary_rows:
        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
