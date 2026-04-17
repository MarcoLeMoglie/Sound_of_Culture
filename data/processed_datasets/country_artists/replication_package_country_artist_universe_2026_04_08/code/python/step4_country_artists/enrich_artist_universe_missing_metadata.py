#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import shutil
import sys
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
import pyreadstat
import requests
from bs4 import BeautifulSoup

BASE_DIR = Path("/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from execution.step4_country_artists import build_country_artists_dataset as builder

OUTPUT_DIR = BASE_DIR / "data" / "processed_datasets" / "country_artists"
INTERMEDIATE_DIR = OUTPUT_DIR / "intermediate"

CORE_BASENAME = "artist_universe_country_core"
ADJ_BASENAME = "artist_universe_adjacent_only"
FULL_BASENAME = "artist_universe_country_plus_adjacent"
REVIEW_BASENAME = "artist_universe_manual_review"
QC_REPORT_PATH = OUTPUT_DIR / "artist_universe_qc_report.md"

VARIABLE_LABELS = {
    "artist_id": "Unique artist ID",
    "name_primary": "Primary artist name",
    "birth_name": "Birth name",
    "stage_name": "Stage or professional name",
    "aliases": "Alternative names and aliases",
    "wikidata_qid": "Wikidata entity ID",
    "musicbrainz_mbid": "MusicBrainz artist ID",
    "isni": "ISNI identifier",
    "viaf_id": "VIAF identifier",
    "wikipedia_url": "Wikipedia page URL",
    "birth_year": "Birth year or formation year",
    "birth_city": "Birth city or origin city",
    "birth_county": "Birth county",
    "birth_state": "Birth state or origin state",
    "birth_state_abbr": "Birth state abbreviation",
    "birth_country": "Birth country or origin country",
    "death_date": "Death date",
    "death_year": "Death year",
    "death_place_raw": "Raw death place text",
    "citizenship": "Citizenship",
    "occupations": "Occupations",
    "genres_raw": "Raw genres text",
    "genres_normalized": "Normalized genres",
    "country_relevance_score": "Country relevance score",
    "instruments": "Instruments",
    "member_of": "Group memberships",
    "record_labels": "Record labels",
    "awards": "Awards and honors",
    "official_website": "Official website",
    "birth_decade": "Birth decade",
    "us_macro_region": "US macro region",
    "is_deceased": "Deceased indicator",
    "age_or_age_at_death": "Age or age at death",
    "is_us_born": "US-born indicator",
    "is_solo_person": "Solo person indicator",
    "is_country_core": "Core country indicator",
    "is_country_broad": "Broad country indicator",
    "flag_restricted_sample": "Restricted sample flag",
    "flag_expanded_sample": "Expanded sample flag",
    "sample_membership": "Sample membership label",
    "inclusion_reason": "Inclusion reason",
    "exclusion_reason": "Exclusion reason",
    "source_primary": "Primary source",
    "source_secondary": "Secondary source",
    "source_seed": "Seed source",
    "evidence_urls": "Evidence URLs",
    "source_count": "Number of contributing sources",
    "birth_date_confidence": "Birth date confidence",
    "birth_place_confidence": "Birth place confidence",
    "genre_confidence": "Genre confidence",
    "manual_review_needed": "Needs manual review",
    "notes": "Processing notes",
    "name_key": "Normalized name key",
    "in_songfile": "Present in song file",
    "in_country_restricted": "Present in restricted country file",
    "in_country_master": "Present in master country file",
    "in_adjacent_seed_pool": "Present in adjacent seed pool",
    "source_membership_count": "Number of source memberships",
    "artist_name_songfile_example": "Observed song-file artist name",
    "adjacent_genre_bucket": "Adjacent genre bucket",
    "adjacent_source_group": "Adjacent source group",
    "core_union_flag": "Country-core union flag",
    "birth_admin_labels": "Birth place admin labels",
    "wikipedia_categories": "Wikipedia categories",
    "wp_birth_place_raw": "Wikipedia raw birth place",
    "wp_birth_year": "Wikipedia birth year",
    "wp_birth_date": "Wikipedia birth date",
    "birth_date": "Birth date or start date",
    "birth_place_raw": "Raw birth or origin place text",
    "birth_country_raw": "Raw birth country text",
}

COUNTRY_ALIASES = {
    "us": "United States",
    "u s": "United States",
    "usa": "United States",
    "united states": "United States",
    "united states of america": "United States",
    "ca": "Canada",
    "canada": "Canada",
    "au": "Australia",
    "australia": "Australia",
    "nz": "New Zealand",
    "new zealand": "New Zealand",
    "gb": "United Kingdom",
    "united kingdom": "United Kingdom",
    "uk": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    "northern ireland": "United Kingdom",
    "ie": "Ireland",
    "ireland": "Ireland",
    "fr": "France",
    "france": "France",
    "de": "Germany",
    "germany": "Germany",
    "nl": "Netherlands",
    "netherlands": "Netherlands",
    "kingdom of the netherlands": "Netherlands",
    "it": "Italy",
    "italy": "Italy",
    "jm": "Jamaica",
    "jamaica": "Jamaica",
    "no": "Norway",
    "norway": "Norway",
    "se": "Sweden",
    "sweden": "Sweden",
    "in": "India",
    "india": "India",
}

NON_US_SUBDIVISIONS = {
    "Alberta",
    "British Columbia",
    "Manitoba",
    "New Brunswick",
    "Newfoundland and Labrador",
    "Nova Scotia",
    "Ontario",
    "Prince Edward Island",
    "Quebec",
    "Saskatchewan",
    "Queensland",
    "New South Wales",
    "South Australia",
    "Western Australia",
    "Victoria",
    "Tasmania",
    "Northern Territory",
    "Australian Capital Territory",
    "England",
    "Scotland",
    "Wales",
    "Northern Ireland",
}

WEB_CONFIRMED_OVERRIDES = {
    "Catherine McGrath": {
        "birth_date": "1997-06-09",
        "birth_year": 1997,
        "birth_state": "Northern Ireland",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace/date: Wikipedia",
    },
    "Keith Urban": {
        "birth_state": "Northland",
        "birth_country": "New Zealand",
        "notes_append": "web-confirmed birthplace region: Britannica/Wikipedia",
    },
    "Sam Palladio": {
        "birth_state": "Kent",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "John McNicholl": {
        "birth_city": "Foreglen",
        "birth_state": "Northern Ireland",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "John Landry": {
        "birth_date": "1969-12-22",
        "birth_year": 1969,
        "birth_state": "Quebec",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birthplace/date: Wikipedia",
    },
    "Matt Lang": {
        "birth_state": "Quebec",
        "birth_country": "Canada",
        "birth_city": "Maniwaki",
        "force_keys": ["birth_city"],
        "notes_append": "web-confirmed birthplace region: Wikipedia/official site",
    },
    "Mary Duff": {
        "birth_state": "County Meath",
        "birth_country": "Ireland",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Mitch Zorn": {
        "birth_city": "Nakusp",
        "birth_state": "British Columbia",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Robyn Ottolini": {
        "birth_date": "1995-07-31",
        "birth_year": 1995,
        "birth_state": "Ontario",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birthplace/date: Wikipedia",
    },
    "Raquel Cole": {
        "birth_date": "1993-03-22",
        "birth_year": 1993,
        "birth_state": "British Columbia",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birthplace/date: Wikipedia",
    },
    "Griffen Palmer": {
        "birth_year": 1995,
        "birth_state": "Ontario",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birthplace/year: official site + Wikipedia disambiguation",
    },
    "Gil Grand": {
        "birth_state": "Ontario",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birthplace region: Wikipedia/Merritt Walk of Stars",
    },
    "Sean McConnell": {
        "birth_year": 1985,
        "notes_append": "web-confirmed birth year inferred from Wikipedia biography: debut album in 2000 at age 15",
    },
    "Logan Mize": {
        "birth_year": 1985,
        "birth_city": "Wichita",
        "birth_state": "Kansas",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace/year: Famous Birthdays + artist bio",
    },
    "Karen Staley": {
        "birth_city": "Weirton",
        "birth_state": "West Virginia",
        "birth_country": "United States",
        "force_keys": ["birth_state"],
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Jonny Fritz": {
        "birth_city": "Missoula",
        "birth_state": "Montana",
        "birth_country": "United States",
        "force_keys": ["birth_city"],
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Evan Bartels": {
        "birth_city": "Tobias",
        "birth_state": "Nebraska",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Cary Morin": {
        "birth_city": "Billings",
        "birth_state": "Montana",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Karen Harding": {
        "birth_state": "England",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Phil Lee": {
        "birth_state": "England",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "John Barry": {
        "birth_state": "England",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Adam Barry": {
        "birth_state": "England",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Zach Bryan": {
        "birth_state": "Okinawa Prefecture",
        "birth_country": "Japan",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Ida Mae": {
        "birth_state": "England",
        "birth_country": "United Kingdom",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Leahy": {
        "birth_state": "Ontario",
        "birth_country": "Canada",
        "notes_append": "web-confirmed origin region: Wikipedia",
    },
    "River Town Saints": {
        "birth_state": "Ontario",
        "birth_country": "Canada",
        "notes_append": "web-confirmed origin region: Wikipedia",
    },
    "Cold Creek County": {
        "birth_year": 2013,
        "birth_city": "Brighton",
        "birth_state": "Ontario",
        "birth_country": "Canada",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "The Isaacs": {
        "birth_year": 1987,
        "birth_city": "Hendersonville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year active: Wikipedia",
    },
    "Infamous Stringdusters": {
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin region: Wikipedia",
    },
    "Lasse Stefanz": {
        "birth_state": "Skane County",
        "birth_country": "Sweden",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Roger Springer": {
        "birth_date": "1962-06-15",
        "birth_year": 1962,
        "birth_city": "Caddo",
        "birth_state": "Oklahoma",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace/date: Famous Birthdays + biographical encyclopedia",
    },
    "Roger Alan Wade": {
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed origin region: Wikipedia",
    },
    "Flatt and Scruggs": {
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed duo origin region: Wikipedia",
    },
    "The Road Hammers": {
        "birth_year": 2004,
        "birth_country": "Canada",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Jamestown Revival": {
        "birth_year": 2010,
        "birth_city": "Magnolia",
        "birth_state": "Texas",
        "birth_country": "United States",
        "notes_append": "web-confirmed duo origin/year formed: press profile",
    },
    "The Wagoneers": {
        "birth_year": 1986,
        "birth_city": "Austin",
        "birth_state": "Texas",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "The Rowans": {
        "birth_year": 1970,
        "birth_city": "Wayland",
        "birth_state": "Massachusetts",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Psychograss": {
        "birth_year": 1993,
        "birth_country": "United States",
        "notes_append": "web-confirmed group formed year/country: Wikipedia",
    },
    "Rocket Club": {
        "birth_year": 2008,
        "birth_city": "Minneapolis-Saint Paul",
        "birth_state": "Minnesota",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year active: Wikipedia",
    },
    "Lavender Country": {
        "birth_year": 1972,
        "birth_city": "Seattle",
        "birth_state": "Washington",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Pitchfork interview + discography history",
    },
    "Parmalee": {
        "birth_year": 2001,
        "birth_city": "Parmele",
        "birth_state": "North Carolina",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: fandom biography",
    },
    "Poor Man's Poison": {
        "birth_year": 2009,
        "birth_city": "Hanford",
        "birth_state": "California",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "4 Runner": {
        "birth_year": 1993,
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Tigirlily Gold": {
        "birth_year": 2014,
        "birth_city": "Hazen",
        "birth_state": "North Dakota",
        "birth_country": "United States",
        "notes_append": "web-confirmed duo origin/year active: Wikipedia",
    },
    "Rio Grand": {
        "birth_year": 2006,
        "birth_state": "Texas",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Big Richard": {
        "birth_year": 2021,
        "birth_state": "Colorado",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year active: Wikipedia",
    },
    "Spirit Family Reunion": {
        "birth_year": 2010,
        "birth_city": "Brooklyn",
        "birth_state": "New York",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Southern Raised": {
        "birth_year": 2007,
        "birth_city": "Branson",
        "birth_state": "Missouri",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: official site + German Wikipedia",
    },
    "The Shelton Brothers": {
        "birth_year": 1933,
        "birth_state": "Texas",
        "birth_country": "United States",
        "notes_append": "web-confirmed group base/year active: Wikipedia",
    },
    "The Hodges Brothers": {
        "birth_year": 1952,
        "birth_state": "Mississippi",
        "birth_country": "United States",
        "notes_append": "web-confirmed group home base/first single year: Wikipedia",
    },
    "Authentic Unlimited": {
        "birth_year": 2022,
        "birth_country": "United States",
        "notes_append": "web-confirmed group formed year/country: Wikipedia + Bluegrass Today",
    },
    "Jimmy & Johnny": {
        "birth_year": 1951,
        "birth_country": "United States",
        "notes_append": "web-confirmed duo active by 1951: Wikipedia",
    },
    "Jim Hurst": {
        "birth_country": "United States",
        "notes_append": "web-confirmed nationality: Wikipedia",
    },
    "Lex Rijger": {
        "birth_date": "1944-12-02",
        "birth_year": 1944,
        "birth_country": "Suriname",
        "notes_append": "web-confirmed birthplace/date: biographical references via family profile",
    },
    "Tim Hus": {
        "birth_year": 1979,
        "birth_state": "British Columbia",
        "birth_country": "Canada",
        "notes_append": "web-confirmed birth year/place: Wikipedia",
    },
}

DISCOGS_API = "https://api.discogs.com"
ITUNES_SEARCH_API = "https://itunes.apple.com/search"

NATIONALITY_TO_COUNTRY = {
    "american": "United States",
    "canadian": "Canada",
    "australian": "Australia",
    "british": "United Kingdom",
    "english": "United Kingdom",
    "scottish": "United Kingdom",
    "welsh": "United Kingdom",
    "irish": "Ireland",
    "swedish": "Sweden",
    "jamaican": "Jamaica",
    "italian": "Italy",
    "indian": "India",
    "norwegian": "Norway",
    "dutch": "Netherlands",
    "french": "France",
    "german": "Germany",
}


def normalize_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def clean_text(value: object) -> str:
    return builder.clean_text(value)


def nonempty(value: object) -> str:
    return clean_text(value)


def count_nonmissing(row: pd.Series, columns: Iterable[str]) -> int:
    total = 0
    for col in columns:
        value = row.get(col)
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        total += 1
    return total


def dedupe_by_name(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["name_key"] = work["name_primary"].map(normalize_name)
    work = work[work["name_key"].ne("")].copy()
    work["completeness_score"] = work.apply(
        lambda row: count_nonmissing(row, ["name_primary", "birth_year", "birth_city", "birth_state", "birth_country", "wikidata_qid", "musicbrainz_mbid"]),
        axis=1,
    )
    work["source_membership_count"] = pd.to_numeric(work.get("source_membership_count", 0), errors="coerce").fillna(0)
    work["country_relevance_score"] = pd.to_numeric(work.get("country_relevance_score", 0), errors="coerce").fillna(0)
    work["manual_review_needed"] = pd.to_numeric(work.get("manual_review_needed", 0), errors="coerce").fillna(0)
    work = work.sort_values(
        ["completeness_score", "source_membership_count", "country_relevance_score", "manual_review_needed"],
        ascending=[False, False, False, True],
    )
    work = work.drop_duplicates(subset=["name_key"], keep="first").copy()
    work = work.drop(columns=["completeness_score"])
    work["artist_id"] = [f"artist_{index:06d}" for index in range(1, len(work) + 1)]
    return work.reset_index(drop=True)


def apply_web_confirmed_overrides(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    for idx, row in enriched.iterrows():
        name = nonempty(row.get("name_primary", ""))
        override = WEB_CONFIRMED_OVERRIDES.get(name)
        if not override:
            continue
        for key in ["birth_date", "birth_year", "birth_city", "birth_state", "birth_country"]:
            if key in override:
                current = row.get(key)
                force_keys = set(override.get("force_keys", []))
                should_replace = pd.isna(current) or nonempty(current) == ""
                if key in force_keys:
                    should_replace = True
                if key == "birth_city" and is_suspicious_city(current, row.get("name_primary", "")):
                    should_replace = True
                if key == "birth_country" and nonempty(current) in {"United States", "US", "USA"} and nonempty(override[key]) not in {"United States", "US", "USA"}:
                    should_replace = True
                if should_replace:
                    enriched.at[idx, key] = override[key]
        note = override.get("notes_append", "")
        if note:
            current_notes = nonempty(row.get("notes", ""))
            if note not in current_notes:
                enriched.at[idx, "notes"] = "|".join(piece for piece in [current_notes, note] if piece)
    enriched["birth_state_abbr"] = enriched["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return enriched


def strip_leading_date(value: str) -> str:
    text = nonempty(value)
    if not text:
        return ""
    text = re.sub(r"^\+?\d{4}-\d{2}-\d{2}\s*", "", text).strip()
    text = re.sub(r"^\d{1,2}\s+[A-Za-z]+\s+\d{4}\s*", "", text).strip()
    text = re.sub(r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}\s*", "", text).strip()
    return text.lstrip(", ").strip()


def canonical_country(value: str) -> str:
    key = normalize_name(value)
    return COUNTRY_ALIASES.get(key, nonempty(value))


def parse_place_components(raw_place: str, current_country: str) -> dict:
    raw_place = strip_leading_date(raw_place)
    if not raw_place:
        return {"birth_city": "", "birth_state": "", "birth_country": canonical_country(current_country)}

    parts = [part.strip(" .") for part in raw_place.split(",") if part.strip(" .")]
    if not parts:
        return {"birth_city": "", "birth_state": "", "birth_country": canonical_country(current_country)}

    country = canonical_country(current_country)
    tail_country = canonical_country(parts[-1])
    if tail_country in set(COUNTRY_ALIASES.values()):
        country = tail_country
        parts = parts[:-1]

    state = ""
    if parts:
        candidate = parts[-1]
        if candidate in builder.US_STATE_ABBR:
            state = candidate
            country = country or "United States"
            parts = parts[:-1]
        elif candidate in NON_US_SUBDIVISIONS:
            state = candidate
            parts = parts[:-1]

    city = parts[0] if parts else ""
    return {"birth_city": city, "birth_state": state, "birth_country": country}


def apply_place_parsing(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    enriched["birth_date"] = enriched["birth_date"].map(nonempty)
    enriched["birth_place_raw"] = enriched["birth_place_raw"].map(nonempty)
    enriched["birth_country"] = enriched["birth_country"].map(canonical_country)

    for idx, row in enriched.iterrows():
        birth_date = nonempty(row.get("birth_date", ""))
        if pd.isna(row.get("birth_year")) and birth_date:
            year = builder.extract_year_hint(birth_date)
            if year:
                enriched.at[idx, "birth_year"] = year

        parsed = parse_place_components(row.get("birth_place_raw", ""), row.get("birth_country", ""))
        city = nonempty(row.get("birth_city", ""))
        if not city or re.search(r"\d{4}", city):
            if parsed["birth_city"]:
                enriched.at[idx, "birth_city"] = parsed["birth_city"]
        if not nonempty(row.get("birth_state", "")) and parsed["birth_state"]:
            enriched.at[idx, "birth_state"] = parsed["birth_state"]
        if not nonempty(row.get("birth_country", "")) and parsed["birth_country"]:
            enriched.at[idx, "birth_country"] = parsed["birth_country"]

    enriched["birth_state_abbr"] = enriched["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return enriched


def likely_group_name(name: object) -> bool:
    text = nonempty(name)
    if not text:
        return False
    return bool(
        re.search(
            r"^(The\s)|\&| Brothers| Band| Family| Sisters| Singers| Trio| Quartet| Ramblers| Minstrels| Party| Ensemble| Twenties| Boys| Girls| Club| Raised| Country$| Unlimited$",
            text,
            flags=re.I,
        )
    )


def parse_years_active_start(text: object) -> int | None:
    raw = nonempty(text)
    match = re.search(r"(18\d{2}|19\d{2}|20\d{2})", raw)
    return int(match.group(1)) if match else None


def parse_wikipedia_origin_years(s: requests.Session, wikipedia_url: str) -> dict:
    response = s.get(wikipedia_url, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    infobox = soup.select_one("table.infobox")
    result = {
        "origin_place_raw": "",
        "origin_start_year": "",
    }
    if not infobox:
        return result

    origin_text = ""
    years_active_text = ""
    for tr in infobox.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = builder.clean_text(th.get_text(" ", strip=True))
        if label == "Origin":
            origin_text = builder.clean_text(td.get_text(" ", strip=True))
        elif label in {"Years active", "Years Active"}:
            years_active_text = builder.clean_text(td.get_text(" ", strip=True))

    result["origin_place_raw"] = origin_text
    result["origin_start_year"] = str(parse_years_active_start(years_active_text) or "")
    return result


def enrich_from_wikipedia_origin_years(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["wikipedia_url"].fillna("").astype(str).str.strip().ne("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["wikipedia_url", "origin_place_raw", "origin_start_year"])

    done = set(checkpoint_df["wikipedia_url"].fillna("").astype(str))
    pending = needs[~needs["wikipedia_url"].isin(done)].copy()
    s = builder.session()
    new_rows: List[dict] = []
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        url = nonempty(row["wikipedia_url"])
        if not url:
            continue
        try:
            parsed = parse_wikipedia_origin_years(s, url)
        except Exception as exc:
            builder.log(f"Wikipedia origin/years enrichment failed for {url}: {exc}")
            parsed = {"origin_place_raw": "", "origin_start_year": ""}
        new_rows.append({"wikipedia_url": url, **parsed})
        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["wikipedia_url"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved Wikipedia origin/years checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    merged = enriched.merge(checkpoint_df, on="wikipedia_url", how="left")
    for idx, row in merged.iterrows():
        is_group = likely_group_name(row.get("name_primary", ""))
        if is_group and pd.isna(row.get("birth_year")):
            year = builder.extract_year_hint(row.get("origin_start_year", ""))
            if year:
                merged.at[idx, "birth_year"] = year
        if nonempty(row.get("birth_state", "")) == "" and nonempty(row.get("origin_place_raw", "")):
            parsed = parse_place_components(row.get("origin_place_raw", ""), row.get("birth_country", ""))
            if parsed["birth_city"] and nonempty(row.get("birth_city", "")) == "":
                merged.at[idx, "birth_city"] = parsed["birth_city"]
            if parsed["birth_state"]:
                merged.at[idx, "birth_state"] = parsed["birth_state"]
            if parsed["birth_country"] and nonempty(row.get("birth_country", "")) == "":
                merged.at[idx, "birth_country"] = parsed["birth_country"]
    merged["birth_state_abbr"] = merged["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return merged.drop(columns=["origin_place_raw", "origin_start_year"])


def is_suspicious_city(city: object, name: object) -> bool:
    text = nonempty(city)
    if not text:
        return False
    lowered = text.lower()
    if any(token in lowered for token in ["attended", "college", "lives in", "lives ", " and ", "[", "]", "born "]):
        return True
    if len(text.split()) > 5:
        return True
    if re.search(r"\d{4}", text):
        return True
    name_key = normalize_name(name)
    city_key = normalize_name(text)
    if name_key and len(city_key.split()) > 2 and name_key in city_key:
        return True
    if name_key:
        name_parts = name_key.split()
        city_parts = city_key.split()
        overlap = sum(1 for part in name_parts if part in city_parts)
        if overlap >= 2:
            return True
    return False


def clean_birth_geography(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned["birth_country"] = cleaned["birth_country"].map(canonical_country)
    if "birth_country_raw" in cleaned.columns:
        cleaned["birth_country_raw"] = cleaned["birth_country_raw"].map(canonical_country)

    for idx, row in cleaned.iterrows():
        city = nonempty(row.get("birth_city", ""))
        state = nonempty(row.get("birth_state", ""))
        country = canonical_country(row.get("birth_country", ""))
        place_raw = nonempty(row.get("birth_place_raw", ""))
        if is_suspicious_city(city, row.get("name_primary", "")):
            cleaned.at[idx, "birth_city"] = ""
        if city and city in {state, country}:
            cleaned.at[idx, "birth_city"] = ""
        if state and country and canonical_country(state) == country and state not in builder.US_STATE_ABBR and state not in NON_US_SUBDIVISIONS:
            cleaned.at[idx, "birth_state"] = ""
        if not country and place_raw:
            parsed = parse_place_components(place_raw, "")
            if parsed["birth_country"]:
                cleaned.at[idx, "birth_country"] = parsed["birth_country"]
        if not nonempty(cleaned.at[idx, "birth_city"]) and place_raw:
            parsed = parse_place_components(place_raw, cleaned.at[idx, "birth_country"])
            if parsed["birth_city"] and not is_suspicious_city(parsed["birth_city"], row.get("name_primary", "")):
                cleaned.at[idx, "birth_city"] = parsed["birth_city"]
            if not nonempty(cleaned.at[idx, "birth_state"]) and parsed["birth_state"]:
                cleaned.at[idx, "birth_state"] = parsed["birth_state"]
            if not nonempty(cleaned.at[idx, "birth_country"]) and parsed["birth_country"]:
                cleaned.at[idx, "birth_country"] = parsed["birth_country"]

    cleaned["birth_state_abbr"] = cleaned["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return cleaned


def resolve_missing_qids_from_names(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["wikidata_qid"].fillna("").astype(str).str.strip().eq("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["name_primary", "resolved_wikidata_qid"])

    done = set(checkpoint_df["name_primary"].fillna("").astype(str))
    pending_names = [name for name in needs["name_primary"].fillna("").astype(str).tolist() if name and name not in done]
    if pending_names:
        s = builder.session()
        new_rows: List[dict] = []
        for batch_start in range(0, len(pending_names), 20):
            batch = pending_names[batch_start : batch_start + 20]
            try:
                batch_map = builder.wikipedia_titles_to_qids(s, batch)
            except Exception as exc:
                builder.log(f"Batch Wikipedia title-to-QID lookup failed for {len(batch)} names: {exc}")
                batch_map = {name: "" for name in batch}
            for name in batch:
                new_rows.append({"name_primary": name, "resolved_wikidata_qid": batch_map.get(name, "") or ""})
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["name_primary"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved name-to-QID checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

        unresolved_mask = checkpoint_df["resolved_wikidata_qid"].fillna("").astype(str).str.strip().eq("")
        unresolved_names = checkpoint_df.loc[unresolved_mask, "name_primary"].dropna().astype(str).tolist()
        consecutive_search_failures = 0
        for index, name in enumerate(unresolved_names, start=1):
            try:
                qid = builder.search_wikidata_qid(s, name) or ""
                consecutive_search_failures = 0
            except Exception as exc:
                builder.log(f"Wikidata title search failed for {name}: {exc}")
                qid = ""
                consecutive_search_failures += 1
            checkpoint_df.loc[checkpoint_df["name_primary"].eq(name), "resolved_wikidata_qid"] = qid
            if index % 25 == 0 or index == len(unresolved_names):
                checkpoint_df.to_csv(checkpoint_path, index=False)
                builder.log(f"Updated name-to-QID checkpoint at {len(checkpoint_df)} rows")
            if consecutive_search_failures >= 10:
                builder.log("Stopping single-title QID search early after repeated failures/rate limits")
                break

    merged = enriched.merge(checkpoint_df, on="name_primary", how="left")
    merged["wikidata_qid"] = merged.apply(
        lambda row: nonempty(row.get("wikidata_qid", "")) or nonempty(row.get("resolved_wikidata_qid", "")),
        axis=1,
    )
    return merged.drop(columns=["resolved_wikidata_qid"])


def enrich_from_wikipedia_sources(df: pd.DataFrame, checkpoint_prefix: str) -> pd.DataFrame:
    enriched = df.copy()
    for col in ["birth_admin_labels", "wikipedia_categories", "wp_birth_place_raw", "wp_birth_year", "wp_birth_date"]:
        if col not in enriched.columns:
            enriched[col] = ""
    needs_mask = (
        enriched["wikipedia_url"].fillna("").astype(str).str.strip().ne("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
            | enriched["name_primary"].fillna("").astype(str).str.strip().eq("")
        )
    )
    needs = enriched.loc[needs_mask].copy()
    if needs.empty:
        return enriched
    s = builder.session()
    try:
        needs = builder.fill_missing_wikipedia_urls_from_sitelinks(s, needs)
    except Exception as exc:
        builder.log(f"fill_missing_wikipedia_urls_from_sitelinks failed for {checkpoint_prefix}: {exc}")
    try:
        needs = builder.enrich_from_wikipedia(
            s,
            needs,
            INTERMEDIATE_DIR / f"{checkpoint_prefix}_wikipedia_enrichment.csv",
        )
    except Exception as exc:
        builder.log(f"Wikipedia infobox enrichment failed for {checkpoint_prefix}: {exc}")
    try:
        needs = builder.enrich_from_wikipedia_lead(
            s,
            needs,
            INTERMEDIATE_DIR / f"{checkpoint_prefix}_wikipedia_lead.csv",
        )
    except Exception as exc:
        builder.log(f"Wikipedia lead enrichment failed for {checkpoint_prefix}: {exc}")
    needs = collapse_merge_columns(needs)
    needs = needs.set_index("artist_id")
    merged = enriched.set_index("artist_id")
    shared_columns = [col for col in needs.columns if col in merged.columns]
    for col in shared_columns:
        source = needs[col]
        target = merged[col]
        if str(getattr(target, "dtype", "")) == "object":
            target_clean = target.fillna("").astype(str).str.strip()
            source_clean = source.fillna("").astype(str).str.strip()
            merged[col] = target.where(target_clean.ne(""), source)
        else:
            merged[col] = target.where(target.notna(), source)
    return merged.reset_index()


def parse_discogs_profile(profile: str) -> dict:
    text = nonempty(profile)
    if not text:
        return {"birth_year": None, "birth_city": "", "birth_state": "", "birth_country": ""}

    lower = text.lower()
    birth_year = builder.extract_year_hint(text)
    birth_city = ""
    birth_state = ""
    birth_country = ""

    for adjective, country in NATIONALITY_TO_COUNTRY.items():
        if re.search(rf"\b{re.escape(adjective)}\b", lower):
            birth_country = country
            break

    born_match = re.search(r"\bborn\b(?:[^.]{0,80}?)\bin\s+([^.;]+)", text, flags=re.IGNORECASE)
    from_match = re.search(r"\bfrom\s+([^.;]+)", text, flags=re.IGNORECASE)
    location_text = born_match.group(1).strip() if born_match else (from_match.group(1).strip() if from_match else "")
    parsed = parse_place_components(location_text, birth_country)
    birth_city = parsed["birth_city"]
    birth_state = parsed["birth_state"]
    birth_country = parsed["birth_country"] or birth_country

    return {
        "birth_year": birth_year,
        "birth_city": birth_city,
        "birth_state": birth_state,
        "birth_country": birth_country,
    }


def discogs_headers() -> dict:
    return {"User-Agent": "SoundOfCultureResearch/1.0 (local pipeline)"}


def enrich_with_discogs_and_itunes(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["birth_year"].isna()
        | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
        | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(
            columns=[
                "name_primary",
                "discogs_birth_year",
                "discogs_birth_city",
                "discogs_birth_state",
                "discogs_birth_country",
                "discogs_url",
                "itunes_artist_url",
            ]
        )

    done = set(checkpoint_df["name_primary"].fillna("").astype(str))
    pending = needs[~needs["name_primary"].isin(done)].copy()
    if pending.empty:
        merged = enriched.merge(checkpoint_df, on="name_primary", how="left")
        return apply_discogs_itunes_merge(merged)

    s = requests.Session()
    new_rows: List[dict] = []
    consecutive_discogs_failures = 0
    consecutive_itunes_failures = 0
    discogs_enabled = True
    itunes_enabled = True
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        name = nonempty(row["name_primary"])
        discogs_birth_year = None
        discogs_birth_city = ""
        discogs_birth_state = ""
        discogs_birth_country = ""
        discogs_url = ""
        itunes_artist_url = ""

        if discogs_enabled:
            try:
                search = s.get(
                    f"{DISCOGS_API}/database/search",
                    params={"q": name, "type": "artist", "per_page": 5, "page": 1},
                    headers=discogs_headers(),
                    timeout=20,
                )
                search.raise_for_status()
                results = search.json().get("results", [])
                best = None
                target_key = normalize_name(name)
                for item in results:
                    title_key = normalize_name(item.get("title", ""))
                    if title_key == target_key:
                        best = item
                        break
                if not best and results:
                    best = results[0]
                if best and best.get("resource_url"):
                    artist_resp = s.get(best["resource_url"], headers=discogs_headers(), timeout=20)
                    artist_resp.raise_for_status()
                    artist_payload = artist_resp.json()
                    parsed = parse_discogs_profile(artist_payload.get("profile", ""))
                    discogs_birth_year = parsed["birth_year"]
                    discogs_birth_city = parsed["birth_city"]
                    discogs_birth_state = parsed["birth_state"]
                    discogs_birth_country = parsed["birth_country"]
                    discogs_url = nonempty(best.get("uri", ""))
                consecutive_discogs_failures = 0
            except Exception as exc:
                builder.log(f"Discogs enrichment failed for {name}: {exc}")
                consecutive_discogs_failures += 1
                if consecutive_discogs_failures >= 10:
                    discogs_enabled = False
                    builder.log("Discogs enrichment disabled for the rest of this run after repeated failures/rate limits")

        if itunes_enabled:
            try:
                itunes = s.get(
                    ITUNES_SEARCH_API,
                    params={"term": name, "entity": "musicArtist", "attribute": "artistTerm", "limit": 5},
                    timeout=20,
                )
                itunes.raise_for_status()
                results = itunes.json().get("results", [])
                best = None
                target_key = normalize_name(name)
                for item in results:
                    title_key = normalize_name(item.get("artistName", ""))
                    if title_key == target_key:
                        best = item
                        break
                if not best and results:
                    best = results[0]
                if best:
                    itunes_artist_url = nonempty(best.get("artistLinkUrl", ""))
                consecutive_itunes_failures = 0
            except Exception as exc:
                builder.log(f"iTunes/Apple Music lookup failed for {name}: {exc}")
                consecutive_itunes_failures += 1
                if consecutive_itunes_failures >= 10:
                    itunes_enabled = False
                    builder.log("iTunes/Apple Music enrichment disabled for the rest of this run after repeated failures/rate limits")

        new_rows.append(
            {
                "name_primary": name,
                "discogs_birth_year": discogs_birth_year,
                "discogs_birth_city": discogs_birth_city,
                "discogs_birth_state": discogs_birth_state,
                "discogs_birth_country": discogs_birth_country,
                "discogs_url": discogs_url,
                "itunes_artist_url": itunes_artist_url,
            }
        )

        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["name_primary"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved Discogs/iTunes checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    merged = enriched.merge(checkpoint_df, on="name_primary", how="left")
    return apply_discogs_itunes_merge(merged)


def apply_discogs_itunes_merge(df: pd.DataFrame) -> pd.DataFrame:
    merged = df.copy()
    merged["birth_year"] = merged.apply(
        lambda row: row["birth_year"] if pd.notna(row["birth_year"]) else builder.extract_year_hint(row.get("discogs_birth_year", "")),
        axis=1,
    )
    for source_col, target_col in [
        ("discogs_birth_city", "birth_city"),
        ("discogs_birth_state", "birth_state"),
        ("discogs_birth_country", "birth_country"),
    ]:
        merged[target_col] = merged.apply(
            lambda row: nonempty(row.get(target_col, "")) or canonical_country(row.get(source_col, "")),
            axis=1,
        )
    merged["birth_state_abbr"] = merged["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    merged["evidence_urls"] = merged.apply(
        lambda row: "|".join(piece for piece in [nonempty(row.get("evidence_urls", "")), nonempty(row.get("discogs_url", "")), nonempty(row.get("itunes_artist_url", ""))] if piece),
        axis=1,
    )
    merged["notes"] = merged.apply(
        lambda row: "|".join(
            piece
            for piece in [
                nonempty(row.get("notes", "")),
                "birth data enriched from Discogs" if any(nonempty(row.get(col, "")) for col in ["discogs_birth_year", "discogs_birth_city", "discogs_birth_state", "discogs_birth_country"]) else "",
                "artist confirmed via iTunes/Apple Music" if nonempty(row.get("itunes_artist_url", "")) else "",
            ]
            if piece
        ),
        axis=1,
    )
    drop_cols = [col for col in ["discogs_birth_year", "discogs_birth_city", "discogs_birth_state", "discogs_birth_country", "discogs_url", "itunes_artist_url"] if col in merged.columns]
    return merged.drop(columns=drop_cols)


def generic_musicbrainz_area_context(s: requests.Session, area_id: str, cache: Dict[str, dict]) -> dict:
    if not area_id:
        return {"birth_city": "", "birth_state": "", "birth_country": ""}
    if area_id in cache:
        return cache[area_id]

    payload = builder.request_json(
        s,
        f"{builder.MUSICBRAINZ_API}/area/{area_id}",
        params={"fmt": "json", "inc": "area-rels"},
        pause=1.05,
    )
    area_name = nonempty(payload.get("name", ""))
    area_type = nonempty(payload.get("type", ""))
    city = area_name if area_type == "City" else ""
    state = area_name if area_name in NON_US_SUBDIVISIONS or area_name in builder.US_STATE_ABBR else ""
    country = ""

    for code in payload.get("iso-3166-1-codes", []) or []:
        country = canonical_country(code)
    if country in {"US", "GB"}:
        country = canonical_country(country)

    for relation in payload.get("relations", []) or []:
        rel_area = relation.get("area", {}) or {}
        rel_name = nonempty(rel_area.get("name", ""))
        rel_type = nonempty(rel_area.get("type", ""))
        for code in rel_area.get("iso-3166-1-codes", []) or []:
            country = canonical_country(code) or country
        if rel_type == "Country" and rel_name:
            country = canonical_country(rel_name)
        elif rel_name in builder.US_STATE_ABBR or rel_name in NON_US_SUBDIVISIONS:
            state = state or rel_name

    result = {"birth_city": city, "birth_state": state, "birth_country": country}
    cache[area_id] = result
    return result


def enrich_from_musicbrainz_generic(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["musicbrainz_mbid"].fillna("").astype(str).str.strip().ne("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["musicbrainz_mbid", "mbx_birth_year", "mbx_birth_city", "mbx_birth_state", "mbx_birth_country"])
    done = set(checkpoint_df["musicbrainz_mbid"].fillna("").astype(str))
    pending = needs[~needs["musicbrainz_mbid"].isin(done)].copy()
    area_cache: Dict[str, dict] = {}
    new_rows: List[dict] = []
    s = builder.session()

    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        mbid = nonempty(row["musicbrainz_mbid"])
        try:
            payload = builder.request_json(s, f"{builder.MUSICBRAINZ_API}/artist/{mbid}", params={"fmt": "json"}, pause=1.05)
            begin = nonempty(payload.get("life-span", {}).get("begin", ""))
            year = builder.extract_year_hint(begin) or ""
            begin_area = payload.get("begin-area", {}) or {}
            area = payload.get("area", {}) or {}
            ctx = generic_musicbrainz_area_context(s, nonempty(begin_area.get("id", "")), area_cache) if begin_area.get("id") else {
                "birth_city": "",
                "birth_state": "",
                "birth_country": "",
            }
            country = ctx["birth_country"] or canonical_country(payload.get("country", "")) or canonical_country(area.get("name", ""))
            city = ctx["birth_city"] or nonempty(begin_area.get("name", ""))
            state = ctx["birth_state"]
            if city in {country, state}:
                city = ""
            new_rows.append(
                {
                    "musicbrainz_mbid": mbid,
                    "mbx_birth_year": year,
                    "mbx_birth_city": city,
                    "mbx_birth_state": state,
                    "mbx_birth_country": country,
                }
            )
        except Exception as exc:
            builder.log(f"Generic MusicBrainz enrichment failed for {mbid}: {exc}")
            new_rows.append(
                {
                    "musicbrainz_mbid": mbid,
                    "mbx_birth_year": "",
                    "mbx_birth_city": "",
                    "mbx_birth_state": "",
                    "mbx_birth_country": "",
                }
            )
        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["musicbrainz_mbid"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved generic MusicBrainz checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    merged = enriched.merge(checkpoint_df, on="musicbrainz_mbid", how="left")
    merged["birth_year"] = merged.apply(
        lambda row: row["birth_year"] if pd.notna(row["birth_year"]) else builder.extract_year_hint(row.get("mbx_birth_year", "")),
        axis=1,
    )
    for source_col, target_col in [
        ("mbx_birth_city", "birth_city"),
        ("mbx_birth_state", "birth_state"),
        ("mbx_birth_country", "birth_country"),
    ]:
        merged[target_col] = merged.apply(
            lambda row: nonempty(row[target_col]) or nonempty(row[source_col]),
            axis=1,
        )
    merged["birth_state_abbr"] = merged["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return merged.drop(columns=["mbx_birth_year", "mbx_birth_city", "mbx_birth_state", "mbx_birth_country"])


def enrich_from_wikidata_qids(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    for col in ["wd_birth_date", "wd_birth_place_raw", "wd_birth_country_raw"]:
        if col in enriched.columns:
            enriched = enriched.drop(columns=[col])
    needs = enriched[
        enriched["wikidata_qid"].fillna("").astype(str).str.startswith("Q")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["wikidata_qid", "birth_date", "birth_place_raw", "birth_country_raw"])
    done = set(checkpoint_df["wikidata_qid"].fillna("").astype(str))
    pending = [qid for qid in needs["wikidata_qid"].drop_duplicates().tolist() if qid not in done]
    s = builder.session()
    new_rows: List[dict] = []
    consecutive_failures = 0
    for index, qid in enumerate(pending, start=1):
        try:
            rows = builder.wikidata_detail_rows_from_entities(s, [qid])
            row = rows[0] if rows else {"wikidata_qid": qid}
            consecutive_failures = 0
        except Exception as exc:
            builder.log(f"Targeted QID enrichment failed for {qid}: {exc}")
            row = {"wikidata_qid": qid}
            consecutive_failures += 1
        new_rows.append(
            {
                "wikidata_qid": qid,
                "wd_birth_date": nonempty(row.get("birth_date", "")),
                "wd_birth_place_raw": nonempty(row.get("birth_place_raw", "")),
                "wd_birth_country_raw": nonempty(row.get("birth_country_raw", "")),
            }
        )
        if new_rows and (index % 20 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["wikidata_qid"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved targeted Wikidata checkpoint at {len(checkpoint_df)} rows")
            new_rows = []
        if consecutive_failures >= 10:
            builder.log("Stopping targeted QID enrichment early after repeated failures/rate limits")
            break

    merged = enriched.merge(checkpoint_df, on="wikidata_qid", how="left")
    merged["birth_date"] = merged.apply(lambda row: nonempty(row.get("birth_date", "")) or nonempty(row.get("wd_birth_date", "")), axis=1)
    merged["birth_place_raw"] = merged.apply(lambda row: nonempty(row.get("birth_place_raw", "")) or nonempty(row.get("wd_birth_place_raw", "")), axis=1)
    merged["birth_country"] = merged.apply(lambda row: nonempty(row.get("birth_country", "")) or canonical_country(row.get("wd_birth_country_raw", "")), axis=1)
    merged = apply_place_parsing(merged)
    return merged.drop(columns=["wd_birth_date", "wd_birth_place_raw", "wd_birth_country_raw"])


def coerce_for_stata(df: pd.DataFrame) -> pd.DataFrame:
    coerced = df.copy()
    for col in coerced.columns:
        if coerced[col].dtype == "object":
            coerced[col] = coerced[col].where(coerced[col].notna(), None)
            coerced[col] = coerced[col].map(lambda value: None if value is None else str(value))
    return coerced


def collapse_merge_columns(df: pd.DataFrame) -> pd.DataFrame:
    collapsed = df.copy()
    base_candidates = set()
    for col in collapsed.columns:
        if col.endswith("_x") or col.endswith("_y"):
            base_candidates.add(col[:-2])
    for base in sorted(base_candidates):
        candidates = [col for col in [base, f"{base}_x", f"{base}_y"] if col in collapsed.columns]
        if not candidates:
            continue
        merged = pd.Series([""] * len(collapsed), index=collapsed.index, dtype="object")
        for candidate in candidates:
            series = collapsed[candidate]
            if candidate == "birth_year" or base == "birth_year":
                continue
            series = series.fillna("").astype(str).str.strip()
            merged = merged.mask(merged.ne(""), merged)
            merged = merged.where(merged.ne(""), series)
        if base in collapsed.columns:
            collapsed[base] = collapsed[base].where(collapsed[base].notna(), "")
            if collapsed[base].dtype == "object":
                current = collapsed[base].fillna("").astype(str).str.strip()
                collapsed[base] = current.where(current.ne(""), merged)
        else:
            collapsed[base] = merged
        for candidate in [f"{base}_x", f"{base}_y"]:
            if candidate in collapsed.columns:
                collapsed = collapsed.drop(columns=[candidate])
    return collapsed


def save_dataset(df: pd.DataFrame, basename: str) -> None:
    csv_path = OUTPUT_DIR / f"{basename}.csv"
    dta_path = OUTPUT_DIR / f"{basename}.dta"
    df = collapse_merge_columns(df)
    df = coerce_for_stata(df)
    column_labels = [VARIABLE_LABELS.get(col, col) for col in df.columns]
    data_label = {
        CORE_BASENAME: "Country core artist universe",
        ADJ_BASENAME: "Adjacent-only artist universe",
        FULL_BASENAME: "Country plus adjacent artist universe",
        REVIEW_BASENAME: "Artist universe manual review subset",
    }.get(basename, "Artist universe dataset")
    df.to_csv(csv_path, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
    pyreadstat.write_dta(df, dta_path, column_labels=column_labels, file_label=data_label, version=15)
    builder.log(f"Saved {csv_path.name} and {dta_path.name}")
    if basename == CORE_BASENAME:
        shutil.copy2(csv_path, OUTPUT_DIR / "artist_universe_country_only.csv")
        shutil.copy2(dta_path, OUTPUT_DIR / "artist_universe_country_only.dta")


def write_qc_report(core_df: pd.DataFrame, adj_df: pd.DataFrame, full_df: pd.DataFrame) -> None:
    def miss(df: pd.DataFrame, col: str) -> int:
        if col == "birth_year":
            return int(df[col].isna().sum())
        return int(df[col].fillna("").astype(str).str.strip().eq("").sum())

    lines = [
        "# artist_universe_qc_report",
        "",
        "## Counts",
        f"- core country-union artists: {len(core_df)}",
        f"- adjacent-only artists added: {len(adj_df)}",
        f"- full artist universe: {len(full_df)}",
        "",
        "## Missingness In Full Universe",
    ]
    for col in ["name_primary", "birth_year", "birth_city", "birth_state", "birth_country", "wikidata_qid", "musicbrainz_mbid"]:
        lines.append(f"- {col}: {miss(full_df, col)} missing")
    lines.extend(
        [
            "",
            "## Membership",
            f"- artists linked to current songs file: {int(full_df['in_songfile'].fillna(0).astype(int).sum())}",
            f"- artists in country_artists_restricted: {int(full_df['in_country_restricted'].fillna(0).astype(int).sum())}",
            f"- artists in country_artists_master: {int(full_df['in_country_master'].fillna(0).astype(int).sum())}",
            f"- artists coming from adjacent-genre seed pool: {int(full_df['in_adjacent_seed_pool'].fillna(0).astype(int).sum())}",
        ]
    )
    QC_REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    core_df = collapse_merge_columns(pd.read_csv(OUTPUT_DIR / f"{CORE_BASENAME}.csv"))
    adj_df = collapse_merge_columns(pd.read_csv(OUTPUT_DIR / f"{ADJ_BASENAME}.csv"))
    full_df = collapse_merge_columns(pd.read_csv(OUTPUT_DIR / f"{FULL_BASENAME}.csv"))
    review_df = collapse_merge_columns(pd.read_csv(OUTPUT_DIR / f"{REVIEW_BASENAME}.csv"))

    adj_df = dedupe_by_name(adj_df)
    full_df = dedupe_by_name(full_df)
    core_df = dedupe_by_name(core_df)

    for label, df in [("core", core_df), ("adjacent", adj_df)]:
        builder.log(f"Applying place parsing heuristics to {label}")
        df = apply_place_parsing(df)
        df = clean_birth_geography(df)
        builder.log(f"Resolving missing Wikidata QIDs by artist name for {label}")
        df = resolve_missing_qids_from_names(df, f"artist_universe_{label}_name_to_qid.csv")
        builder.log(f"Running Wikipedia enrichment for {label}")
        df = enrich_from_wikipedia_sources(df, f"artist_universe_{label}")
        builder.log(f"Running Wikipedia origin/years enrichment for {label}")
        df = enrich_from_wikipedia_origin_years(df, f"artist_universe_{label}_wikipedia_origin_years.csv")
        df = apply_place_parsing(df)
        df = clean_birth_geography(df)
        builder.log(f"Running generic MusicBrainz enrichment for {label}")
        df = enrich_from_musicbrainz_generic(df, f"artist_universe_{label}_generic_musicbrainz.csv")
        builder.log(f"Running targeted Wikidata QID enrichment for {label}")
        df = enrich_from_wikidata_qids(df, f"artist_universe_{label}_targeted_wikidata.csv")
        builder.log(f"Running Discogs/iTunes enrichment for {label}")
        df = enrich_with_discogs_and_itunes(df, f"artist_universe_{label}_discogs_itunes.csv")
        df = apply_place_parsing(df)
        df = clean_birth_geography(df)
        df = apply_web_confirmed_overrides(df)
        if label == "core":
            core_df = df
        else:
            adj_df = df

    full_df = dedupe_by_name(pd.concat([core_df, adj_df, full_df], ignore_index=True, sort=False))
    full_df = apply_place_parsing(full_df)
    full_df = clean_birth_geography(full_df)
    review_df = full_df[full_df["manual_review_needed"].fillna(0).astype(int).eq(1)].copy()
    review_df = dedupe_by_name(review_df)

    save_dataset(core_df, CORE_BASENAME)
    save_dataset(adj_df, ADJ_BASENAME)
    save_dataset(full_df, FULL_BASENAME)
    save_dataset(review_df, REVIEW_BASENAME)
    write_qc_report(core_df, adj_df, full_df)


if __name__ == "__main__":
    main()
