#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sys
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd
import pyreadstat
import requests
from bs4 import BeautifulSoup

BASE_DIR = Path("/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from execution.phase_01_dataset_construction import build_country_artists_dataset as builder

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
    "musicbrainz_artist_type": "MusicBrainz artist type",
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

COUNTY_PATTERN = re.compile(r"\bcounty\b", re.I)

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
    "Breland": {
        "birth_date": "1995-07-18",
        "birth_year": 1995,
        "birth_city": "Burlington Township",
        "birth_state": "New Jersey",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace/date: Wikipedia",
    },
    "George Birge": {
        "birth_city": "Austin",
        "birth_state": "Texas",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace: Wikipedia",
    },
    "Bryan Martin": {
        "birth_city": "Logansport",
        "birth_state": "Louisiana",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace: Wikipedia",
    },
    "Old Dominion": {
        "birth_year": 2007,
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Due West": {
        "birth_year": 2004,
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Shenandoah": {
        "birth_year": 1984,
        "birth_city": "Muscle Shoals",
        "birth_state": "Alabama",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "River Road": {
        "birth_year": 1989,
        "birth_state": "Louisiana",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Great Plains": {
        "birth_year": 1987,
        "birth_city": "Nashville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/year formed: Wikipedia",
    },
    "Sparx": {
        "birth_year": 1980,
        "birth_state": "New Mexico",
        "birth_country": "United States",
        "notes_append": "web-confirmed group origin/active decade: Wikipedia",
    },
    "Warren Zeiders": {
        "birth_city": "Hershey",
        "birth_state": "Pennsylvania",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace: Wikipedia",
    },
    "Oliver Anthony": {
        "birth_date": "1992-06-30",
        "birth_year": 1992,
        "birth_city": "Farmville",
        "birth_state": "Virginia",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace/date: Wikipedia",
    },
    "Lillie Mae": {
        "birth_date": "1991-06-26",
        "birth_year": 1991,
        "birth_city": "Galena",
        "birth_state": "Illinois",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace/date: Wikipedia",
    },
    "Kylie Morgan": {
        "birth_state": "Oklahoma",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Ash Bowers": {
        "birth_city": "Jackson",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Logan Brill": {
        "birth_city": "Knoxville",
        "birth_state": "Tennessee",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
    },
    "Sean Patrick McGraw": {
        "birth_city": "Dunkirk",
        "birth_state": "New York",
        "birth_country": "United States",
        "notes_append": "web-confirmed birthplace region: Wikipedia",
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
    text = text.replace(" U.S.", " United States").replace(", U.S.", ", United States").replace(" U.S", " United States")
    text = re.sub(r"^\+?\d{4}-\d{2}-\d{2}\s*", "", text).strip()
    text = re.sub(r"^\d{1,2}\s+[A-Za-z]+\s+\d{4}\s*", "", text).strip()
    text = re.sub(r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}\s*", "", text).strip()
    text = re.sub(r"^(?:c\.|ca\.|circa)\s*(18\d{2}|19\d{2}|20\d{2})\s*", "", text, flags=re.I).strip()
    text = re.sub(r"^(18\d{2}|19\d{2}|20\d{2})\s+or\s+(18\d{2}|19\d{2}|20\d{2})\s*", "", text, flags=re.I).strip()
    return text.lstrip(", ").strip()


def canonical_country(value: str) -> str:
    key = normalize_name(value)
    return COUNTRY_ALIASES.get(key, nonempty(value))


def parse_place_components(raw_place: str, current_country: str) -> dict:
    raw_place = strip_leading_date(raw_place)
    if not raw_place:
        return {"birth_city": "", "birth_county": "", "birth_state": "", "birth_country": canonical_country(current_country)}

    parts = [part.strip(" .") for part in raw_place.split(",") if part.strip(" .")]
    if not parts:
        return {"birth_city": "", "birth_county": "", "birth_state": "", "birth_country": canonical_country(current_country)}

    country = canonical_country(current_country)
    tail_raw = parts[-1]
    tail_country = canonical_country(tail_raw)
    if tail_raw not in NON_US_SUBDIVISIONS and tail_country in set(COUNTRY_ALIASES.values()):
        country = tail_country
        parts = parts[:-1]

    county = ""
    state = ""
    if parts:
        candidate = parts[-1]
        if COUNTY_PATTERN.search(candidate):
            county = candidate
            parts = parts[:-1]
            candidate = parts[-1] if parts else ""
        if candidate in builder.US_STATE_ABBR:
            state = candidate
            country = country or "United States"
            parts = parts[:-1]
        elif candidate in NON_US_SUBDIVISIONS:
            state = candidate
            if candidate in {"England", "Scotland", "Wales", "Northern Ireland"}:
                country = country or "United Kingdom"
            parts = parts[:-1]
        elif country and country != "United States" and len(parts) > 1:
            state = candidate
            parts = parts[:-1]

    city = parts[0] if parts else ""
    if not county and len(parts) > 1 and COUNTY_PATTERN.search(parts[-1]):
        county = parts[-1]
        if city == county:
            city = ""
    return {"birth_city": city, "birth_county": county, "birth_state": state, "birth_country": country}


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
        if not nonempty(row.get("birth_county", "")) and parsed["birth_county"]:
            enriched.at[idx, "birth_county"] = parsed["birth_county"]
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
            r"^(The\s)| Brothers| Band| Family| Sisters| Singers| Trio| Quartet| Ramblers| Minstrels| Party| Ensemble| Twenties| Boys| Girls| Club| Raised| Country$| Unlimited$",
            text,
            flags=re.I,
        )
    )


def infer_group_role(name: object, categories: object, occupations: object, notes: object = "", musicbrainz_type: object = "") -> str:
    combined = " | ".join(nonempty(value).lower() for value in [name, categories, occupations, notes, musicbrainz_type] if nonempty(value))
    if any(token in combined for token in [" duo", "duo", "duos"]):
        return "duo"
    if any(token in combined for token in [" trio", "trio", "trios"]):
        return "trio"
    if any(token in combined for token in [" quartet", "quartet", "quartets"]):
        return "quartet"
    return "band"


def is_group_entity(row: pd.Series | dict) -> bool:
    name = nonempty(row.get("name_primary", ""))
    occupations = nonempty(row.get("occupations", "")).lower()
    categories = nonempty(row.get("wikipedia_categories", "")).lower()
    notes = nonempty(row.get("notes", "")).lower()
    mb_type = nonempty(row.get("musicbrainz_artist_type", "")).lower()
    collab_like = bool(re.search(r"\s(?:&|and)\s", name, flags=re.I))
    has_structural_group_name = likely_group_name(name)
    has_web_group_evidence = (
        mb_type == "group"
        or any(token in categories for token in ["musical groups", "music groups", "bands", "duos", "trios", "quartets", "ensembles", "groups from"])
        or nonempty(row.get("wikipedia_url", "")) != ""
        or nonempty(row.get("musicbrainz_mbid", "")) != ""
    )
    if collab_like and not has_structural_group_name and not has_web_group_evidence:
        return False
    if mb_type == "group":
        return True
    if has_structural_group_name:
        return True
    if any(token in occupations for token in ["band", "duo", "trio", "quartet", "group", "ensemble"]):
        return True
    if any(
        token in categories
        for token in ["musical groups", "music groups", "bands", "duos", "trios", "quartets", "ensembles", "groups from"]
    ):
        return True
    if "group origin" in notes or "group formed" in notes or "duo origin" in notes:
        return True
    return False


def append_pipe_value(existing: object, value: str) -> str:
    current = [piece.strip() for piece in nonempty(existing).split("|") if piece.strip()]
    if value and value not in current:
        current.append(value)
    return "|".join(current)


def infer_occupations_from_categories(categories: object) -> List[str]:
    text = nonempty(categories).lower()
    inferred: List[str] = []
    pattern_map = [
        (r"singer.songwriters|singer-songwriters", "singer-songwriter"),
        (r"\bsingers\b|\bvocalists\b", "singer"),
        (r"\bsongwriters\b", "songwriter"),
        (r"\bguitarists\b", "guitarist"),
        (r"\bmusicians\b", "musician"),
        (r"\bactors\b|\bactresses\b", "actor"),
        (r"\brecord producers\b", "record producer"),
    ]
    for pattern, label in pattern_map:
        if re.search(pattern, text) and label not in inferred:
            inferred.append(label)
    if any(token in text for token in ["musical groups", "music groups", "bands", "duos", "trios", "quartets", "ensembles"]):
        inferred.append(infer_group_role("", text, ""))
    return inferred


def apply_group_and_occupation_annotations(df: pd.DataFrame) -> pd.DataFrame:
    annotated = df.copy()
    if "wikipedia_categories" not in annotated.columns:
        annotated["wikipedia_categories"] = ""
    if "musicbrainz_artist_type" not in annotated.columns:
        annotated["musicbrainz_artist_type"] = ""

    for idx, row in annotated.iterrows():
        categories = row.get("wikipedia_categories", "")
        occupation_parts = [piece.strip() for piece in nonempty(row.get("occupations", "")).split("|") if piece.strip()]
        for inferred in infer_occupations_from_categories(categories):
            if inferred not in occupation_parts:
                occupation_parts.append(inferred)
        if is_group_entity(row):
            group_role = infer_group_role(row.get("name_primary", ""), categories, "|".join(occupation_parts), row.get("notes", ""), row.get("musicbrainz_artist_type", ""))
            if group_role not in occupation_parts:
                occupation_parts.append(group_role)
            annotated.at[idx, "is_solo_person"] = 0
        else:
            occupation_parts = [piece for piece in occupation_parts if piece not in {"band", "duo", "trio", "quartet"}]
            annotated.at[idx, "is_solo_person"] = 1
        annotated.at[idx, "occupations"] = "|".join(occupation_parts)

    return annotated


def parse_years_active_start(text: object) -> int | None:
    raw = nonempty(text)
    match = re.search(r"(18\d{2}|19\d{2}|20\d{2})", raw)
    if match:
        return int(match.group(1))
    decade_match = re.search(r"\b((?:18|19|20)\d)0s\b", raw)
    if decade_match:
        return int(f"{decade_match.group(1)}0")
    return None


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
        elif label in {"Years active", "Years Active", "Active"}:
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
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
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
        is_group = is_group_entity(row)
        if is_group and pd.isna(row.get("birth_year")):
            year = builder.extract_year_hint(row.get("origin_start_year", ""))
            if year:
                merged.at[idx, "birth_year"] = year
        if (
            (nonempty(row.get("birth_state", "")) == "" or nonempty(row.get("birth_city", "")) == "")
            and nonempty(row.get("origin_place_raw", ""))
        ):
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
            if not nonempty(cleaned.at[idx, "birth_county"]) and parsed["birth_county"]:
                cleaned.at[idx, "birth_county"] = parsed["birth_county"]
            if not nonempty(cleaned.at[idx, "birth_state"]) and parsed["birth_state"]:
                cleaned.at[idx, "birth_state"] = parsed["birth_state"]
            if not nonempty(cleaned.at[idx, "birth_country"]) and parsed["birth_country"]:
                cleaned.at[idx, "birth_country"] = parsed["birth_country"]

    cleaned["birth_state_abbr"] = cleaned["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return cleaned


def infer_state_from_existing_city_country(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()
    reference = enriched[
        enriched["birth_city"].fillna("").astype(str).str.strip().ne("")
        & enriched["birth_state"].fillna("").astype(str).str.strip().ne("")
    ].copy()
    if reference.empty:
        return enriched

    reference["_city_country_key"] = list(
        zip(
            reference["birth_city"].fillna("").astype(str).str.strip().str.lower(),
            reference["birth_country"].fillna("").astype(str).str.strip().str.lower(),
        )
    )
    unique_keys = (
        reference.groupby("_city_country_key")["birth_state"]
        .nunique()
        .reset_index(name="n_states")
    )
    unique_keys = set(unique_keys[unique_keys["n_states"].eq(1)]["_city_country_key"])
    lookup = (
        reference[reference["_city_country_key"].isin(unique_keys)]
        .drop_duplicates(subset=["_city_country_key"])
        .set_index("_city_country_key")["birth_state"]
        .to_dict()
    )
    if not lookup:
        return enriched

    for idx, row in enriched.iterrows():
        if nonempty(row.get("birth_state", "")):
            continue
        city = nonempty(row.get("birth_city", ""))
        if not city:
            continue
        country = nonempty(row.get("birth_country", ""))
        key = (city.lower(), country.lower())
        inferred_state = nonempty(lookup.get(key, ""))
        if inferred_state:
            enriched.at[idx, "birth_state"] = inferred_state

    enriched["birth_state_abbr"] = enriched["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return enriched


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
            | enriched["occupations"].fillna("").astype(str).str.strip().eq("")
            | enriched["wikipedia_categories"].fillna("").astype(str).str.strip().eq("")
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


def resolve_missing_musicbrainz_mbids(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["musicbrainz_mbid"].fillna("").astype(str).str.strip().eq("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["name_primary", "resolved_musicbrainz_mbid", "resolved_musicbrainz_type", "resolved_musicbrainz_country"])

    done = set(checkpoint_df["name_primary"].fillna("").astype(str))
    pending = [name for name in needs["name_primary"].fillna("").astype(str).tolist() if name and name not in done]
    if pending:
        s = builder.session()
        new_rows: List[dict] = []
        for index, name in enumerate(pending, start=1):
            mbid = ""
            artist_type = ""
            country = ""
            try:
                payload = builder.request_json(
                    s,
                    f"{builder.MUSICBRAINZ_API}/artist/",
                    params={"fmt": "json", "limit": 5, "query": f'artist:"{name}"'},
                    pause=1.05,
                )
                candidates = payload.get("artists", [])
                target_key = normalize_name(name)
                best = None
                for item in candidates:
                    candidate_key = normalize_name(item.get("name", ""))
                    if candidate_key == target_key:
                        best = item
                        break
                if not best and candidates:
                    best = candidates[0]
                if best:
                    mbid = nonempty(best.get("id", ""))
                    artist_type = nonempty(best.get("type", ""))
                    country = canonical_country(best.get("country", ""))
            except Exception as exc:
                builder.log(f"MusicBrainz name-to-MBID lookup failed for {name}: {exc}")
            new_rows.append(
                {
                    "name_primary": name,
                    "resolved_musicbrainz_mbid": mbid,
                    "resolved_musicbrainz_type": artist_type,
                    "resolved_musicbrainz_country": country,
                }
            )
            if new_rows and (index % 25 == 0 or index == len(pending)):
                checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
                checkpoint_df = checkpoint_df.drop_duplicates(subset=["name_primary"], keep="last")
                checkpoint_df.to_csv(checkpoint_path, index=False)
                builder.log(f"Saved MusicBrainz name-to-MBID checkpoint at {len(checkpoint_df)} rows")
                new_rows = []

    merged = enriched.merge(checkpoint_df, on="name_primary", how="left")
    merged["musicbrainz_mbid"] = merged.apply(
        lambda row: nonempty(row.get("musicbrainz_mbid", "")) or nonempty(row.get("resolved_musicbrainz_mbid", "")),
        axis=1,
    )
    merged["musicbrainz_artist_type"] = merged.apply(
        lambda row: nonempty(row.get("musicbrainz_artist_type", "")) or nonempty(row.get("resolved_musicbrainz_type", "")),
        axis=1,
    )
    merged["birth_country"] = merged.apply(
        lambda row: nonempty(row.get("birth_country", "")) or nonempty(row.get("resolved_musicbrainz_country", "")),
        axis=1,
    )
    return merged.drop(columns=["resolved_musicbrainz_mbid", "resolved_musicbrainz_type", "resolved_musicbrainz_country"], errors="ignore")


def jsonld_objects_from_html(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    objects: List[dict] = []
    for node in soup.select('script[type="application/ld+json"]'):
        raw = node.string or node.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        stack = payload if isinstance(payload, list) else [payload]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                objects.append(item)
                for key in ["@graph", "itemListElement"]:
                    value = item.get(key)
                    if isinstance(value, list):
                        stack.extend(value)
                    elif isinstance(value, dict):
                        stack.append(value)
            elif isinstance(item, list):
                stack.extend(item)
    return objects


def parse_schema_place(value: object, default_country: str = "") -> dict:
    if isinstance(value, str):
        return parse_place_components(value, default_country)
    if not isinstance(value, dict):
        return {"birth_city": "", "birth_county": "", "birth_state": "", "birth_country": canonical_country(default_country)}

    city = nonempty(value.get("addressLocality", "")) or nonempty(value.get("city", "")) or nonempty(value.get("name", ""))
    state = nonempty(value.get("addressRegion", "")) or nonempty(value.get("state", ""))
    country = canonical_country(value.get("addressCountry", "")) or canonical_country(default_country)
    county = nonempty(value.get("addressCounty", "")) or nonempty(value.get("county", ""))

    address = value.get("address")
    if isinstance(address, dict):
        if not city:
            city = nonempty(address.get("addressLocality", "")) or nonempty(address.get("name", ""))
        if not state:
            state = nonempty(address.get("addressRegion", "")) or nonempty(address.get("state", ""))
        if not country:
            country = canonical_country(address.get("addressCountry", ""))
        if not county:
            county = nonempty(address.get("addressCounty", "")) or nonempty(address.get("county", ""))
    elif isinstance(address, str):
        parsed = parse_place_components(address, country or default_country)
        city = city or parsed["birth_city"]
        state = state or parsed["birth_state"]
        country = country or parsed["birth_country"]
        county = county or parsed["birth_county"]

    if city and city in {state, country}:
        city = ""
    return {"birth_city": city, "birth_county": county, "birth_state": state, "birth_country": country}


def parse_official_website_schema(official_url: str, is_group: bool) -> dict:
    result = {
        "official_birth_year": None,
        "official_birth_city": "",
        "official_birth_county": "",
        "official_birth_state": "",
        "official_birth_country": "",
        "official_schema_url": "",
    }
    if not nonempty(official_url):
        return result

    response = requests.get(official_url, timeout=45, headers={"User-Agent": builder.USER_AGENT})
    response.raise_for_status()
    result["official_schema_url"] = official_url
    objects = jsonld_objects_from_html(response.text)
    if not objects:
        return result

    candidate_types = {"MusicGroup", "MusicArtist", "Person", "Organization"}
    for item in objects:
        raw_types = item.get("@type", [])
        types = {raw_types} if isinstance(raw_types, str) else set(raw_types or [])
        if types and not (types & candidate_types):
            continue

        date_candidates = []
        if is_group or "MusicGroup" in types or "Organization" in types:
            date_candidates.extend([item.get("foundingDate", ""), item.get("startDate", ""), item.get("dateCreated", "")])
        date_candidates.extend([item.get("birthDate", ""), item.get("datePublished", "")])
        for candidate in date_candidates:
            year = builder.extract_year_hint(candidate)
            if year:
                result["official_birth_year"] = year
                break

        place_candidates = []
        for key in ["birthPlace", "foundingLocation", "locationCreated", "homeLocation", "location", "address"]:
            value = item.get(key)
            if value:
                place_candidates.append(value)
        for candidate in place_candidates:
            parsed = parse_schema_place(candidate, result["official_birth_country"])
            for key in ["birth_city", "birth_county", "birth_state", "birth_country"]:
                if not result[f"official_{key}"] and parsed[key]:
                    result[f"official_{key}"] = parsed[key]
        if result["official_birth_year"] or result["official_birth_state"] or result["official_birth_city"] or result["official_birth_country"]:
            break

    return result


def enrich_from_official_websites(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["official_website"].fillna("").astype(str).str.strip().ne("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(
            columns=[
                "official_website",
                "official_birth_year",
                "official_birth_city",
                "official_birth_county",
                "official_birth_state",
                "official_birth_country",
                "official_schema_url",
            ]
        )

    done = set(checkpoint_df["official_website"].fillna("").astype(str))
    pending = needs[~needs["official_website"].isin(done)].copy()
    new_rows: List[dict] = []
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        url = nonempty(row["official_website"])
        parsed = {
            "official_birth_year": None,
            "official_birth_city": "",
            "official_birth_county": "",
            "official_birth_state": "",
            "official_birth_country": "",
            "official_schema_url": "",
        }
        try:
            parsed = parse_official_website_schema(url, is_group_entity(row))
        except Exception as exc:
            builder.log(f"Official website schema enrichment failed for {url}: {exc}")
        new_rows.append({"official_website": url, **parsed})
        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["official_website"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved official-website checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    merged = enriched.merge(checkpoint_df, on="official_website", how="left")
    merged["birth_year"] = merged.apply(
        lambda row: row["birth_year"] if pd.notna(row["birth_year"]) else builder.extract_year_hint(row.get("official_birth_year", "")),
        axis=1,
    )
    for source_col, target_col in [
        ("official_birth_city", "birth_city"),
        ("official_birth_county", "birth_county"),
        ("official_birth_state", "birth_state"),
        ("official_birth_country", "birth_country"),
    ]:
        merged[target_col] = merged.apply(
            lambda row: nonempty(row.get(target_col, "")) or nonempty(row.get(source_col, "")),
            axis=1,
        )
    merged["evidence_urls"] = merged.apply(
        lambda row: "|".join(piece for piece in [nonempty(row.get("evidence_urls", "")), nonempty(row.get("official_schema_url", ""))] if piece),
        axis=1,
    )
    merged["notes"] = merged.apply(
        lambda row: "|".join(
            piece
            for piece in [nonempty(row.get("notes", "")), "birth/origin data enriched from official website schema" if nonempty(row.get("official_schema_url", "")) else ""]
            if piece
        ),
        axis=1,
    )
    merged["birth_state_abbr"] = merged["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    drop_cols = [
        "official_birth_year",
        "official_birth_city",
        "official_birth_county",
        "official_birth_state",
        "official_birth_country",
        "official_schema_url",
    ]
    return merged.drop(columns=[col for col in drop_cols if col in merged.columns], errors="ignore")


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
        checkpoint_df = pd.DataFrame(columns=["musicbrainz_mbid", "mbx_birth_year", "mbx_birth_city", "mbx_birth_state", "mbx_birth_country", "musicbrainz_artist_type"])
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
                    "musicbrainz_artist_type": nonempty(payload.get("type", "")),
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
                    "musicbrainz_artist_type": "",
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
    merged["musicbrainz_artist_type"] = merged.apply(
        lambda row: nonempty(row.get("musicbrainz_artist_type", "")) or nonempty(row.get("musicbrainz_artist_type_y", "")) or nonempty(row.get("musicbrainz_artist_type_x", "")),
        axis=1,
    )
    merged["birth_state_abbr"] = merged["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    return merged.drop(columns=["mbx_birth_year", "mbx_birth_city", "mbx_birth_state", "mbx_birth_country"], errors="ignore")


def enrich_from_wikidata_qids(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    for col in ["wd_birth_date", "wd_birth_place_raw", "wd_birth_country_raw", "wd_occupations", "wd_member_of", "wd_wikipedia_url", "wd_musicbrainz_mbid", "wd_aliases"]:
        if col in enriched.columns:
            enriched = enriched.drop(columns=[col])
    needs = enriched[
        enriched["wikidata_qid"].fillna("").astype(str).str.startswith("Q")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
            | enriched["occupations"].fillna("").astype(str).str.strip().eq("")
            | enriched["wikipedia_url"].fillna("").astype(str).str.strip().eq("")
            | enriched["musicbrainz_mbid"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["wikidata_qid", "wd_birth_date", "wd_birth_place_raw", "wd_birth_country_raw", "wd_occupations", "wd_member_of", "wd_wikipedia_url", "wd_musicbrainz_mbid", "wd_aliases"])
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
                "wd_occupations": nonempty(row.get("occupations", "")),
                "wd_member_of": nonempty(row.get("member_of", "")),
                "wd_wikipedia_url": nonempty(row.get("wikipedia_url", "")),
                "wd_musicbrainz_mbid": nonempty(row.get("musicbrainz_mbid", "")),
                "wd_aliases": nonempty(row.get("aliases", "")),
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
    merged["occupations"] = merged.apply(lambda row: nonempty(row.get("occupations", "")) or nonempty(row.get("wd_occupations", "")), axis=1)
    merged["member_of"] = merged.apply(lambda row: nonempty(row.get("member_of", "")) or nonempty(row.get("wd_member_of", "")), axis=1)
    merged["wikipedia_url"] = merged.apply(lambda row: nonempty(row.get("wikipedia_url", "")) or nonempty(row.get("wd_wikipedia_url", "")), axis=1)
    merged["musicbrainz_mbid"] = merged.apply(lambda row: nonempty(row.get("musicbrainz_mbid", "")) or nonempty(row.get("wd_musicbrainz_mbid", "")), axis=1)
    merged["aliases"] = merged.apply(lambda row: nonempty(row.get("aliases", "")) or nonempty(row.get("wd_aliases", "")), axis=1)
    merged = apply_place_parsing(merged)
    return merged.drop(columns=["wd_birth_date", "wd_birth_place_raw", "wd_birth_country_raw", "wd_occupations", "wd_member_of", "wd_wikipedia_url", "wd_musicbrainz_mbid", "wd_aliases"], errors="ignore")


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


def save_country_only_dataset(df: pd.DataFrame) -> None:
    csv_path = OUTPUT_DIR / "artist_universe_country_only.csv"
    dta_path = OUTPUT_DIR / "artist_universe_country_only.dta"
    df = collapse_merge_columns(df)
    df = coerce_for_stata(df)
    column_labels = [VARIABLE_LABELS.get(col, col) for col in df.columns]
    df.to_csv(csv_path, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
    pyreadstat.write_dta(df, dta_path, column_labels=column_labels, file_label="Country-only artist universe", version=15)
    builder.log(f"Saved {csv_path.name} and {dta_path.name}")


def refresh_country_only_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    refreshed = df.copy()
    refreshed["birth_year"] = pd.to_numeric(refreshed["birth_year"], errors="coerce")
    refreshed["death_year"] = refreshed["death_date"].fillna("").astype(str).str.slice(0, 4).map(builder.maybe_int)
    refreshed["birth_state_abbr"] = refreshed["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    refreshed["birth_decade"] = refreshed["birth_year"].map(lambda value: int(value // 10 * 10) if pd.notna(value) else None)
    refreshed["us_macro_region"] = refreshed["birth_state"].map(lambda value: builder.STATE_TO_REGION.get(nonempty(value), "Unknown"))
    non_us_mask = (
        refreshed["birth_country"].fillna("").astype(str).str.strip().ne("")
        & refreshed["birth_country"].fillna("").astype(str).str.strip().ne("United States")
    )
    refreshed.loc[non_us_mask, "us_macro_region"] = "Non-US"
    refreshed["is_deceased"] = refreshed["death_date"].fillna("").astype(str).str.strip().ne("").astype(int)
    refreshed["is_us_born"] = refreshed["birth_country"].fillna("").astype(str).str.strip().eq("United States").astype(int)

    today = date.today()

    def calc_age(row: pd.Series) -> int | None:
        birth_date = nonempty(row.get("birth_date", ""))
        death_date = nonempty(row.get("death_date", ""))
        if not birth_date:
            return None
        try:
            born = datetime.fromisoformat(birth_date.replace("Z", "+00:00")).date()
            ended = datetime.fromisoformat(death_date.replace("Z", "+00:00")).date() if death_date else today
            return ended.year - born.year - ((ended.month, ended.day) < (born.month, born.day))
        except Exception:
            return None

    refreshed["age_or_age_at_death"] = refreshed.apply(calc_age, axis=1)
    return refreshed


def report_missingness(df: pd.DataFrame, label: str) -> None:
    occupation_missing = int(df["occupations"].fillna("").astype(str).str.strip().eq("").sum())
    birth_year_missing = int(pd.to_numeric(df["birth_year"], errors="coerce").isna().sum())
    birth_state_missing = int(df["birth_state"].fillna("").astype(str).str.strip().eq("").sum())
    builder.log(
        f"{label}: occupation missing={occupation_missing}, "
        f"birth_year missing={birth_year_missing}, birth_state missing={birth_state_missing}"
    )


def enrich_country_only_direct() -> None:
    path = OUTPUT_DIR / "artist_universe_country_only.csv"
    df = collapse_merge_columns(pd.read_csv(path, low_memory=False))
    report_missingness(df, "country_only before enrichment")

    df = apply_place_parsing(df)
    df = clean_birth_geography(df)
    df = resolve_missing_qids_from_names(df, "artist_universe_country_only_name_to_qid.csv")
    df = enrich_from_wikidata_qids(df, "artist_universe_country_only_targeted_wikidata.csv")
    df = enrich_from_wikipedia_sources(df, "country_artists")
    df = resolve_missing_musicbrainz_mbids(df, "artist_universe_country_only_name_to_musicbrainz.csv")
    df = builder.targeted_musicbrainz_bio_enrichment(builder.session(), df, INTERMEDIATE_DIR / "artist_universe_country_only_targeted_musicbrainz_bio.csv")
    df = enrich_from_musicbrainz_generic(df, "artist_universe_country_only_generic_musicbrainz.csv")
    df = enrich_from_wikipedia_origin_years(df, "artist_universe_country_only_wikipedia_origin_years.csv")
    df = enrich_with_discogs_and_itunes(df, "artist_universe_country_only_discogs_itunes.csv")
    df = enrich_from_official_websites(df, "artist_universe_country_only_official_websites.csv")
    df = apply_web_confirmed_overrides(df)
    df = apply_group_and_occupation_annotations(df)
    df = apply_place_parsing(df)
    df = clean_birth_geography(df)
    df = infer_state_from_existing_city_country(df)
    df = refresh_country_only_derived_columns(df)
    report_missingness(df, "country_only after enrichment")
    save_country_only_dataset(df)


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
    parser = argparse.ArgumentParser(description="Enrich artist-universe metadata.")
    parser.add_argument("--country-only-direct", action="store_true", help="Enrich artist_universe_country_only.csv in place.")
    args = parser.parse_args()

    if args.country_only_direct:
        enrich_country_only_direct()
        return

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
        df = resolve_missing_musicbrainz_mbids(df, f"artist_universe_{label}_name_to_musicbrainz.csv")
        df = builder.targeted_musicbrainz_bio_enrichment(builder.session(), df, INTERMEDIATE_DIR / f"artist_universe_{label}_targeted_musicbrainz_bio.csv")
        df = enrich_from_musicbrainz_generic(df, f"artist_universe_{label}_generic_musicbrainz.csv")
        builder.log(f"Running targeted Wikidata QID enrichment for {label}")
        df = enrich_from_wikidata_qids(df, f"artist_universe_{label}_targeted_wikidata.csv")
        builder.log(f"Running Discogs/iTunes enrichment for {label}")
        df = enrich_with_discogs_and_itunes(df, f"artist_universe_{label}_discogs_itunes.csv")
        builder.log(f"Running official-website schema enrichment for {label}")
        df = enrich_from_official_websites(df, f"artist_universe_{label}_official_websites.csv")
        df = apply_place_parsing(df)
        df = clean_birth_geography(df)
        df = infer_state_from_existing_city_country(df)
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
