import csv
import math
import os
import re
import time
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence
from urllib.parse import unquote, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup


SCRIPT_PATH = Path(__file__).resolve()


# Replication Package Path Resolution
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
OUTPUT_DIR = ROOT / "output"
INTERMEDIATE_DIR = ROOT / "data"
SCHEMA_PATH = ROOT / "docs" / "country_artists_schema.csv"
FALLBACK_MARKER_PATH = INTERMEDIATE_DIR / "_bundled_fallback_used.txt"

USER_AGENT = "SoundOfCultureCountryArtistsBot/1.0 (research pipeline; contact: local-run)"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
MUSICBRAINZ_API = "https://musicbrainz.org/ws/2"
SPARQL_AVAILABLE = True

COUNTRY_GENRE_PATTERN = re.compile(
    r"country|bluegrass|americana|honky|western swing|outlaw|alt-country|country gospel|bro-country",
    re.I,
)
SINGER_PATTERN = re.compile(r"singer|vocalist|musician|songwriter|recording artist|composer", re.I)
COUNTY_PATTERN = re.compile(r"\bcounty\b", re.I)
REFERENCE_MARKER_PATTERN = re.compile(r"\[\s*[^\]]+?\s*\]")
YEAR_PATTERN = re.compile(r"\b(18\d{2}|19\d{2}|20\d{2})\b")

WIKIPEDIA_CATEGORY_TITLES = [
    "Category:American country singer-songwriters",
    "Category:American country singers",
    "Category:Country musicians from Tennessee",
    "Category:Country musicians from Texas",
    "Category:Country musicians from Oklahoma",
    "Category:Country musicians from Kentucky",
]

WIKIPEDIA_LIST_PAGES = [
    "List of country performers",
    "List of members of the Country Music Hall of Fame",
]

US_STATE_ABBR = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
}
US_ABBR_STATE = {abbr: state for state, abbr in US_STATE_ABBR.items()}

STATE_TO_REGION = {
    "Alabama": "South",
    "Alaska": "West",
    "Arizona": "West",
    "Arkansas": "South",
    "California": "West",
    "Colorado": "West",
    "Connecticut": "Northeast",
    "Delaware": "South",
    "Florida": "South",
    "Georgia": "South",
    "Hawaii": "West",
    "Idaho": "West",
    "Illinois": "Midwest",
    "Indiana": "Midwest",
    "Iowa": "Midwest",
    "Kansas": "Midwest",
    "Kentucky": "Appalachia",
    "Louisiana": "South",
    "Maine": "Northeast",
    "Maryland": "South",
    "Massachusetts": "Northeast",
    "Michigan": "Midwest",
    "Minnesota": "Midwest",
    "Mississippi": "South",
    "Missouri": "Midwest",
    "Montana": "West",
    "Nebraska": "Midwest",
    "Nevada": "West",
    "New Hampshire": "Northeast",
    "New Jersey": "Northeast",
    "New Mexico": "West",
    "New York": "Northeast",
    "North Carolina": "South",
    "North Dakota": "Midwest",
    "Ohio": "Midwest",
    "Oklahoma": "South",
    "Oregon": "West",
    "Pennsylvania": "Appalachia",
    "Rhode Island": "Northeast",
    "South Carolina": "South",
    "South Dakota": "Midwest",
    "Tennessee": "Appalachia",
    "Texas": "South",
    "Utah": "West",
    "Vermont": "Northeast",
    "Virginia": "South",
    "Washington": "West",
    "West Virginia": "Appalachia",
    "Wisconsin": "Midwest",
    "Wyoming": "West",
    "District of Columbia": "South",
}

GENRE_NORMALIZATION = {
    "country": "country",
    "country pop": "country pop",
    "country rock": "country rock",
    "outlaw country": "outlaw country",
    "bluegrass": "bluegrass",
    "americana": "americana",
    "western swing": "western swing",
    "honky-tonk": "honky-tonk",
    "honky tonk": "honky-tonk",
    "alt-country": "alt-country",
    "alternative country": "alt-country",
    "bro-country": "bro-country",
    "country gospel": "country gospel",
}

INSTRUMENT_NORMALIZATION = {
    "voice": "voice",
    "vocals": "vocals",
    "guitar": "guitar",
    "acoustic guitar": "acoustic guitar",
    "electric guitar": "electric guitar",
    "banjo": "banjo",
    "fiddle": "fiddle",
    "violin": "violin",
    "mandolin": "mandolin",
    "piano": "piano",
    "pedal steel guitar": "pedal steel guitar",
    "dobro": "dobro",
}


def log(message: str) -> None:
    print(f"[country-artists] {message}", flush=True)


def disable_sparql(reason: str) -> None:
    global SPARQL_AVAILABLE
    if SPARQL_AVAILABLE:
        SPARQL_AVAILABLE = False
        log(f"Disabling SPARQL for the rest of the run: {reason}")


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)


def bounded_retry_wait(retry_after: Optional[str], fallback_wait: int, *, max_wait: int = 10) -> int:
    if retry_after and retry_after.isdigit():
        return min(int(retry_after), max_wait)
    return min(fallback_wait, max_wait)


def resolve_bundled_intermediate_dir() -> Optional[Path]:
    for parent in SCRIPT_PATH.parents:
        candidate = parent / "datasets" / "country_artists" / "intermediate"
        if candidate.exists():
            return candidate
    return None


def restore_bundled_intermediate_files(filenames: Optional[Sequence[str]] = None, *, overwrite: bool = False) -> bool:
    bundled_dir = resolve_bundled_intermediate_dir()
    if bundled_dir is None or not bundled_dir.exists():
        return False

    selected = list(filenames) if filenames is not None else [path.name for path in bundled_dir.iterdir() if path.is_file()]
    restored = False
    for name in selected:
        src = bundled_dir / name
        dst = INTERMEDIATE_DIR / name
        if src.exists() and (overwrite or not dst.exists()):
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())
            restored = True
    if restored:
        FALLBACK_MARKER_PATH.write_text("bundled_intermediate_fallback_used\n", encoding="utf-8")
        log("Restored bundled intermediate checkpoints into the working directory")
    return restored


def request_json(
    s: requests.Session,
    url: str,
    *,
    params: Optional[dict] = None,
    pause: float = 0.0,
    attempts: int = 3,
    timeout: int = 15,
    backoff_seconds: int = 2,
) -> dict:
    last_error = None
    for attempt in range(attempts):
        try:
            response = s.get(url, params=params, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504}:
                last_error = requests.HTTPError(f"{response.status_code} for {response.url}")
                retry_after = response.headers.get("Retry-After")
                wait_time = bounded_retry_wait(retry_after, backoff_seconds * (attempt + 1))
                if response.status_code == 429 and retry_after and retry_after.isdigit() and int(retry_after) > 60:
                    raise last_error
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            if pause:
                time.sleep(pause)
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            time.sleep(backoff_seconds * (attempt + 1))
    raise last_error


def post_sparql(
    s: requests.Session,
    query: str,
    *,
    attempts: int = 3,
    timeout: int = 20,
    backoff_seconds: int = 5,
) -> List[dict]:
    last_error = None
    for attempt in range(attempts):
        try:
            response = s.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                timeout=timeout,
                headers={"Accept": "application/sparql-results+json", "User-Agent": USER_AGENT},
            )
            if response.status_code in {429, 500, 502, 503, 504}:
                last_error = requests.HTTPError(f"{response.status_code} for {response.url}")
                retry_after = response.headers.get("Retry-After")
                wait_time = bounded_retry_wait(retry_after, backoff_seconds * (attempt + 1))
                if response.status_code == 429 and retry_after and retry_after.isdigit() and int(retry_after) > 60:
                    raise last_error
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            return response.json()["results"]["bindings"]
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            time.sleep(backoff_seconds * (attempt + 1))
    raise last_error


def clean_title(title: str) -> str:
    title = title.replace("_", " ").strip()
    return re.sub(r"\s+", " ", title)


def clean_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "nat", "none"} else text


def strip_reference_markers(text: str) -> str:
    return clean_text(REFERENCE_MARKER_PATTERN.sub("", clean_text(text)))


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", clean_text(text)).strip()


def extract_year_hint(value: object) -> Optional[int]:
    text = normalize_space(strip_reference_markers(clean_text(value)))
    if not text:
        return None
    if re.match(r"^[+-]?\d{4}-\d{2}-\d{2}T", text):
        year = maybe_int(text.lstrip("+")[:4])
        return year if year and 1800 <= year <= date.today().year else None
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        year = maybe_int(text[:4])
        return year if year and 1800 <= year <= date.today().year else None
    match = YEAR_PATTERN.search(text)
    if not match:
        return None
    year = maybe_int(match.group(1))
    return year if year and 1800 <= year <= date.today().year else None


def pipe_join(values: Iterable[str]) -> str:
    seen = set()
    output = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text or text.lower() == "nan":
            continue
        if text not in seen:
            seen.add(text)
            output.append(text)
    return "|".join(output)


def is_blank(value: object) -> bool:
    return clean_text(value) == ""


def maybe_int(value: Optional[str]) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def normalize_values(raw_text: str, mapping: Dict[str, str]) -> str:
    if not raw_text:
        return ""
    normalized = []
    for piece in raw_text.split("|"):
        piece_clean = piece.strip()
        if not piece_clean:
            continue
        piece_lower = piece_clean.lower()
        matched = None
        for key, value in mapping.items():
            if key in piece_lower:
                matched = value
                break
        if matched:
            normalized.append(matched)
    return pipe_join(normalized)


def category_members(s: requests.Session, category_title: str) -> List[str]:
    members = []
    cmcontinue = None
    while True:
        params = {
            "action": "query",
            "list": "categorymembers",
            "format": "json",
            "cmtitle": category_title,
            "cmlimit": "max",
            "cmnamespace": 0,
        }
        if cmcontinue:
            params["cmcontinue"] = cmcontinue
        payload = request_json(s, WIKIPEDIA_API, params=params)
        for item in payload.get("query", {}).get("categorymembers", []):
            title = clean_title(item.get("title", ""))
            if title:
                members.append(title)
        cmcontinue = payload.get("continue", {}).get("cmcontinue")
        if not cmcontinue:
            break
    return members


def parse_wikipedia_born_cell(born_text: str, birth_date: str) -> tuple[str, str]:
    text = normalize_space(strip_reference_markers(born_text))
    name_candidate = ""

    if birth_date:
        pattern = rf"^(.*?)\(\s*{re.escape(birth_date)}\s*\)"
        match = re.match(pattern, text)
        if match:
            prefix = clean_text(match.group(1)).strip(", ")
            if prefix and not re.search(r"\d", prefix):
                name_candidate = prefix

    place_text = text
    if name_candidate and place_text.startswith(name_candidate):
        place_text = place_text[len(name_candidate) :].strip()
    if birth_date:
        place_text = re.sub(rf"\(\s*{re.escape(birth_date)}\s*\)", "", place_text)
    place_text = re.sub(
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b",
        "",
        place_text,
    )
    place_text = re.sub(r"\([^)]*age[^)]*\)", "", place_text, flags=re.I)
    place_text = re.sub(r"\([^)]*died[^)]*\)", "", place_text, flags=re.I)
    place_text = normalize_space(place_text).strip(",; ")
    place_text = place_text.replace(" U.S.", " United States")
    place_text = place_text.replace(", U.S.", ", United States")
    place_text = place_text.replace(" US", " United States")
    return name_candidate, place_text


def extract_date_from_text(text: str) -> str:
    text = normalize_space(strip_reference_markers(text))
    if not text:
        return ""
    patterns = [
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
        r"(Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+\d{1,2},\s+\d{4}",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if not match:
            continue
        candidate = match.group(0).replace("Sept", "Sep")
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(candidate, fmt).date().isoformat()
            except ValueError:
                continue
    return ""


def extract_wikipedia_birth_year(text: str, birth_date: str) -> Optional[int]:
    if birth_date:
        year = maybe_int(clean_text(birth_date)[:4])
        if year:
            return year
    return extract_year_hint(text)


def extract_birth_year_from_categories(categories: str) -> Optional[int]:
    for piece in str(categories).split("|"):
        text = normalize_space(piece)
        match = re.fullmatch(r"(18\d{2}|19\d{2}|20\d{2}) births", text)
        if match:
            year = maybe_int(match.group(1))
            if year:
                return year
    return None


def extract_wikipedia_title(wikipedia_url: str) -> str:
    wikipedia_url = clean_text(wikipedia_url)
    if not wikipedia_url:
        return ""
    path = urlparse(wikipedia_url).path
    title = unquote(path.rsplit("/", 1)[-1]).replace("_", " ")
    return normalize_space(title)


def wikipedia_page_key(wikipedia_url: str) -> str:
    return extract_wikipedia_title(wikipedia_url)


def normalize_infobox_field_text(text: str) -> str:
    text = strip_reference_markers(text)
    text = normalize_space(text.replace("•", "|"))
    return pipe_join(piece.strip() for piece in re.split(r"\s+\|\s+|\s*;\s*|\s*,\s*", text))


def prefer_more_informative_text(primary: str, secondary: str) -> str:
    primary = normalize_space(primary)
    secondary = normalize_space(secondary)
    if not primary:
        return secondary
    if not secondary:
        return primary
    if primary in {"United States", "USA"} and secondary not in {"United States", "USA"}:
        return secondary
    if "," not in primary and "," in secondary:
        return secondary
    if len(secondary) > len(primary) + 6:
        return secondary
    return primary


def choose_name_primary(row: pd.Series) -> str:
    candidates = [
        row.get("name_primary", ""),
        row.get("stage_name", ""),
        row.get("seed_title", ""),
        row.get("wp_page_title", ""),
        row.get("birth_name", ""),
    ]
    for candidate in candidates:
        text = normalize_space(candidate)
        if text:
            return text
    return ""


def parse_state_from_text(text: str) -> str:
    text = normalize_space(text)
    for state in US_STATE_ABBR:
        if re.search(rf"\b{re.escape(state)}\b", text):
            return state
    return ""


def infer_us_location_from_categories(categories: str, source_seed: str) -> dict:
    pieces = [normalize_space(piece) for piece in pipe_join([clean_text(categories), clean_text(source_seed)]).split("|") if normalize_space(piece)]
    text = "|".join(pieces)
    state = parse_state_from_text(text)
    city = ""
    county = ""

    for piece in pieces:
        for pattern in [
            r"People from ([A-Za-z .'\-]+ County), ([A-Za-z ]+)",
            r"People from ([A-Za-z .'\-]+), ([A-Za-z ]+)",
            r"Singers from ([A-Za-z .'\-]+), ([A-Za-z ]+)",
            r"Musicians from ([A-Za-z .'\-]+), ([A-Za-z ]+)",
            r"Songwriters from ([A-Za-z .'\-]+), ([A-Za-z ]+)",
            r"Rappers from ([A-Za-z .'\-]+), ([A-Za-z ]+)",
        ]:
            match = re.search(pattern, piece)
            if not match:
                continue
            location = normalize_space(match.group(1))
            matched_state = normalize_space(match.group(2)).replace("(U.S. state)", "").replace("(state)", "").strip()
            if matched_state in US_STATE_ABBR:
                state = matched_state
                if "County" in location and not county:
                    county = location
                elif not city:
                    city = location
                break
        if city and county and state:
            break

    if not state:
        for pattern in [
            r"Country musicians from ([A-Za-z ]+)",
            r"Singer-songwriters from ([A-Za-z ]+)",
            r"Musicians from ([A-Za-z ]+)",
        ]:
            match = re.search(pattern, text)
            if match:
                candidate = normalize_space(match.group(1))
                if candidate in US_STATE_ABBR:
                    state = candidate
                    break

    if state and not city:
        for piece in pieces:
            for pattern in [
                r"Singers from ([A-Za-z .'\-]+)$",
                r"Musicians from ([A-Za-z .'\-]+)$",
                r"Songwriters from ([A-Za-z .'\-]+)$",
                r"Rappers from ([A-Za-z .'\-]+)$",
                r"People from ([A-Za-z .'\-]+)$",
            ]:
                match = re.search(pattern, piece)
                if not match:
                    continue
                location = normalize_space(match.group(1))
                if location in US_STATE_ABBR:
                    continue
                if "County" in location:
                    if not county:
                        county = location
                else:
                    city = location
                    break
            if city:
                break

    return {
        "birth_city_from_categories": city,
        "birth_county_from_categories": county,
        "birth_state_from_categories": state,
        "birth_country_from_categories": "United States" if state else "",
    }


def has_strong_country_seed_evidence(source_seed: str, wikipedia_categories: str) -> bool:
    text = pipe_join([clean_text(source_seed), clean_text(wikipedia_categories)]).lower()
    return any(
        phrase in text
        for phrase in [
            "american country singer-songwriters",
            "american country singers",
            "country musicians from",
        ]
    )


def parse_wikipedia_infobox(s: requests.Session, wikipedia_url: str, current_name: str) -> dict:
    response = s.get(wikipedia_url, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    infobox = soup.select_one("table.infobox")
    result = {
        "wikipedia_url": wikipedia_url,
        "wp_page_title": extract_wikipedia_title(wikipedia_url),
        "wp_birth_date": "",
        "wp_birth_year": "",
        "wp_birth_name": "",
        "wp_stage_name": "",
        "wp_birth_place_raw": "",
        "wp_aliases": "",
        "wp_genres_raw": "",
        "wp_categories": "",
    }
    if not infobox:
        result["wp_categories"] = pipe_join(a.get_text(" ", strip=True) for a in soup.select("#mw-normal-catlinks ul li a"))
        return result

    born_td = None
    birth_name_text = ""
    aliases_text = ""
    genres_text = ""
    for tr in infobox.select("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        label = clean_text(th.get_text(" ", strip=True))
        if label == "Born":
            born_td = td
        elif label == "Birth name":
            birth_name_text = clean_text(td.get_text(" ", strip=True))
        elif label == "Also known as":
            aliases_text = clean_text(td.get_text(" | ", strip=True))
        elif label in {"Genres", "Genre"}:
            genres_text = clean_text(td.get_text(" | ", strip=True))

    if born_td is not None:
        bday = born_td.select_one(".bday")
        birth_date = clean_text(bday.get_text(strip=True) if bday else "") or extract_date_from_text(
            born_td.get_text(" ", strip=True)
        )
        born_text = strip_reference_markers(born_td.get_text(" ", strip=True))
        born_name, born_place = parse_wikipedia_born_cell(born_text, birth_date)
        result["wp_birth_date"] = birth_date
        result["wp_birth_year"] = extract_wikipedia_birth_year(born_text, birth_date) or ""
        result["wp_birth_place_raw"] = born_place
        if born_name:
            result["wp_birth_name"] = born_name

    if birth_name_text:
        result["wp_birth_name"] = birth_name_text

    if result["wp_birth_name"] and clean_text(current_name) and result["wp_birth_name"].lower() != clean_text(current_name).lower():
        result["wp_stage_name"] = clean_text(current_name)

    result["wp_aliases"] = normalize_infobox_field_text(aliases_text)
    result["wp_genres_raw"] = normalize_infobox_field_text(genres_text)
    result["wp_categories"] = pipe_join(a.get_text(" ", strip=True) for a in soup.select("#mw-normal-catlinks ul li a"))
    return result


def parse_wikipedia_lead_extract(extract: str) -> dict:
    text = normalize_space(strip_reference_markers(extract))
    result = {
        "wpl_birth_date": "",
        "wpl_birth_year": "",
        "wpl_birth_place_raw": "",
    }
    if not text:
        return result

    patterns = [
        r"\(born\s+([A-Z][a-z]+ \d{1,2}, \d{4})\s+in\s+([^)]+)\)",
        r"\(born\s+(\d{4})\s+in\s+([^)]+)\)",
        r"\(born\s+in\s+([^)]+)\)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        groups = match.groups()
        if len(groups) == 2:
            first, second = groups
            if re.match(r"^\d{4}$", first):
                result["wpl_birth_year"] = first
                result["wpl_birth_place_raw"] = normalize_space(second)
            else:
                parsed_date = extract_date_from_text(first)
                result["wpl_birth_date"] = parsed_date
                result["wpl_birth_year"] = str(extract_year_hint(first) or "")
                result["wpl_birth_place_raw"] = normalize_space(second)
        elif len(groups) == 1:
            result["wpl_birth_place_raw"] = normalize_space(groups[0])
        break

    if not result["wpl_birth_year"]:
        born_year_match = re.search(r"\(born[^)]*\b(18\d{2}|19\d{2}|20\d{2})\b", text)
        if born_year_match:
            result["wpl_birth_year"] = born_year_match.group(1)
    if not result["wpl_birth_date"]:
        born_date_match = re.search(r"\(born[^)]*((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})", text)
        if born_date_match:
            result["wpl_birth_date"] = extract_date_from_text(born_date_match.group(1))
            if not result["wpl_birth_year"]:
                result["wpl_birth_year"] = str(extract_year_hint(born_date_match.group(1)) or "")

    born_in_match = re.search(r"\b(?:born|was born)\s+in\s+([A-Z][A-Za-z .'\-]+(?:,\s*[A-Z][A-Za-z .'\-]+){0,2})", text)
    if born_in_match and not result["wpl_birth_place_raw"]:
        result["wpl_birth_place_raw"] = normalize_space(born_in_match.group(1))

    return result


def enrich_from_wikipedia_lead(s: requests.Session, df: pd.DataFrame, checkpoint_path: Path) -> pd.DataFrame:
    needs = df[
        df["wikipedia_url"].map(lambda x: not is_blank(x))
        & (
            df["birth_year"].isna()
            | df["birth_city"].map(is_blank)
            | df["birth_state"].map(is_blank)
        )
    ].copy()
    if needs.empty:
        return df

    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["wikipedia_url", "wpl_birth_date", "wpl_birth_year", "wpl_birth_place_raw"])
    for column in ["wikipedia_url", "wpl_birth_date", "wpl_birth_year", "wpl_birth_place_raw"]:
        if column not in checkpoint_df.columns:
            checkpoint_df[column] = ""

    done_urls = set(checkpoint_df["wikipedia_url"].dropna().astype(str).tolist())
    pending = needs[~needs["wikipedia_url"].isin(done_urls)].copy()
    log(f"Wikipedia lead enrichment pending for {len(pending)} records")

    title_to_url = {
        wikipedia_page_key(url): clean_text(url)
        for url in pending["wikipedia_url"].dropna().astype(str).tolist()
        if clean_text(url)
    }
    new_rows = []
    titles = list(title_to_url.keys())
    for batch in batched(titles, 20):
        payload = request_json(
            s,
            WIKIPEDIA_API,
            params={
                "action": "query",
                "format": "json",
                "prop": "extracts",
                "exintro": 1,
                "explaintext": 1,
                "redirects": 1,
                "titles": "|".join(batch),
            },
            pause=0.05,
        )
        pages = payload.get("query", {}).get("pages", {})
        for page in pages.values():
            title = clean_title(page.get("title", ""))
            url = title_to_url.get(title, "")
            parsed = parse_wikipedia_lead_extract(page.get("extract", ""))
            new_rows.append({"wikipedia_url": url, **parsed})

        checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
        checkpoint_df = checkpoint_df.drop_duplicates(subset=["wikipedia_url"], keep="last")
        checkpoint_df.to_csv(checkpoint_path, index=False)
        new_rows = []
        log(f"Saved Wikipedia lead checkpoint at {len(checkpoint_df)} rows")

    enriched = df.merge(checkpoint_df, on="wikipedia_url", how="left")
    enriched["birth_date"] = enriched.apply(
        lambda row: clean_text(row["birth_date"]) or clean_text(row["wpl_birth_date"]),
        axis=1,
    )
    enriched["birth_year"] = enriched.apply(
        lambda row: row["birth_year"] if pd.notna(row["birth_year"]) else extract_year_hint(row.get("wpl_birth_year", "")),
        axis=1,
    )
    enriched["birth_place_raw"] = enriched.apply(
        lambda row: prefer_more_informative_text(row["birth_place_raw"], row["wpl_birth_place_raw"]),
        axis=1,
    )
    return enriched.drop(columns=["wpl_birth_date", "wpl_birth_year", "wpl_birth_place_raw"])


def enrich_from_wikipedia(s: requests.Session, df: pd.DataFrame, checkpoint_path: Path) -> pd.DataFrame:
    needs = df[
        df["wikipedia_url"].map(lambda x: not is_blank(x))
        & (
            df["birth_date"].map(lambda x: parse_iso_date(clean_text(x)) == "")
            | df["birth_place_raw"].map(is_blank)
            | df["birth_country_raw"].map(is_blank)
            | df["birth_admin_labels"].map(is_blank)
            | (df["birth_country_raw"].map(clean_text).eq("United States") & df["birth_admin_labels"].map(is_blank))
            | df["genres_raw"].map(is_blank)
            | df["name_primary"].map(is_blank)
        )
    ].copy()

    if needs.empty:
        return df

    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(
            columns=[
                "wikipedia_url",
                "wp_page_title",
                "wp_birth_date",
                "wp_birth_year",
                "wp_birth_name",
                "wp_stage_name",
                "wp_birth_place_raw",
                "wp_aliases",
                "wp_genres_raw",
                "wp_categories",
            ]
        )
    for column in [
        "wikipedia_url",
        "wp_page_title",
        "wp_birth_date",
        "wp_birth_year",
        "wp_birth_name",
        "wp_stage_name",
        "wp_birth_place_raw",
        "wp_aliases",
        "wp_genres_raw",
        "wp_categories",
    ]:
        if column not in checkpoint_df.columns:
            checkpoint_df[column] = ""

    done_urls = set(checkpoint_df["wikipedia_url"].dropna().astype(str).tolist())
    pending = needs[~needs["wikipedia_url"].isin(done_urls)].copy()
    log(f"Wikipedia enrichment pending for {len(pending)} records")

    new_rows = []
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        url = clean_text(row["wikipedia_url"])
        if not url:
            continue
        try:
            new_rows.append(parse_wikipedia_infobox(s, url, clean_text(row["name_primary"])))
            time.sleep(0.1)
        except Exception as exc:
            new_rows.append(
                {
                    "wikipedia_url": url,
                    "wp_page_title": extract_wikipedia_title(url),
                    "wp_birth_date": "",
                    "wp_birth_year": "",
                    "wp_birth_name": "",
                    "wp_stage_name": "",
                    "wp_birth_place_raw": "",
                    "wp_aliases": "",
                    "wp_genres_raw": "",
                    "wp_categories": "",
                }
            )
            log(f"Wikipedia enrichment failed for {url}: {exc}")

        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["wikipedia_url"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            log(f"Saved Wikipedia enrichment checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    if new_rows:
        checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
        checkpoint_df = checkpoint_df.drop_duplicates(subset=["wikipedia_url"], keep="last")
        checkpoint_df.to_csv(checkpoint_path, index=False)

    enriched = df.merge(checkpoint_df, on="wikipedia_url", how="left")
    enriched["birth_date"] = enriched.apply(
        lambda row: clean_text(row["birth_date"]) if parse_iso_date(clean_text(row["birth_date"])) else clean_text(row["wp_birth_date"]),
        axis=1,
    )
    enriched["birth_name"] = enriched.apply(
        lambda row: clean_text(row["birth_name"]) or clean_text(row["wp_birth_name"]),
        axis=1,
    )
    enriched["stage_name"] = enriched.apply(
        lambda row: clean_text(row["stage_name"]) or clean_text(row["wp_stage_name"]),
        axis=1,
    )
    enriched["birth_place_raw"] = enriched.apply(
        lambda row: prefer_more_informative_text(row["birth_place_raw"], row["wp_birth_place_raw"]),
        axis=1,
    )
    enriched["aliases"] = enriched.apply(
        lambda row: pipe_join([clean_text(row["aliases"]), clean_text(row["wp_aliases"])]),
        axis=1,
    )
    enriched["genres_raw"] = enriched.apply(
        lambda row: pipe_join([clean_text(row["genres_raw"]), clean_text(row["wp_genres_raw"])]),
        axis=1,
    )
    return enriched.drop(
        columns=[
            "wp_birth_date",
            "wp_birth_name",
            "wp_stage_name",
            "wp_birth_place_raw",
            "wp_aliases",
            "wp_genres_raw",
        ]
    )


def fill_missing_wikipedia_urls_from_sitelinks(s: requests.Session, df: pd.DataFrame) -> pd.DataFrame:
    needs = df[df["wikidata_qid"].map(lambda x: not is_blank(x)) & df["wikipedia_url"].map(is_blank)].copy()
    if needs.empty:
        return df
    cache: Dict[str, dict] = {}
    qids = needs["wikidata_qid"].dropna().astype(str).unique().tolist()
    try:
        ensure_entity_cache(s, qids, cache, props="sitelinks")
    except Exception as exc:
        log(f"Skipping sitelink backfill after Wikidata rate limit/failure: {exc}")
        return df
    url_map = {}
    for qid in qids:
        entity = cache.get(qid, {})
        title = entity.get("sitelinks", {}).get("enwiki", {}).get("title", "")
        if title:
            url_map[qid] = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
    if not url_map:
        return df
    filled = df.copy()
    filled["wikipedia_url"] = filled.apply(
        lambda row: clean_text(row["wikipedia_url"]) or clean_text(url_map.get(clean_text(row["wikidata_qid"]), "")),
        axis=1,
    )
    return filled


def musicbrainz_area_context(
    s: requests.Session,
    area_id: str,
    checkpoint: Dict[str, dict],
) -> dict:
    if not area_id:
        return {"mb_birth_city": "", "mb_birth_county": "", "mb_birth_state": "", "mb_birth_country": ""}
    if area_id in checkpoint:
        return checkpoint[area_id]

    payload = request_json(
        s,
        f"{MUSICBRAINZ_API}/area/{area_id}",
        params={"fmt": "json", "inc": "area-rels"},
        pause=1.05,
    )
    area_name = clean_text(payload.get("name", ""))
    area_type = clean_text(payload.get("type", ""))
    state = ""
    country = ""
    county = ""
    city = ""

    if area_type == "County" or "County" in area_name:
        county = area_name
    elif area_type == "City":
        city = area_name
    elif area_name in US_STATE_ABBR:
        state = area_name

    for code in payload.get("iso-3166-2-codes", []) or []:
        if code.startswith("US-"):
            candidate = US_ABBR_STATE.get(code.split("-", 1)[1], "")
            if candidate:
                state = candidate
                country = "United States"

    for relation in payload.get("relations", []) or []:
        rel_area = relation.get("area", {})
        rel_name = clean_text(rel_area.get("name", ""))
        rel_type = clean_text(rel_area.get("type", ""))
        for code in rel_area.get("iso-3166-2-codes", []) or []:
            if code.startswith("US-"):
                candidate = US_ABBR_STATE.get(code.split("-", 1)[1], "")
                if candidate:
                    state = candidate
                    country = "United States"
        for code in rel_area.get("iso-3166-1-codes", []) or []:
            if code == "US":
                country = "United States"
        if rel_name in US_STATE_ABBR and not state:
            state = rel_name
            country = "United States"
        if rel_type == "Country" and rel_name == "United States":
            country = "United States"

    result = {
        "mb_birth_city": city,
        "mb_birth_county": county,
        "mb_birth_state": state,
        "mb_birth_country": country,
    }
    checkpoint[area_id] = result
    return result


def targeted_musicbrainz_bio_enrichment(s: requests.Session, df: pd.DataFrame, checkpoint_path: Path) -> pd.DataFrame:
    needs = df[
        df["musicbrainz_mbid"].map(lambda x: not is_blank(x))
        & (
            df["birth_year"].isna()
            | df["birth_city"].map(is_blank)
            | df["birth_state"].map(is_blank)
            | df["birth_country"].map(is_blank)
        )
    ].copy()
    if needs.empty:
        return df

    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(
            columns=[
                "musicbrainz_mbid",
                "mb_birth_date",
                "mb_birth_year",
                "mb_birth_city",
                "mb_birth_county",
                "mb_birth_state",
                "mb_birth_country",
            ]
        )
    for column in [
        "musicbrainz_mbid",
        "mb_birth_date",
        "mb_birth_year",
        "mb_birth_city",
        "mb_birth_county",
        "mb_birth_state",
        "mb_birth_country",
    ]:
        if column not in checkpoint_df.columns:
            checkpoint_df[column] = ""

    done = set(checkpoint_df["musicbrainz_mbid"].dropna().astype(str).tolist())
    pending = needs[~needs["musicbrainz_mbid"].isin(done)].copy()
    log(f"Targeted MusicBrainz bio enrichment pending for {len(pending)} records")

    area_cache: Dict[str, dict] = {}
    new_rows = []
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        mbid = clean_text(row["musicbrainz_mbid"])
        try:
            payload = request_json(s, f"{MUSICBRAINZ_API}/artist/{mbid}", params={"fmt": "json"}, pause=1.05)
            begin_area = payload.get("begin-area", {}) or {}
            begin_area_name = clean_text(begin_area.get("name", ""))
            begin_area_type = clean_text(begin_area.get("type", ""))
            begin_area_id = clean_text(begin_area.get("id", ""))
            area = payload.get("area", {}) or {}
            area_name = clean_text(area.get("name", ""))
            area_state = area_name if area_name in US_STATE_ABBR else ""
            area_country = "United States" if clean_text(payload.get("country", "")) == "US" else ""
            area_ctx = musicbrainz_area_context(s, begin_area_id, area_cache) if begin_area_id else {
                "mb_birth_city": "",
                "mb_birth_county": "",
                "mb_birth_state": "",
                "mb_birth_country": "",
            }
            mb_birth_date = parse_iso_date(clean_text(payload.get("life-span", {}).get("begin", "")))
            mb_birth_year = extract_year_hint(clean_text(payload.get("life-span", {}).get("begin", ""))) or ""
            mb_birth_city = ""
            mb_birth_county = area_ctx["mb_birth_county"]
            mb_birth_state = area_ctx["mb_birth_state"] or area_state
            mb_birth_country = area_ctx["mb_birth_country"] or area_country

            if begin_area_type == "City" and begin_area_name:
                mb_birth_city = begin_area_name
            elif begin_area_name and "County" in begin_area_name:
                mb_birth_county = mb_birth_county or begin_area_name

            new_rows.append(
                {
                    "musicbrainz_mbid": mbid,
                    "mb_birth_date": mb_birth_date,
                    "mb_birth_year": mb_birth_year,
                    "mb_birth_city": mb_birth_city,
                    "mb_birth_county": mb_birth_county,
                    "mb_birth_state": mb_birth_state,
                    "mb_birth_country": mb_birth_country,
                }
            )
        except Exception as exc:
            new_rows.append(
                {
                    "musicbrainz_mbid": mbid,
                    "mb_birth_date": "",
                    "mb_birth_year": "",
                    "mb_birth_city": "",
                    "mb_birth_county": "",
                    "mb_birth_state": "",
                    "mb_birth_country": "",
                }
            )
            log(f"Targeted MusicBrainz enrichment failed for {mbid}: {exc}")

        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["musicbrainz_mbid"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            log(f"Saved MusicBrainz bio checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    enriched = df.merge(checkpoint_df, on="musicbrainz_mbid", how="left")
    enriched["birth_date"] = enriched.apply(
        lambda row: clean_text(row["birth_date"]) or clean_text(row["mb_birth_date"]),
        axis=1,
    )
    enriched["birth_year"] = enriched.apply(
        lambda row: row["birth_year"] if pd.notna(row["birth_year"]) else extract_year_hint(row.get("mb_birth_year", "")),
        axis=1,
    )
    enriched["birth_city"] = enriched.apply(
        lambda row: clean_text(row["birth_city"]) or clean_text(row["mb_birth_city"]),
        axis=1,
    )
    enriched["birth_county"] = enriched.apply(
        lambda row: clean_text(row["birth_county"]) or clean_text(row["mb_birth_county"]),
        axis=1,
    )
    enriched["birth_state"] = enriched.apply(
        lambda row: clean_text(row["birth_state"]) or clean_text(row["mb_birth_state"]),
        axis=1,
    )
    enriched["birth_country"] = enriched.apply(
        lambda row: clean_text(row["birth_country"]) or clean_text(row["mb_birth_country"]),
        axis=1,
    )
    enriched["birth_state_abbr"] = enriched["birth_state"].map(lambda x: US_STATE_ABBR.get(clean_text(x), ""))
    enriched["notes"] = enriched.apply(
        lambda row: pipe_join(
            [
                clean_text(row.get("notes", "")),
                "birth data enriched from MusicBrainz"
                if any(
                    clean_text(row.get(col, ""))
                    for col in ["mb_birth_date", "mb_birth_city", "mb_birth_county", "mb_birth_state", "mb_birth_country"]
                )
                else "",
            ]
        ),
        axis=1,
    )
    return enriched.drop(
        columns=[
            "mb_birth_date",
            "mb_birth_year",
            "mb_birth_city",
            "mb_birth_county",
            "mb_birth_state",
            "mb_birth_country",
        ]
    )


def page_links(s: requests.Session, page_title: str) -> List[str]:
    payload = request_json(
        s,
        WIKIPEDIA_API,
        params={"action": "parse", "page": page_title, "prop": "links", "format": "json", "formatversion": 2},
    )
    links = []
    for item in payload.get("parse", {}).get("links", []):
        if item.get("ns") == 0 and item.get("exists") is not False:
            title = clean_title(item.get("*", item.get("title", "")))
            if title:
                links.append(title)
    return links


def search_wikidata_qid(s: requests.Session, title: str) -> Optional[str]:
    payload = request_json(
        s,
        WIKIDATA_API,
        params={
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "limit": 5,
            "search": title,
        },
        pause=0.05,
    )
    title_lower = title.lower()
    for item in payload.get("search", []):
        label = str(item.get("label", "")).lower()
        description = str(item.get("description", "")).lower()
        aliases = " ".join(alias.lower() for alias in item.get("aliases", []))
        if (label == title_lower or title_lower in aliases) and any(
            keyword in description for keyword in ["singer", "musician", "songwriter", "artist", "performer"]
        ):
            return item.get("id")
    if payload.get("search"):
        return payload["search"][0].get("id")
    return None


def wikipedia_titles_to_qids(s: requests.Session, titles: Sequence[str]) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    cleaned_titles = [clean_title(title) for title in titles if clean_title(title)]
    for batch in batched(cleaned_titles, 50):
        payload = request_json(
            s,
            WIKIPEDIA_API,
            params={
                "action": "query",
                "format": "json",
                "prop": "pageprops",
                "ppprop": "wikibase_item",
                "redirects": 1,
                "titles": "|".join(batch),
            },
            pause=0.05,
        )
        normalized = {item["from"]: item["to"] for item in payload.get("query", {}).get("normalized", [])}
        redirects = {item["from"]: item["to"] for item in payload.get("query", {}).get("redirects", [])}
        pages = payload.get("query", {}).get("pages", {})
        batch_result: Dict[str, Optional[str]] = {}
        for page in pages.values():
            title = clean_title(page.get("title", ""))
            qid = page.get("pageprops", {}).get("wikibase_item")
            if title:
                batch_result[title] = qid
        for original in batch:
            resolved = redirects.get(normalized.get(original, original), normalized.get(original, original))
            resolved = clean_title(resolved)
            mapping[original] = batch_result.get(resolved)
    return mapping


def collect_seed_candidates(s: requests.Session) -> pd.DataFrame:
    seed_rows = []
    for category in WIKIPEDIA_CATEGORY_TITLES:
        try:
            titles = category_members(s, category)
            log(f"Collected {len(titles)} titles from {category}")
            for title in titles:
                seed_rows.append({"seed_title": title, "source_seed": category})
        except Exception as exc:
            log(f"Skipping category {category}: {exc}")
    for page in WIKIPEDIA_LIST_PAGES:
        try:
            titles = page_links(s, page)
            log(f"Collected {len(titles)} links from {page}")
            for title in titles:
                seed_rows.append({"seed_title": title, "source_seed": page})
        except Exception as exc:
            log(f"Skipping page {page}: {exc}")

    df = pd.DataFrame(seed_rows)
    if df.empty:
        return pd.DataFrame(columns=["seed_title", "source_seed", "wikidata_qid"])

    df["seed_title"] = df["seed_title"].map(clean_title)
    df = df.drop_duplicates()
    qid_map = wikipedia_titles_to_qids(s, df["seed_title"].tolist())
    df["wikidata_qid"] = df["seed_title"].map(qid_map)

    unresolved_titles = df[df["wikidata_qid"].isna()]["seed_title"].tolist()
    if unresolved_titles:
        log(f"Falling back to Wikidata search for {len(unresolved_titles)} unresolved Wikipedia titles")
    fallback_map: Dict[str, Optional[str]] = {}
    for title in unresolved_titles:
        try:
            fallback_map[title] = search_wikidata_qid(s, title)
            time.sleep(0.2)
        except Exception as exc:
            log(f"Wikidata lookup failed for {title}: {exc}")
            fallback_map[title] = None
    if fallback_map:
        df["wikidata_qid"] = df.apply(
            lambda row: row["wikidata_qid"] if pd.notna(row["wikidata_qid"]) else fallback_map.get(row["seed_title"]),
            axis=1,
        )
    resolved = int(df["wikidata_qid"].notna().sum())
    unresolved = int(df["wikidata_qid"].isna().sum())
    log(f"Resolved {resolved} Wikipedia titles to Wikidata QIDs; unresolved: {unresolved}")
    return df


def broad_country_candidates(s: requests.Session) -> pd.DataFrame:
    query = """
    SELECT DISTINCT ?person ?personLabel WHERE {
      ?person wdt:P31 wd:Q5 ;
              wdt:P19 ?birthPlace ;
              wdt:P136 ?genre .
      ?birthPlace wdt:P17 wd:Q30 .
      ?genre rdfs:label ?genreLabel .
      FILTER(LANG(?genreLabel) = "en")
      FILTER(REGEX(LCASE(STR(?genreLabel)), "country|bluegrass|americana|honky|western swing|outlaw"))
      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """
    rows = []
    try:
        if not SPARQL_AVAILABLE:
            return pd.DataFrame(columns=["seed_title", "source_seed", "wikidata_qid"])
        # This expansion is optional; fail fast so a cold start does not stall before
        # the first checkpoint is written.
        for row in post_sparql(s, query, attempts=1, timeout=10, backoff_seconds=2):
            person_url = row["person"]["value"]
            rows.append(
                {
                    "seed_title": row.get("personLabel", {}).get("value", ""),
                    "source_seed": "Wikidata genre query",
                    "wikidata_qid": person_url.rsplit("/", 1)[-1],
                }
            )
    except Exception as exc:
        disable_sparql(f"broad candidate expansion failed: {exc}")
        log(f"Skipping broad Wikidata expansion after query failure: {exc}")
        return pd.DataFrame(columns=["seed_title", "source_seed", "wikidata_qid"])
    return pd.DataFrame(rows)


def batched(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for index in range(0, len(values), size):
        yield list(values[index : index + size])


def wikidata_entities(s: requests.Session, ids: Sequence[str], props: str = "labels|aliases|claims|sitelinks") -> Dict[str, dict]:
    entities: Dict[str, dict] = {}
    cleaned_ids = [entity_id for entity_id in ids if entity_id]
    for batch in batched(cleaned_ids, 50):
        payload = request_json(
            s,
            WIKIDATA_API,
            params={
                "action": "wbgetentities",
                "format": "json",
                "ids": "|".join(batch),
                "languages": "en",
                "props": props,
            },
            pause=0.2,
        )
        entities.update(payload.get("entities", {}))
    return entities


def label_from_entity(entity: Optional[dict]) -> str:
    if not entity:
        return ""
    return entity.get("labels", {}).get("en", {}).get("value", "")


def aliases_from_entity(entity: Optional[dict]) -> str:
    if not entity:
        return ""
    aliases = entity.get("aliases", {}).get("en", [])
    return pipe_join(item.get("value", "") for item in aliases)


def claim_datavalues(entity: Optional[dict], prop: str) -> List[dict]:
    if not entity:
        return []
    values = []
    for claim in entity.get("claims", {}).get(prop, []):
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue")
        if datavalue is not None:
            values.append(datavalue.get("value"))
    return values


def entity_id_claims(entity: Optional[dict], prop: str) -> List[str]:
    ids = []
    for value in claim_datavalues(entity, prop):
        if isinstance(value, dict):
            entity_id = value.get("id")
            if entity_id:
                ids.append(entity_id)
    return ids


def string_claim(entity: Optional[dict], prop: str) -> str:
    values = claim_datavalues(entity, prop)
    for value in values:
        if isinstance(value, dict) and "text" in value:
            return value.get("text", "")
        if isinstance(value, str):
            return value
    return ""


def time_claim(entity: Optional[dict], prop: str) -> str:
    values = claim_datavalues(entity, prop)
    for value in values:
        if isinstance(value, dict) and "time" in value:
            return value.get("time", "")
    return ""


def claim_text_values(
    entity: Optional[dict],
    prop: str,
    cache: Optional[Dict[str, dict]] = None,
    s: Optional[requests.Session] = None,
) -> List[str]:
    output: List[str] = []
    for value in claim_datavalues(entity, prop):
        if isinstance(value, dict):
            if "text" in value:
                output.append(clean_text(value.get("text", "")))
                continue
            if "time" in value:
                output.append(clean_text(value.get("time", "")))
                continue
            if "id" in value:
                entity_id = clean_text(value.get("id", ""))
                if entity_id and cache is not None and s is not None:
                    ensure_entity_cache(s, [entity_id], cache, props="labels|claims|aliases|sitelinks")
                    output.append(label_from_entity(cache.get(entity_id)))
                continue
        elif isinstance(value, str):
            output.append(clean_text(value))
    return [piece for piece in output if piece]


def claim_text(
    entity: Optional[dict],
    prop: str,
    cache: Optional[Dict[str, dict]] = None,
    s: Optional[requests.Session] = None,
) -> str:
    values = claim_text_values(entity, prop, cache=cache, s=s)
    return values[0] if values else ""


def ensure_entity_cache(s: requests.Session, entity_ids: Sequence[str], cache: Dict[str, dict], props: str = "labels|claims|aliases|sitelinks") -> None:
    missing = [entity_id for entity_id in entity_ids if entity_id and entity_id not in cache]
    if not missing:
        return
    cache.update(wikidata_entities(s, missing, props=props))


def entity_labels(entity_ids: Sequence[str], cache: Dict[str, dict]) -> str:
    return pipe_join(label_from_entity(cache.get(entity_id)) for entity_id in entity_ids)


def place_context(
    place_id: str,
    cache: Dict[str, dict],
    s: requests.Session,
    resolved_cache: Optional[Dict[str, dict]] = None,
    max_depth: int = 5,
) -> dict:
    if not place_id:
        return {"birth_place_raw": "", "birth_admin_labels": "", "birth_country_raw": ""}
    if resolved_cache is not None and place_id in resolved_cache:
        return resolved_cache[place_id]

    ensure_entity_cache(s, [place_id], cache)
    place_entity = cache.get(place_id, {})
    place_label = label_from_entity(place_entity)

    admin_labels: List[str] = []
    country_label = ""
    seen = set()
    frontier = [(place_id, 0)]

    while frontier:
        current_id, depth = frontier.pop(0)
        if current_id in seen or depth > max_depth:
            continue
        seen.add(current_id)
        ensure_entity_cache(s, [current_id], cache)
        current_entity = cache.get(current_id, {})

        if current_id != place_id:
            label = label_from_entity(current_entity)
            if label:
                admin_labels.append(label)

        country_ids = entity_id_claims(current_entity, "P17")
        if country_ids and not country_label:
            ensure_entity_cache(s, country_ids, cache)
            country_label = label_from_entity(cache.get(country_ids[0]))

        parent_ids = entity_id_claims(current_entity, "P131")
        ensure_entity_cache(s, parent_ids, cache)
        for parent_id in parent_ids:
            frontier.append((parent_id, depth + 1))

    result = {
        "birth_place_raw": place_label,
        "birth_admin_labels": pipe_join(admin_labels),
        "birth_country_raw": country_label,
    }
    if resolved_cache is not None:
        resolved_cache[place_id] = result
    return result


def wikidata_detail_query(batch: Sequence[str]) -> str:
    values = " ".join(f"wd:{qid}" for qid in batch if qid)
    return f"""
    SELECT ?person
           (SAMPLE(?personLabelRaw) AS ?personLabel)
           (SAMPLE(?birthNameRaw) AS ?birthName)
           (SAMPLE(?stageNameRaw) AS ?stageName)
           (GROUP_CONCAT(DISTINCT ?alias;separator="|") AS ?aliases)
           (SAMPLE(?birthDateRaw) AS ?birthDate)
           (SAMPLE(?deathDateRaw) AS ?deathDate)
           (SAMPLE(?birthPlaceLabelRaw) AS ?birthPlace)
           (SAMPLE(?birthStateLabelRaw) AS ?birthState)
           (SAMPLE(?birthCountryLabelRaw) AS ?birthCountry)
           (SAMPLE(?deathPlaceLabelRaw) AS ?deathPlace)
           (GROUP_CONCAT(DISTINCT ?citizenshipLabel;separator="|") AS ?citizenships)
           (GROUP_CONCAT(DISTINCT ?occupationLabel;separator="|") AS ?occupations)
           (GROUP_CONCAT(DISTINCT ?genreLabel;separator="|") AS ?genres)
           (GROUP_CONCAT(DISTINCT ?instrumentLabel;separator="|") AS ?instruments)
           (GROUP_CONCAT(DISTINCT ?memberOfLabel;separator="|") AS ?memberOf)
           (GROUP_CONCAT(DISTINCT ?recordLabelName;separator="|") AS ?recordLabels)
           (GROUP_CONCAT(DISTINCT ?awardLabel;separator="|") AS ?awards)
           (SAMPLE(STR(?websiteRaw)) AS ?website)
           (SAMPLE(STR(?wikipediaRaw)) AS ?wikipedia)
           (SAMPLE(STR(?mbidRaw)) AS ?mbid)
           (SAMPLE(STR(?isniRaw)) AS ?isni)
           (SAMPLE(STR(?viafRaw)) AS ?viaf)
    WHERE {{
      VALUES ?person {{ {values} }}
      OPTIONAL {{ ?person rdfs:label ?personLabelRaw FILTER(LANG(?personLabelRaw) = "en") }}
      OPTIONAL {{ ?person skos:altLabel ?alias FILTER(LANG(?alias) = "en") }}
      OPTIONAL {{ ?person wdt:P1477 ?birthNameRaw FILTER(LANG(?birthNameRaw) = "en") }}
      OPTIONAL {{ ?person wdt:P742 ?stageNameRaw FILTER(LANG(?stageNameRaw) = "en") }}
      OPTIONAL {{ ?person wdt:P569 ?birthDateRaw }}
      OPTIONAL {{ ?person wdt:P570 ?deathDateRaw }}
      OPTIONAL {{
        ?person wdt:P19 ?birthPlaceEntity .
        ?birthPlaceEntity rdfs:label ?birthPlaceLabelRaw FILTER(LANG(?birthPlaceLabelRaw) = "en")
        OPTIONAL {{
          ?birthPlaceEntity wdt:P131* ?birthStateEntity .
          ?birthStateEntity wdt:P31/wdt:P279* wd:Q35657 .
          ?birthStateEntity rdfs:label ?birthStateLabelRaw FILTER(LANG(?birthStateLabelRaw) = "en")
        }}
        OPTIONAL {{
          ?birthPlaceEntity wdt:P131*/wdt:P17 ?birthCountryEntity .
          ?birthCountryEntity rdfs:label ?birthCountryLabelRaw FILTER(LANG(?birthCountryLabelRaw) = "en")
        }}
      }}
      OPTIONAL {{
        ?person wdt:P20 ?deathPlaceEntity .
        ?deathPlaceEntity rdfs:label ?deathPlaceLabelRaw FILTER(LANG(?deathPlaceLabelRaw) = "en")
      }}
      OPTIONAL {{ ?person wdt:P27 ?citizenshipEntity .
                 ?citizenshipEntity rdfs:label ?citizenshipLabel FILTER(LANG(?citizenshipLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P106 ?occupationEntity .
                 ?occupationEntity rdfs:label ?occupationLabel FILTER(LANG(?occupationLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P136 ?genreEntity .
                 ?genreEntity rdfs:label ?genreLabel FILTER(LANG(?genreLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P1303 ?instrumentEntity .
                 ?instrumentEntity rdfs:label ?instrumentLabel FILTER(LANG(?instrumentLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P463 ?memberOfEntity .
                 ?memberOfEntity rdfs:label ?memberOfLabel FILTER(LANG(?memberOfLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P264 ?recordLabelEntity .
                 ?recordLabelEntity rdfs:label ?recordLabelName FILTER(LANG(?recordLabelName) = "en") }}
      OPTIONAL {{ ?person wdt:P166 ?awardEntity .
                 ?awardEntity rdfs:label ?awardLabel FILTER(LANG(?awardLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P856 ?websiteRaw }}
      OPTIONAL {{
        ?wikipediaRaw schema:about ?person ;
                      schema:isPartOf <https://en.wikipedia.org/> .
      }}
      OPTIONAL {{ ?person wdt:P434 ?mbidRaw }}
      OPTIONAL {{ ?person wdt:P213 ?isniRaw }}
      OPTIONAL {{ ?person wdt:P214 ?viafRaw }}
    }}
    GROUP BY ?person
    """


def parse_wikidata_detail_rows(results: List[dict]) -> List[dict]:
    rows = []
    for item in results:
        person_url = item["person"]["value"]
        birth_state = item.get("birthState", {}).get("value", "")
        rows.append(
            {
                "wikidata_qid": person_url.rsplit("/", 1)[-1],
                "name_primary": item.get("personLabel", {}).get("value", ""),
                "birth_name": item.get("birthName", {}).get("value", ""),
                "stage_name": item.get("stageName", {}).get("value", ""),
                "aliases": item.get("aliases", {}).get("value", ""),
                "birth_date": item.get("birthDate", {}).get("value", ""),
                "death_date": item.get("deathDate", {}).get("value", ""),
                "birth_place_raw": item.get("birthPlace", {}).get("value", ""),
                "birth_admin_labels": birth_state,
                "birth_country_raw": item.get("birthCountry", {}).get("value", ""),
                "death_place_raw": item.get("deathPlace", {}).get("value", ""),
                "citizenship": item.get("citizenships", {}).get("value", ""),
                "occupations": item.get("occupations", {}).get("value", ""),
                "genres_raw": item.get("genres", {}).get("value", ""),
                "instruments": item.get("instruments", {}).get("value", ""),
                "member_of": item.get("memberOf", {}).get("value", ""),
                "record_labels": item.get("recordLabels", {}).get("value", ""),
                "awards": item.get("awards", {}).get("value", ""),
                "official_website": item.get("website", {}).get("value", ""),
                "wikipedia_url": item.get("wikipedia", {}).get("value", ""),
                "musicbrainz_mbid": item.get("mbid", {}).get("value", ""),
                "isni": item.get("isni", {}).get("value", ""),
                "viaf_id": item.get("viaf", {}).get("value", ""),
            }
        )
    return rows


def wikidata_detail_rows_from_entities(s: requests.Session, batch: Sequence[str]) -> List[dict]:
    entity_cache = wikidata_entities(s, batch, props="labels|aliases|claims|sitelinks")
    related_ids: List[str] = []
    for qid in batch:
        entity = entity_cache.get(qid, {})
        birth_place_ids = entity_id_claims(entity, "P19")
        death_place_ids = entity_id_claims(entity, "P20")
        related_ids.extend(birth_place_ids)
        related_ids.extend(death_place_ids)
        related_ids.extend(entity_id_claims(entity, "P27"))
        related_ids.extend(entity_id_claims(entity, "P106"))
        related_ids.extend(entity_id_claims(entity, "P136"))
        related_ids.extend(entity_id_claims(entity, "P1303"))
        related_ids.extend(entity_id_claims(entity, "P463"))
        related_ids.extend(entity_id_claims(entity, "P264"))
        related_ids.extend(entity_id_claims(entity, "P166"))
        for place_id in birth_place_ids + death_place_ids:
            place_entity = entity_cache.get(place_id, {})
            related_ids.extend(entity_id_claims(place_entity, "P131"))
            related_ids.extend(entity_id_claims(place_entity, "P17"))
    ensure_entity_cache(s, related_ids, entity_cache, props="labels|claims|aliases|sitelinks")

    rows: List[dict] = []
    for qid in batch:
        entity = entity_cache.get(qid, {})
        birth_place_ids = entity_id_claims(entity, "P19")
        death_place_ids = entity_id_claims(entity, "P20")
        birth_place_id = birth_place_ids[0] if birth_place_ids else ""
        death_place_id = death_place_ids[0] if death_place_ids else ""
        birth_place_entity = entity_cache.get(birth_place_id, {}) if birth_place_id else {}
        birth_parent_ids = entity_id_claims(birth_place_entity, "P131")
        birth_country_ids = entity_id_claims(birth_place_entity, "P17")
        death_place_label = label_from_entity(entity_cache.get(death_place_id)) if death_place_id else ""
        wikipedia_title = entity.get("sitelinks", {}).get("enwiki", {}).get("title", "")

        rows.append(
            {
                "wikidata_qid": qid,
                "name_primary": label_from_entity(entity),
                "birth_name": claim_text(entity, "P1477", cache=entity_cache, s=s),
                "stage_name": claim_text(entity, "P742", cache=entity_cache, s=s),
                "aliases": aliases_from_entity(entity),
                "birth_date": time_claim(entity, "P569"),
                "death_date": time_claim(entity, "P570"),
                "birth_place_raw": label_from_entity(birth_place_entity),
                "birth_admin_labels": entity_labels(birth_parent_ids, entity_cache),
                "birth_country_raw": label_from_entity(entity_cache.get(birth_country_ids[0])) if birth_country_ids else "",
                "death_place_raw": death_place_label,
                "citizenship": entity_labels(entity_id_claims(entity, "P27"), entity_cache),
                "occupations": entity_labels(entity_id_claims(entity, "P106"), entity_cache),
                "genres_raw": entity_labels(entity_id_claims(entity, "P136"), entity_cache),
                "instruments": entity_labels(entity_id_claims(entity, "P1303"), entity_cache),
                "member_of": entity_labels(entity_id_claims(entity, "P463"), entity_cache),
                "record_labels": entity_labels(entity_id_claims(entity, "P264"), entity_cache),
                "awards": entity_labels(entity_id_claims(entity, "P166"), entity_cache),
                "official_website": claim_text(entity, "P856"),
                "wikipedia_url": f"https://en.wikipedia.org/wiki/{wikipedia_title.replace(' ', '_')}" if wikipedia_title else "",
                "musicbrainz_mbid": claim_text(entity, "P434"),
                "isni": claim_text(entity, "P213"),
                "viaf_id": claim_text(entity, "P214"),
            }
        )
    return rows


def fetch_wikidata_detail_batch(s: requests.Session, batch: Sequence[str]) -> List[dict]:
    if not SPARQL_AVAILABLE:
        return wikidata_detail_rows_from_entities(s, batch)
    try:
        return parse_wikidata_detail_rows(
            post_sparql(s, wikidata_detail_query(batch), attempts=1, timeout=10, backoff_seconds=2)
        )
    except Exception as exc:
        disable_sparql(f"detail enrichment failed: {exc}")
        log(f"SPARQL detail fetch failed for batch of {len(batch)} QIDs; falling back to wbgetentities API: {exc}")
        try:
            return wikidata_detail_rows_from_entities(s, batch)
        except Exception as api_exc:
            log(f"wbgetentities fallback failed for batch of {len(batch)} QIDs: {api_exc}")
        if len(batch) == 1:
            qid = batch[0]
            log(f"Falling back to minimal Wikidata row for {qid}")
            return [
                {
                    "wikidata_qid": qid,
                    "name_primary": "",
                    "birth_name": "",
                    "stage_name": "",
                    "aliases": "",
                    "birth_date": "",
                    "death_date": "",
                    "birth_place_raw": "",
                    "birth_admin_labels": "",
                    "birth_country_raw": "",
                    "death_place_raw": "",
                    "citizenship": "",
                    "occupations": "",
                    "genres_raw": "",
                    "instruments": "",
                    "member_of": "",
                    "record_labels": "",
                    "awards": "",
                    "official_website": "",
                    "wikipedia_url": "",
                    "musicbrainz_mbid": "",
                    "isni": "",
                    "viaf_id": "",
                }
            ]
        midpoint = max(1, len(batch) // 2)
        left = fetch_wikidata_detail_batch(s, batch[:midpoint])
        right = fetch_wikidata_detail_batch(s, batch[midpoint:])
        return left + right


def wikidata_details(s: requests.Session, qids: Sequence[str], checkpoint_path: Optional[Path] = None) -> pd.DataFrame:
    rows: List[dict] = []
    ordered_qids = sorted(set(qids))
    detail_batch_size = 25 if not SPARQL_AVAILABLE else 10
    for batch in batched(ordered_qids, detail_batch_size):
        rows.extend(fetch_wikidata_detail_batch(s, batch))
        if checkpoint_path is not None:
            checkpoint_df = pd.DataFrame(rows)
            if checkpoint_path.exists():
                existing_checkpoint_df = pd.read_csv(checkpoint_path)
                checkpoint_df = pd.concat([existing_checkpoint_df, checkpoint_df], ignore_index=True)
                checkpoint_df = checkpoint_df.drop_duplicates(subset=["wikidata_qid"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
        log(f"Fetched Wikidata detail rows for batch of {len(batch)} QIDs")
    return pd.DataFrame(rows)


def parse_iso_date(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    direct_match = re.match(r"^[+-]?(\d{4}-\d{2}-\d{2})T", value)
    if direct_match:
        candidate = direct_match.group(1)
        if "-00-" in candidate or candidate.endswith("-00"):
            return ""
        try:
            return datetime.strptime(candidate, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        if "-00-" in value or value.endswith("-00"):
            return ""
        try:
            return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
        except ValueError:
            return ""
    try:
        return pd.to_datetime(value.lstrip("+"), utc=True).date().isoformat()
    except Exception:
        return ""


def resolve_birth_geography(row: pd.Series) -> dict:
    birth_admin_labels = "" if pd.isna(row.get("birth_admin_labels", "")) else str(row.get("birth_admin_labels", ""))
    labels = [piece.strip() for piece in birth_admin_labels.split("|") if piece.strip()]
    raw = str(row.get("birth_place_raw", "")).strip()
    birth_state = ""
    birth_country = ""
    birth_city = ""
    birth_county = ""

    if raw:
        birth_city = raw.split(",")[0].strip()
        if COUNTY_PATTERN.search(raw):
            birth_county = raw.split(",")[0].strip()

    for label in [raw] + labels:
        for state in US_STATE_ABBR:
            if re.search(rf"\b{re.escape(state)}\b", label):
                birth_state = state
                break
        if birth_state:
            break

    raw_country = str(row.get("birth_country_raw", "")).strip()
    combined_geo = "|".join([raw, birth_admin_labels, raw_country])
    if "United States" in combined_geo or "USA" in combined_geo or birth_state:
        birth_country = "United States"
    elif raw_country:
        birth_country = raw_country

    for label in labels:
        if COUNTY_PATTERN.search(label):
            birth_county = label
            break

    return {
        "birth_city": birth_city if birth_city and birth_city != birth_county else birth_city,
        "birth_county": birth_county,
        "birth_state": birth_state,
        "birth_state_abbr": US_STATE_ABBR.get(birth_state, ""),
        "birth_country": birth_country,
    }


def apply_category_location_fallback(row: pd.Series) -> dict:
    inferred = infer_us_location_from_categories(row.get("wp_categories", ""), row.get("source_seed", ""))
    birth_city = clean_text(row.get("birth_city", "")) or clean_text(inferred["birth_city_from_categories"])
    birth_county = clean_text(row.get("birth_county", "")) or clean_text(inferred["birth_county_from_categories"])
    birth_state = clean_text(row.get("birth_state", "")) or clean_text(inferred["birth_state_from_categories"])
    birth_country = clean_text(row.get("birth_country", "")) or clean_text(inferred["birth_country_from_categories"])
    if not birth_country and birth_state:
        birth_country = "United States"
    return {
        "birth_city": birth_city,
        "birth_county": birth_county,
        "birth_state": birth_state,
        "birth_state_abbr": US_STATE_ABBR.get(birth_state, ""),
        "birth_country": birth_country,
    }


def normalize_country_genres(genres_raw: str) -> str:
    if not genres_raw or pd.isna(genres_raw):
        return ""
    pieces = [piece.strip() for piece in re.split(r"\||,|;", str(genres_raw)) if piece.strip()]
    matched = []
    for piece in pieces:
        piece_lower = piece.lower()
        for key, value in GENRE_NORMALIZATION.items():
            if key in piece_lower:
                matched.append(value)
                break
        else:
            if "country" in piece_lower:
                matched.append("country")
    return pipe_join(matched)


def score_country_relevance(genres_raw: str, occupations: str, source_seed: str, awards: str, wikipedia_categories: str) -> float:
    score = 0.0
    genres_lower = "" if pd.isna(genres_raw) else str(genres_raw).lower()
    occupations_lower = "" if pd.isna(occupations) else str(occupations).lower()
    seed_lower = "" if pd.isna(source_seed) else str(source_seed).lower()
    awards_lower = "" if pd.isna(awards) else str(awards).lower()
    categories_lower = "" if pd.isna(wikipedia_categories) else str(wikipedia_categories).lower()
    if "country music hall of fame" in awards_lower or "country music hall of fame" in seed_lower:
        score += 0.45
    if "american country singer-songwriters" in seed_lower or "american country singers" in seed_lower:
        score += 0.35
    elif "country musicians from" in seed_lower or "list of country performers" in seed_lower:
        score += 0.25
    if "american country singer-songwriters" in categories_lower or "american country singers" in categories_lower:
        score += 0.05
    if COUNTRY_GENRE_PATTERN.search(genres_lower):
        score += 0.35
    if "country" in genres_lower:
        score += 0.15
    if COUNTRY_GENRE_PATTERN.search(categories_lower):
        score += 0.2
    if SINGER_PATTERN.search(occupations_lower):
        score += 0.1
    if "country singer" in occupations_lower:
        score += 0.15
    if "americana" in genres_lower or "bluegrass" in genres_lower:
        score += 0.05
    return min(score, 1.0)


def infer_samples(row: pd.Series) -> dict:
    birth_country = clean_text(row.get("birth_country", ""))
    is_us_born = int(birth_country == "United States")
    is_known_non_us = int(bool(birth_country) and birth_country != "United States")
    has_birth_country = int(bool(birth_country))
    is_solo_person = 1
    genres_norm = row["genres_normalized"]
    occupations = str(row["occupations"]).lower()
    score = row["country_relevance_score"]
    name_primary = "" if pd.isna(row["name_primary"]) else str(row["name_primary"]).strip()
    has_name = int(bool(name_primary) and name_primary.lower() != "nan")
    has_key_birth_metadata = int(pd.notna(row["birth_year"]) and bool(row["birth_state"]))

    is_country_core = int(
        any(genre in genres_norm.split("|") for genre in ["country", "country pop", "country rock", "outlaw country", "honky-tonk", "western swing"])
        or "country singer" in occupations
    )
    is_country_broad = int(bool(genres_norm) or score >= 0.35)

    birth_date_conf = row["birth_date_confidence"]
    birth_place_conf = row["birth_place_confidence"]
    restricted = int(
        is_us_born
        and is_solo_person
        and has_name
        and has_key_birth_metadata
        and is_country_core
        and row["manual_review_needed"] == 0
        and birth_date_conf in {"high", "medium"}
        and birth_place_conf in {"high", "medium"}
        and row["genre_confidence"] in {"medium", "high"}
        and score >= 0.5
    )
    expanded = int(is_us_born and is_solo_person and has_name and has_key_birth_metadata and is_country_broad)
    if restricted:
        sample_membership = "restricted"
        inclusion_reason = "US-born core country singer with reliable birth metadata"
    elif expanded:
        sample_membership = "expanded_only"
        inclusion_reason = "US-born country-relevant artist retained in expanded sample"
    else:
        sample_membership = ""
        inclusion_reason = ""

    exclusion_reason = ""
    if not has_name:
        exclusion_reason = "missing_name_primary"
    elif is_known_non_us:
        exclusion_reason = "non_us_birth"
    elif not has_birth_country:
        exclusion_reason = "unresolved_birth_country"
    elif not has_key_birth_metadata:
        exclusion_reason = "missing_key_birth_metadata"
    elif not is_country_broad:
        exclusion_reason = "insufficient_country_evidence"

    return {
        "is_us_born": is_us_born,
        "is_solo_person": is_solo_person,
        "is_country_core": is_country_core,
        "is_country_broad": is_country_broad,
        "flag_restricted_sample": restricted,
        "flag_expanded_sample": expanded,
        "sample_membership": sample_membership,
        "inclusion_reason": inclusion_reason,
        "exclusion_reason": exclusion_reason,
    }


def maybe_verify_musicbrainz(s: requests.Session, df: pd.DataFrame, max_lookups: int = 150) -> pd.DataFrame:
    if df.empty:
        return df
    verified = []
    lookups = 0
    for _, row in df.iterrows():
        mbid = row.get("musicbrainz_mbid", "")
        name = row.get("name_primary", "")
        birth_year = row.get("birth_year", "")
        mb_aliases = ""
        mb_name = ""
        mb_country = ""
        matched = 0
        evidence_url = row.get("evidence_urls", "")
        source_secondary = row.get("source_secondary", "")
        notes = row.get("notes", "")
        try:
            if mbid and lookups < max_lookups:
                payload = request_json(s, f"{MUSICBRAINZ_API}/artist/{mbid}", params={"fmt": "json", "inc": "aliases"}, pause=1.05)
                lookups += 1
                mb_name = payload.get("name", "")
                mb_country = payload.get("country", "")
                mb_aliases = pipe_join(alias.get("name", "") for alias in payload.get("aliases", []))
                matched = 1
            elif not mbid and lookups < max_lookups:
                payload = request_json(
                    s,
                    f"{MUSICBRAINZ_API}/artist/",
                    params={"fmt": "json", "limit": 1, "query": f'artist:"{name}"'},
                    pause=1.05,
                )
                lookups += 1
                artists = payload.get("artists", [])
                if artists:
                    candidate = artists[0]
                    candidate_year = str(candidate.get("life-span", {}).get("begin", ""))[:4]
                    if not birth_year or candidate_year == str(birth_year):
                        mbid = candidate.get("id", "")
                        mb_name = candidate.get("name", "")
                        mb_country = candidate.get("country", "")
                        matched = 1
        except Exception as exc:
            notes = pipe_join([notes, f"MusicBrainz lookup failed: {exc}"])

        if matched:
            source_secondary = "MusicBrainz"
            evidence_url = pipe_join([evidence_url, f"https://musicbrainz.org/artist/{mbid}"])
        verified.append(
            {
                **row.to_dict(),
                "musicbrainz_mbid": mbid,
                "source_secondary": source_secondary,
                "evidence_urls": evidence_url,
                "notes": notes,
                "aliases": pipe_join([row.get("aliases", ""), mb_aliases, mb_name]),
                "source_count": len([piece for piece in evidence_url.split("|") if piece]),
                "mb_country_code": mb_country,
            }
        )
    log(f"Completed MusicBrainz verification for {lookups} records")
    return pd.DataFrame(verified)


def prepare_dataset(seed_df: pd.DataFrame, detail_df: pd.DataFrame, s: requests.Session) -> pd.DataFrame:
    seed_summary = (
        seed_df.groupby("wikidata_qid", dropna=False)
        .agg(
            source_seed=("source_seed", lambda items: pipe_join(items)),
            seed_title=("seed_title", "first"),
        )
        .reset_index()
    )
    merged = detail_df.merge(seed_summary, on="wikidata_qid", how="left")
    merged["source_seed"] = merged["source_seed"].fillna("Wikidata genre query")
    merged = fill_missing_wikipedia_urls_from_sitelinks(s, merged)
    merged = enrich_from_wikipedia(s, merged, INTERMEDIATE_DIR / "country_artists_wikipedia_enrichment.csv")
    merged["name_primary"] = merged.apply(choose_name_primary, axis=1)
    merged["birth_date"] = merged["birth_date"].map(parse_iso_date)
    merged["death_date"] = merged["death_date"].map(parse_iso_date)
    merged["birth_date"] = merged["birth_date"].fillna("").astype(str).replace({"NaT": "", "nan": ""})
    merged["death_date"] = merged["death_date"].fillna("").astype(str).replace({"NaT": "", "nan": ""})
    merged["birth_year"] = merged["birth_date"].str.slice(0, 4).map(maybe_int)

    geo_df = merged.apply(resolve_birth_geography, axis=1, result_type="expand")
    merged = pd.concat([merged, geo_df], axis=1)
    merged = enrich_from_wikipedia_lead(s, merged, INTERMEDIATE_DIR / "country_artists_wikipedia_lead.csv")
    merged["birth_date"] = merged["birth_date"].map(parse_iso_date)
    merged["birth_date"] = merged["birth_date"].fillna("").astype(str).replace({"NaT": "", "nan": ""})
    merged["birth_year_hint"] = merged.apply(
        lambda row: maybe_int(clean_text(row["birth_date"])[:4])
        or row.get("birth_year")
        or extract_year_hint(row.get("wp_birth_year", ""))
        or extract_birth_year_from_categories(row.get("wp_categories", "")),
        axis=1,
    )
    geo_df = merged.apply(resolve_birth_geography, axis=1, result_type="expand")
    merged = merged.drop(columns=["birth_city", "birth_county", "birth_state", "birth_state_abbr", "birth_country"], errors="ignore")
    merged = pd.concat([merged, geo_df], axis=1)
    geo_fallback_df = merged.apply(apply_category_location_fallback, axis=1, result_type="expand")
    for col in ["birth_city", "birth_county", "birth_state", "birth_state_abbr", "birth_country"]:
        merged[col] = geo_fallback_df[col]

    merged["birth_year"] = merged.apply(
        lambda row: maybe_int(clean_text(row["birth_date"])[:4]) if clean_text(row["birth_date"]) else row["birth_year_hint"],
        axis=1,
    )
    merged["death_year"] = merged["death_date"].str.slice(0, 4).map(maybe_int)
    merged["birth_decade"] = merged["birth_year"].map(lambda x: int(math.floor(x / 10) * 10) if pd.notna(x) else None)
    merged["us_macro_region"] = merged["birth_state"].map(lambda x: STATE_TO_REGION.get(x, "Unknown"))
    merged["is_deceased"] = merged["death_date"].map(lambda x: 1 if x else 0)

    today = date.today()

    def calc_age(row: pd.Series) -> Optional[int]:
        if not row["birth_date"]:
            return None
        try:
            born = datetime.fromisoformat(row["birth_date"]).date()
            ended = datetime.fromisoformat(row["death_date"]).date() if row["death_date"] else today
            return ended.year - born.year - ((ended.month, ended.day) < (born.month, born.day))
        except Exception:
            return None

    merged["notes"] = ""
    merged = targeted_musicbrainz_bio_enrichment(s, merged, INTERMEDIATE_DIR / "country_artists_musicbrainz_bio.csv")
    merged["birth_year"] = merged.apply(
        lambda row: maybe_int(clean_text(row["birth_date"])[:4]) if clean_text(row["birth_date"]) else row["birth_year"],
        axis=1,
    )
    merged["age_or_age_at_death"] = merged.apply(calc_age, axis=1)
    merged["genres_normalized"] = merged["genres_raw"].map(normalize_country_genres)
    merged["instruments"] = merged["instruments"].map(lambda x: normalize_values(str(x), INSTRUMENT_NORMALIZATION))
    merged["country_relevance_score"] = merged.apply(
        lambda row: score_country_relevance(
            row["genres_raw"],
            row["occupations"],
            row["source_seed"],
            row["awards"],
            row.get("wp_categories", ""),
        ),
        axis=1,
    )
    merged["birth_date_confidence"] = merged.apply(
        lambda row: "high" if row["birth_date"] else ("medium" if pd.notna(row["birth_year"]) else "low"),
        axis=1,
    )
    merged["birth_place_confidence"] = merged.apply(
        lambda row: "high" if row["birth_state"] and row["birth_place_raw"] else ("medium" if row["birth_state"] else "low"),
        axis=1,
    )
    merged["genre_confidence"] = merged["country_relevance_score"].map(
        lambda x: "high" if x >= 0.8 else ("medium" if x >= 0.5 else "low")
    )
    merged["manual_review_needed"] = merged.apply(
        lambda row: 1
        if (
            not row["wikidata_qid"]
            or pd.isna(row["birth_year"])
            or (row["birth_country"] == "United States" and not row["birth_state"])
            or (row["genre_confidence"] == "low" and not has_strong_country_seed_evidence(row["source_seed"], row.get("wp_categories", "")))
        )
        else 0,
        axis=1,
    )
    merged["source_primary"] = "Wikidata"
    merged["source_secondary"] = ""
    merged["evidence_urls"] = merged.apply(
        lambda row: pipe_join(
            [
                f"https://www.wikidata.org/wiki/{row['wikidata_qid']}" if row["wikidata_qid"] else "",
                row["wikipedia_url"],
                f"https://musicbrainz.org/artist/{row['musicbrainz_mbid']}" if clean_text(row["musicbrainz_mbid"]) else "",
            ]
        ),
        axis=1,
    )
    merged["source_count"] = merged["evidence_urls"].map(lambda x: len([piece for piece in str(x).split("|") if piece]))
    merged["notes"] = merged.apply(
        lambda row: pipe_join(
            [
                clean_text(row.get("notes", "")),
                "birth_state unresolved from structured place hierarchy"
                if row["birth_country"] == "United States" and not row["birth_state"]
                else "",
                "birth geography inferred from Wikipedia categories"
                if row["birth_country"] == "United States" and row["birth_state"] and not row["birth_place_raw"]
                else "",
                "low country confidence" if row["genre_confidence"] == "low" else "",
            ]
        ),
        axis=1,
    )
    sample_df = merged.apply(infer_samples, axis=1, result_type="expand")
    merged = pd.concat([merged, sample_df], axis=1)
    merged["artist_id"] = [f"artist_{index:06d}" for index in range(1, len(merged) + 1)]
    merged = merged.drop(columns=["birth_year_hint"], errors="ignore")
    if "seed_title" in merged.columns:
        merged = merged.drop(columns=["seed_title"])
    return merged


def add_unresolved_seeds(seed_df: pd.DataFrame, dataset_df: pd.DataFrame) -> pd.DataFrame:
    unresolved = seed_df[seed_df["wikidata_qid"].isna()].copy()
    if unresolved.empty:
        return dataset_df

    rows = []
    for _, row in unresolved.iterrows():
        rows.append(
            {
                "artist_id": "",
                "name_primary": row["seed_title"],
                "birth_name": "",
                "stage_name": "",
                "aliases": "",
                "wikidata_qid": "",
                "musicbrainz_mbid": "",
                "isni": "",
                "viaf_id": "",
                "wikipedia_url": "",
                "birth_date": "",
                "birth_year": None,
                "birth_place_raw": "",
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
                "country_relevance_score": 0.0,
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
                "is_country_broad": 0,
                "flag_restricted_sample": 0,
                "flag_expanded_sample": 0,
                "sample_membership": "",
                "inclusion_reason": "",
                "exclusion_reason": "seed_not_resolved",
                "source_primary": "Wikipedia",
                "source_secondary": "",
                "source_seed": row["source_seed"],
                "evidence_urls": "",
                "source_count": 0,
                "birth_date_confidence": "low",
                "birth_place_confidence": "low",
                "genre_confidence": "low",
                "manual_review_needed": 1,
                "notes": "Wikipedia seed could not be resolved to a unique Wikidata entity",
            }
        )
    return pd.concat([dataset_df, pd.DataFrame(rows)], ignore_index=True, sort=False)


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    ranked = df.sort_values(
        by=["flag_restricted_sample", "flag_expanded_sample", "country_relevance_score", "source_count"],
        ascending=[False, False, False, False],
    ).copy()
    ranked["dedupe_key_name"] = (
        ranked["name_primary"].fillna("").str.lower().str.strip()
        + "|"
        + ranked["birth_date"].fillna("")
        + "|"
        + ranked["birth_place_raw"].fillna("").str.lower().str.strip()
    )
    with_qid = ranked[ranked["wikidata_qid"].fillna("").ne("")].drop_duplicates(subset=["wikidata_qid"], keep="first")
    without_qid = ranked[ranked["wikidata_qid"].fillna("").eq("")]
    ranked = pd.concat([with_qid, without_qid], ignore_index=True)

    with_mbid = ranked[ranked["musicbrainz_mbid"].fillna("").ne("")].drop_duplicates(subset=["musicbrainz_mbid"], keep="first")
    without_mbid = ranked[ranked["musicbrainz_mbid"].fillna("").eq("")]
    ranked = pd.concat([with_mbid, without_mbid], ignore_index=True)

    ranked = ranked.drop_duplicates(subset=["dedupe_key_name"], keep="first")
    ranked = ranked.drop(columns=["dedupe_key_name"])
    ranked = ranked.reset_index(drop=True)
    ranked["artist_id"] = [f"artist_{index:06d}" for index in range(1, len(ranked) + 1)]
    return ranked


def build_sources_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in df.iterrows():
        for source_name, source_role in [
            (row.get("source_primary", ""), "primary"),
            (row.get("source_secondary", ""), "secondary"),
            (row.get("source_seed", ""), "seed"),
        ]:
            if source_name:
                rows.append(
                    {
                        "artist_id": row["artist_id"],
                        "name_primary": row["name_primary"],
                        "source_role": source_role,
                        "source_name": source_name,
                        "evidence_urls": row.get("evidence_urls", ""),
                    }
                )
    return pd.DataFrame(rows).drop_duplicates()


def save_csv_and_dta(df: pd.DataFrame, basename: str) -> None:
    csv_path = OUTPUT_DIR / f"{basename}.csv"
    dta_path = OUTPUT_DIR / f"{basename}.dta"
    df.to_csv(csv_path, index=False, encoding="utf-8")
    stata_df = df.copy()
    for col in stata_df.columns:
        if (
            pd.api.types.is_object_dtype(stata_df[col])
            or pd.api.types.is_string_dtype(stata_df[col])
            or stata_df[col].isna().all()
        ):
            stata_df[col] = stata_df[col].astype("string").fillna("")
    stata_df.to_stata(dta_path, write_index=False, version=118)
    log(f"Saved {csv_path.name} and {dta_path.name}")


def write_qc_report(
    *,
    seed_df: pd.DataFrame,
    master_df: pd.DataFrame,
    restricted_df: pd.DataFrame,
    expanded_df: pd.DataFrame,
    excluded_df: pd.DataFrame,
    review_df: pd.DataFrame,
) -> None:
    exclusion_counts = Counter(excluded_df["exclusion_reason"].fillna("").tolist())
    note_counts = Counter(piece for note in master_df["notes"].fillna("") for piece in str(note).split("|") if piece)

    def pct(num: int, den: int) -> str:
        return "0.0%" if den == 0 else f"{(100 * num / den):.1f}%"

    content = f"""# country_artists_qc_report

## Summary

- numero totale candidati raccolti: {len(seed_df)}
- numero totale artisti nel master: {len(master_df)}
- numero nel campione ristretto: {len(restricted_df)}
- numero nel campione allargato: {len(expanded_df)}
- numero esclusi/non-US: {len(excluded_df)}
- numero casi in manual review: {len(review_df)}
- quota di record con birth_state valorizzato: {pct(int(master_df['birth_state'].fillna('').ne('').sum()), len(master_df))}
- quota di record con birth_date valorizzata: {pct(int(master_df['birth_date'].fillna('').ne('').sum()), len(master_df))}
- quota di record con Wikidata QID: {pct(int(master_df['wikidata_qid'].fillna('').ne('').sum()), len(master_df))}
- quota di record con MBID: {pct(int(master_df['musicbrainz_mbid'].fillna('').ne('').sum()), len(master_df))}

## Principali motivi di esclusione

{os.linesep.join(f"- {reason or 'missing_reason'}: {count}" for reason, count in exclusion_counts.most_common(10))}

## Principali conflitti o note

{os.linesep.join(f"- {reason}: {count}" for reason, count in note_counts.most_common(10))}

## Pipeline

1. raccolta seed da Country Music Hall of Fame e pagine/categorie Wikipedia
2. espansione candidati con query Wikidata sui generi country-correlati
3. enrichment strutturato con Wikidata
4. verifica secondaria limitata con MusicBrainz
5. normalizzazione, deduplica, campionamento e export CSV/DTA
"""
    (OUTPUT_DIR / "country_artists_qc_report.md").write_text(content, encoding="utf-8")


def main() -> None:
    ensure_dirs()
    if FALLBACK_MARKER_PATH.exists():
        FALLBACK_MARKER_PATH.unlink()
    s = session()
    seed_path = INTERMEDIATE_DIR / "country_artists_seed_candidates.csv"
    detail_path = INTERMEDIATE_DIR / "country_artists_wikidata_details.csv"

    if seed_path.exists():
        seed_df = pd.read_csv(seed_path)
        log(f"Loaded cached seed candidates from {seed_path.name}")
    else:
        log("Collecting seed candidates")
        try:
            seed_df = collect_seed_candidates(s).drop_duplicates()
            seed_df.to_csv(seed_path, index=False)
            log(f"Saved base seed checkpoint with {len(seed_df)} rows")

            broad_df = broad_country_candidates(s)
            if not broad_df.empty:
                seed_df = pd.concat([seed_df, broad_df], ignore_index=True).drop_duplicates()
                seed_df.to_csv(seed_path, index=False)
                log(f"Updated seed checkpoint with broad expansion to {len(seed_df)} rows")
            else:
                log("Proceeding without broad Wikidata expansion")
        except Exception as exc:
            log(f"Live seed collection failed: {exc}")
            restored = restore_bundled_intermediate_files(
                [
                    "country_artists_seed_candidates.csv",
                    "country_artists_wikidata_details.csv",
                    "country_artists_wikipedia_enrichment.csv",
                    "country_artists_wikipedia_lead.csv",
                    "country_artists_musicbrainz_bio.csv",
                ],
                overwrite=True,
            )
            if restored and seed_path.exists():
                seed_df = pd.read_csv(seed_path)
                log(f"Loaded bundled seed checkpoint from {seed_path.name}")
            else:
                raise

    qids = sorted(set(seed_df["wikidata_qid"].dropna().tolist()))
    if not qids:
        restored = restore_bundled_intermediate_files(
            [
                "country_artists_seed_candidates.csv",
                "country_artists_wikidata_details.csv",
                "country_artists_wikipedia_enrichment.csv",
                "country_artists_wikipedia_lead.csv",
                "country_artists_musicbrainz_bio.csv",
            ],
            overwrite=True,
        )
        if restored and seed_path.exists():
            seed_df = pd.read_csv(seed_path)
            qids = sorted(set(seed_df["wikidata_qid"].dropna().tolist()))
        if not qids:
            raise RuntimeError("No Wikidata QIDs collected from seeds or broad query.")

    log(f"Collected {len(seed_df)} seed rows and {len(qids)} unique QIDs")
    existing_detail_df = pd.read_csv(detail_path) if detail_path.exists() else pd.DataFrame()
    done_qids = set(existing_detail_df["wikidata_qid"].tolist()) if not existing_detail_df.empty else set()
    remaining_qids = [qid for qid in qids if qid not in done_qids]
    if remaining_qids:
        log(f"Fetching Wikidata details for {len(remaining_qids)} remaining QIDs")
        try:
            new_detail_df = wikidata_details(s, remaining_qids, checkpoint_path=detail_path)
            detail_df = pd.concat([existing_detail_df, new_detail_df], ignore_index=True)
            detail_df = detail_df.drop_duplicates(subset=["wikidata_qid"], keep="last")
            detail_df.to_csv(detail_path, index=False)
        except Exception as exc:
            log(f"Live Wikidata detail enrichment failed: {exc}")
            restored = restore_bundled_intermediate_files(
                [
                    "country_artists_wikidata_details.csv",
                    "country_artists_wikipedia_enrichment.csv",
                    "country_artists_wikipedia_lead.csv",
                    "country_artists_musicbrainz_bio.csv",
                ],
                overwrite=True,
            )
            if restored and detail_path.exists():
                detail_df = pd.read_csv(detail_path)
                log(f"Loaded bundled detail checkpoint from {detail_path.name}")
            else:
                raise
    else:
        detail_df = existing_detail_df
        log("Loaded cached Wikidata details with no remaining QIDs to fetch")

    log("Preparing normalized dataset")
    dataset_df = prepare_dataset(seed_df, detail_df, s)
    log("Skipping MusicBrainz for this build")
    dataset_df = maybe_verify_musicbrainz(s, dataset_df, max_lookups=0)
    log("Appending unresolved seeds and deduplicating")
    dataset_df = add_unresolved_seeds(seed_df, dataset_df)
    dataset_df = deduplicate(dataset_df)

    schema_df = pd.read_csv(SCHEMA_PATH)
    ordered_columns = schema_df["column_name"].tolist()
    dataset_df = dataset_df.reindex(columns=ordered_columns)

    master_df = dataset_df[dataset_df["flag_expanded_sample"] == 1].copy()
    excluded_df = dataset_df[dataset_df["flag_expanded_sample"] == 0].copy()
    restricted_df = master_df[master_df["flag_restricted_sample"] == 1].copy()
    expanded_df = master_df.copy()
    review_df = dataset_df[dataset_df["manual_review_needed"] == 1].copy()
    sources_df = build_sources_table(dataset_df)

    log("Saving final CSV/DTA outputs")
    save_csv_and_dta(master_df, "country_artists_master")
    save_csv_and_dta(restricted_df, "country_artists_restricted")
    save_csv_and_dta(expanded_df, "country_artists_expanded")
    save_csv_and_dta(excluded_df, "country_artists_excluded_or_non_us")
    save_csv_and_dta(sources_df, "country_artists_sources")
    save_csv_and_dta(review_df, "manual_review_queue")
    schema_df.to_csv(OUTPUT_DIR / "country_artists_data_dictionary.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    schema_df.to_stata(OUTPUT_DIR / "country_artists_data_dictionary.dta", write_index=False, version=118)

    write_qc_report(
        seed_df=seed_df,
        master_df=master_df,
        restricted_df=restricted_df,
        expanded_df=expanded_df,
        excluded_df=excluded_df,
        review_df=review_df,
    )

    log("Pipeline completed")


if __name__ == "__main__":
    main()
