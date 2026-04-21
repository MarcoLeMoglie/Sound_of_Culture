#!/usr/bin/env python3
"""Validate narrowed country-artist candidates using Wikipedia pages.

The narrowed set is defined as artists who:
- are not present in any high-confidence reference list, and
- either are not country-broad, or are broad but not core with low relevance.

For each candidate, the script fetches the artist's Wikipedia page, extracts the
lead paragraph and page categories, and classifies the artist as:
- likely_country
- review
- likely_non_country
"""

from __future__ import annotations

import json
import re
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


ROOT = Path("/Users/tommaso.colussi/Dropbox/Sound_of_Culture")
DATASET = ROOT / "data/phase_01_dataset_construction/processed/country_artists/artist_universe_country_only.dta"
FLAGS = ROOT / "data/phase_01_dataset_construction/validation_reference_filter/results/artist_reference_flags.csv"
VALIDATION_DIR = ROOT / "validation"
RESULTS_DIR = VALIDATION_DIR / "results"
CACHE_DIR = VALIDATION_DIR / "cache/wiki_candidates"
HEADERS = {"User-Agent": "Mozilla/5.0"}
WIKI_SEARCH_API = "https://en.wikipedia.org/w/api.php"


STRONG_COUNTRY_TERMS = [
    "country music singer",
    "country singer",
    "country singer-songwriter",
    "country musician",
    "country music band",
    "country band",
    "country duo",
    "country group",
    "country artist",
    "country rapper",
    "country pop singer",
    "country rock",
    "country music duo",
    "country music group",
]

COUNTRY_ADJACENT_TERMS = [
    "americana",
    "bluegrass",
    "alt-country",
    "alternative country",
    "honky-tonk",
    "honky tonk",
    "western swing",
    "outlaw country",
    "country pop",
]

STRONG_NON_COUNTRY_TERMS = [
    "dj",
    "electronic",
    "edm",
    "house music",
    "techno",
    "trap music",
    "hip hop",
    "hip-hop",
    "rapper",
    "pop singer",
    "dance-pop",
    "synth-pop",
    "actor",
    "actress",
    "comedian",
    "contemporary worship",
    "christian worship",
    "opera singer",
]


def normalize(text: str) -> str:
    if not isinstance(text, str):
        return ""
    normalized = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def load_narrowed_candidates() -> pd.DataFrame:
    flags = pd.read_csv(FLAGS)
    df = pd.read_stata(DATASET, convert_categoricals=False)
    merged = flags.merge(
        df[["artist_id", "wikipedia_url", "genres_raw", "genres_normalized", "wikipedia_categories"]],
        on="artist_id",
        how="left",
    )
    narrowed = merged[
        (~merged["in_any_reference_list"])
        & (
            (merged["is_country_broad"].fillna(0) == 0)
            | (
                (merged["is_country_broad"].fillna(0) == 1)
                & (merged["is_country_core"].fillna(0) == 0)
                & (merged["country_relevance_score"].fillna(0) < 0.5)
            )
        )
    ].copy()
    return narrowed


def cache_name_from_url(url: str) -> str:
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", slug)
    return f"{slug}.html"


def fetch_html(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        raise ValueError("empty wikipedia_url")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / cache_name_from_url(url)
    if cache_path.exists():
        return cache_path.read_text()
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    html = response.text
    cache_path.write_text(html)
    time.sleep(0.2)
    return html


def fetch_json(url: str, params: dict[str, str], cache_name: str) -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / cache_name
    if cache_path.exists():
        return json.loads(cache_path.read_text())
    response = requests.get(url, params=params, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()
    cache_path.write_text(json.dumps(data))
    time.sleep(0.1)
    return data


def extract_wiki_features(url: str) -> tuple[str, str]:
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    content = soup.find("div", class_="mw-parser-output")
    paragraphs: list[str] = []
    if content is not None:
        for tag in content.find_all("p", recursive=False):
            text = tag.get_text(" ", strip=True)
            text = re.sub(r"\[[^\]]*\]", "", text).strip()
            if text:
                paragraphs.append(text)
            if len(paragraphs) >= 2:
                break

    if not paragraphs:
        paragraphs = [meta.get("content", "") for meta in soup.select('meta[property="og:description"]') if meta.get("content")]

    lead_text = " ".join(paragraphs[:2]).strip()

    categories = []
    for anchor in soup.select("#mw-normal-catlinks ul li a"):
        text = anchor.get_text(" ", strip=True)
        if text:
            categories.append(text)
    category_text = " | ".join(categories)
    return lead_text, category_text


def resolve_wikipedia_url(name_primary: str, current_url: str) -> str:
    if isinstance(current_url, str) and current_url.strip():
        return current_url

    query_variants = [
        f"{name_primary} singer",
        f"{name_primary} musician",
        f"{name_primary} band",
        name_primary,
    ]
    for idx, query in enumerate(query_variants):
        params = {
            "action": "opensearch",
            "search": query,
            "limit": "5",
            "namespace": "0",
            "format": "json",
        }
        cache_name = f"search_{re.sub(r'[^A-Za-z0-9._-]+', '_', query)}_{idx}.json"
        try:
            data = fetch_json(WIKI_SEARCH_API, params, cache_name)
        except Exception:
            continue
        if isinstance(data, list) and len(data) >= 4 and data[3]:
            for candidate_url in data[3]:
                if isinstance(candidate_url, str) and candidate_url.startswith("https://en.wikipedia.org/wiki/"):
                    return candidate_url
    return current_url


def maybe_search_better_wikipedia_url(name_primary: str, current_url: str, lead_text: str) -> str:
    normalized_lead = normalize(lead_text)
    if not current_url or " may refer to" in normalized_lead or normalized_lead.startswith(name_primary.lower() + " may refer to"):
        return resolve_wikipedia_url(name_primary, "")
    return current_url


def find_hits(text: str, terms: list[str]) -> list[str]:
    lowered = normalize(text)
    return [term for term in terms if term in lowered]


def classify_candidate(lead_text: str, category_text: str, genres_text: str) -> tuple[str, str]:
    combined = " || ".join([lead_text or "", category_text or "", genres_text or ""])
    country_hits = find_hits(combined, STRONG_COUNTRY_TERMS)
    adjacent_hits = find_hits(combined, COUNTRY_ADJACENT_TERMS)
    non_country_hits = find_hits(combined, STRONG_NON_COUNTRY_TERMS)

    if country_hits and not non_country_hits:
        return "likely_country", "; ".join(country_hits[:6])
    if country_hits and non_country_hits:
        return "review", "; ".join((country_hits + non_country_hits)[:8])
    if adjacent_hits and not non_country_hits:
        return "likely_country", "; ".join(adjacent_hits[:6])
    if adjacent_hits and non_country_hits:
        return "review", "; ".join((adjacent_hits + non_country_hits)[:8])
    if non_country_hits:
        return "likely_non_country", "; ".join(non_country_hits[:6])
    return "review", "no clear wiki signal"


def main() -> int:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    candidates = load_narrowed_candidates().sort_values(["country_relevance_score", "name_primary"]).copy()
    candidates.to_csv(RESULTS_DIR / "narrowed_country_candidates.csv", index=False)

    rows = []
    for row in candidates.itertuples(index=False):
        try:
            resolved_url = resolve_wikipedia_url(row.name_primary, row.wikipedia_url)
            lead_text, category_text = extract_wiki_features(resolved_url)
            better_url = maybe_search_better_wikipedia_url(row.name_primary, resolved_url, lead_text)
            if better_url != resolved_url:
                resolved_url = better_url
                lead_text, category_text = extract_wiki_features(resolved_url)
            fetch_error = ""
        except Exception as exc:
            resolved_url = row.wikipedia_url
            lead_text, category_text = "", ""
            fetch_error = str(exc)
        genres_text = " | ".join(
            [
                str(getattr(row, "genres_raw", "") or ""),
                str(getattr(row, "genres_normalized", "") or ""),
                str(getattr(row, "wikipedia_categories", "") or ""),
            ]
        )
        label, evidence = classify_candidate(lead_text, category_text, genres_text)
        rows.append(
            {
                "artist_id": row.artist_id,
                "name_primary": row.name_primary,
                "country_relevance_score": row.country_relevance_score,
                "is_country_core": row.is_country_core,
                "is_country_broad": row.is_country_broad,
                "source_seed": row.source_seed,
                "wikipedia_url": resolved_url,
                "wiki_validation_label": label,
                "wiki_validation_evidence": evidence,
                "wiki_fetch_error": fetch_error,
                "wiki_lead_excerpt": lead_text[:500],
                "wiki_category_excerpt": category_text[:500],
            }
        )

    result = pd.DataFrame(rows)
    result.to_csv(RESULTS_DIR / "narrowed_country_candidates_wiki_validation.csv", index=False)
    result[result["wiki_validation_label"] == "likely_non_country"].to_csv(
        RESULTS_DIR / "narrowed_country_candidates_likely_non_country.csv", index=False
    )
    result[result["wiki_validation_label"] == "review"].to_csv(
        RESULTS_DIR / "narrowed_country_candidates_review.csv", index=False
    )
    result[result["wiki_validation_label"] == "likely_country"].to_csv(
        RESULTS_DIR / "narrowed_country_candidates_likely_country.csv", index=False
    )

    summary = (
        result["wiki_validation_label"]
        .value_counts(dropna=False)
        .rename_axis("wiki_validation_label")
        .reset_index(name="count")
    )
    summary.to_csv(RESULTS_DIR / "narrowed_country_candidates_wiki_summary.csv", index=False)

    print(f"narrowed_candidates {len(result)}")
    print(summary.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
