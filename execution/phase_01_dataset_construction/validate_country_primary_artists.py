#!/usr/bin/env python3
"""Validate whether artists belong primarily to country music.

This script is designed for the country-artist universe where false positives
can enter because of one-off collaborations with country artists. It uses a
simple, inspectable scoring rule over text metadata from sources such as
Wikidata, MusicBrainz, Wikipedia, Discogs, or iTunes when those fields are
available in the input dataset.

The script is intentionally flexible about column names because the project has
several intermediate/final exports. It looks for likely metadata columns,
assigns positive evidence to country-centric genres, assigns negative evidence
to obviously non-country primary genres, and emits a validation label:

- core_country
- review
- likely_non_country

Typical use:
    python3 execution/phase_01_dataset_construction/build_country_artists_dataset.py
    python3 execution/phase_01_dataset_construction/validate_country_primary_artists.py \
      --input data/phase_01_dataset_construction/processed/country_artists/artist_universe_country_only.csv \
      --output data/phase_01_dataset_construction/processed/country_artists/artist_universe_country_only_validated.csv
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd


POSITIVE_TERMS = {
    "country": 4,
    "country music": 5,
    "country pop": 3,
    "country rock": 3,
    "outlaw country": 4,
    "classic country": 4,
    "traditional country": 4,
    "contemporary country": 4,
    "progressive country": 3,
    "alternative country": 4,
    "alt-country": 4,
    "americana": 2,
    "bluegrass": 3,
    "honky tonk": 4,
    "honky-tonk": 4,
    "western swing": 3,
    "bakersfield sound": 3,
    "nashville sound": 3,
    "neotraditional country": 4,
    "cowboy": 2,
}

NEGATIVE_TERMS = {
    "edm": -5,
    "electronic dance music": -5,
    "dance": -3,
    "house": -4,
    "techno": -5,
    "trance": -5,
    "dubstep": -5,
    "electro": -4,
    "dj": -2,
    "hip hop": -4,
    "hip-hop": -4,
    "rap": -4,
    "trap": -4,
    "r&b": -3,
    "rhythm and blues": -3,
    "k-pop": -4,
    "pop": -2,
    "dance-pop": -4,
    "synth-pop": -4,
    "electropop": -4,
    "new wave": -3,
}

COUNTRY_EXCLUSION_TERMS = {
    "country of origin",
    "country code",
    "country house",
}

RECOMMENDED_TEXT_COLUMNS = [
    "genre",
    "genres",
    "genre_list",
    "musicbrainz_genres",
    "musicbrainz_tags",
    "wikidata_genres",
    "discogs_genres",
    "discogs_styles",
    "itunes_primary_genre",
    "itunes_genre",
    "wikipedia_lead",
    "wikipedia_summary",
    "bio",
    "description",
    "artist_type",
]

ARTIST_NAME_CANDIDATES = [
    "artist_name",
    "artist",
    "name",
    "canonical_artist_name",
]

ARTIST_ID_CANDIDATES = [
    "artist_id",
    "artist_key",
    "qid",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input CSV or DTA file.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument(
        "--review-output",
        help="Optional CSV with only rows labeled for manual review.",
    )
    parser.add_argument(
        "--min-core-score",
        type=int,
        default=5,
        help="Minimum score to classify an artist as core_country.",
    )
    parser.add_argument(
        "--max-non-country-score",
        type=int,
        default=0,
        help="Maximum score to classify an artist as likely_non_country.",
    )
    return parser.parse_args()


def normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    text = str(value).strip().lower()
    text = re.sub(r"[_|;/]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def candidate_columns(df: pd.DataFrame) -> list[str]:
    lower_to_original = {c.lower(): c for c in df.columns}
    selected: list[str] = []

    for name in RECOMMENDED_TEXT_COLUMNS:
        if name in lower_to_original:
            selected.append(lower_to_original[name])

    for column in df.columns:
        lowered = column.lower()
        if column in selected:
            continue
        if any(token in lowered for token in ("genre", "style", "tag", "wiki", "bio", "desc")):
            selected.append(column)

    return selected


def find_first_column(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    lower_to_original = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate in lower_to_original:
            return lower_to_original[candidate]
    return None


def contains_term(text: str, term: str) -> bool:
    return re.search(rf"(?<![a-z]){re.escape(term)}(?![a-z])", text) is not None


def score_text(text: str) -> tuple[int, list[str]]:
    score = 0
    evidence: list[str] = []
    if not text:
        return score, evidence

    for exclusion in COUNTRY_EXCLUSION_TERMS:
        text = text.replace(exclusion, " ")

    for term, weight in POSITIVE_TERMS.items():
        if contains_term(text, term):
            score += weight
            evidence.append(f"+{weight}:{term}")

    for term, weight in NEGATIVE_TERMS.items():
        if contains_term(text, term):
            score += weight
            evidence.append(f"{weight}:{term}")

    return score, evidence


def load_dataframe(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"Input file is empty: {path}")

    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)

    if path.suffix.lower() == ".dta":
        return pd.read_stata(path, convert_categoricals=False)

    raise ValueError(f"Unsupported input format: {path.suffix}")


def classify(score: int, min_core_score: int, max_non_country_score: int) -> str:
    if score >= min_core_score:
        return "core_country"
    if score <= max_non_country_score:
        return "likely_non_country"
    return "review"


def numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")
    return pd.to_numeric(df[column], errors="coerce")


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    review_output_path = Path(args.review_output) if args.review_output else None

    try:
        df = load_dataframe(input_path)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    text_columns = candidate_columns(df)
    if not text_columns:
        print(
            "No usable text metadata columns found. "
            "Expected columns with names like genre, style, tag, wiki, bio, or desc.",
            file=sys.stderr,
        )
        return 1

    artist_name_col = find_first_column(df, ARTIST_NAME_CANDIDATES)
    artist_id_col = find_first_column(df, ARTIST_ID_CANDIDATES)

    result = df.copy()
    combined_text_series = (
        result[text_columns].fillna("").astype(str).agg(" | ".join, axis=1).map(normalize_text)
    )

    scores: list[int] = []
    evidence_list: list[str] = []
    positive_hits: list[int] = []
    negative_hits: list[int] = []

    for text in combined_text_series:
        score, evidence = score_text(text)
        scores.append(score)
        evidence_list.append(" ; ".join(evidence))
        positive_hits.append(sum(1 for e in evidence if e.startswith("+")))
        negative_hits.append(sum(1 for e in evidence if not e.startswith("+")))

    result["country_validation_score_text"] = scores
    result["country_validation_evidence"] = evidence_list
    result["country_positive_hits"] = positive_hits
    result["country_negative_hits"] = negative_hits

    existing_score = numeric_series(result, "country_relevance_score")
    is_country_core = numeric_series(result, "is_country_core")
    is_country_broad = numeric_series(result, "is_country_broad")
    manual_review_needed = numeric_series(result, "manual_review_needed")

    combined_score = existing_score.copy()
    missing_existing = combined_score.isna()
    combined_score.loc[missing_existing] = result.loc[missing_existing, "country_validation_score_text"]
    result["country_validation_score"] = combined_score

    labels: list[str] = []
    for idx, row in result.iterrows():
        score = row["country_validation_score"]
        core = is_country_core.loc[idx]
        broad = is_country_broad.loc[idx]
        review_flag = manual_review_needed.loc[idx]
        pos_hits = row["country_positive_hits"]
        neg_hits = row["country_negative_hits"]

        # Project-aware rules: artists outside broad country are strong exclusion
        # candidates, while broad-but-not-core artists should mostly enter review.
        if pd.notna(core) and pd.notna(broad):
            if broad == 0:
                label = "likely_non_country"
            elif core == 1 and pd.notna(score) and score >= 0.5:
                label = "core_country"
            elif pd.notna(score) and score >= 0.7 and broad == 1:
                label = "core_country"
            else:
                label = "review"
        else:
            label = classify(int(score) if pd.notna(score) else 0, args.min_core_score, args.max_non_country_score)

        if review_flag == 1:
            label = "review" if label == "core_country" else label
        if pos_hits > 0 and neg_hits > 0:
            label = "review"
        labels.append(label)

    result["country_validation_label"] = labels

    # Escalate to manual review when the evidence is mixed.
    mixed_mask = (result["country_positive_hits"] > 0) & (result["country_negative_hits"] > 0)
    result.loc[mixed_mask, "country_validation_label"] = "review"

    ordered_columns = []
    for col in (artist_id_col, artist_name_col):
        if col and col not in ordered_columns:
            ordered_columns.append(col)
    ordered_columns.extend(
        [
            "country_validation_label",
            "country_validation_score",
            "country_validation_score_text",
            "country_positive_hits",
            "country_negative_hits",
            "country_validation_evidence",
        ]
    )
    remaining_columns = [c for c in result.columns if c not in ordered_columns]
    result = result[ordered_columns + remaining_columns]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)

    if review_output_path is not None:
        review_output_path.parent.mkdir(parents=True, exist_ok=True)
        review_df = result[result["country_validation_label"] == "review"].copy()
        review_df.to_csv(review_output_path, index=False)

    counts = result["country_validation_label"].value_counts(dropna=False).to_dict()
    print(f"Loaded {len(result)} rows from {input_path}")
    print(f"Metadata columns used: {', '.join(text_columns)}")
    print(f"Label counts: {counts}")
    if artist_name_col:
        sample = result.loc[
            result["country_validation_label"] != "core_country",
            [artist_name_col, "country_validation_label", "country_validation_score", "country_validation_evidence"],
        ].head(20)
        if not sample.empty:
            print("\nSample non-core rows:")
            print(sample.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
