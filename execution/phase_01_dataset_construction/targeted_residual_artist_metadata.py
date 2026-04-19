#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution.phase_01_dataset_construction import build_country_artists_dataset as builder
from execution.phase_01_dataset_construction import enrich_artist_universe_missing_metadata as meta


def main() -> None:
    path = meta.OUTPUT_DIR / "artist_universe_country_only.csv"
    df = meta.collapse_merge_columns(pd.read_csv(path, low_memory=False))
    meta.report_missingness(df, "country_only targeted residuals before")

    df = meta.apply_place_parsing(df)
    df = meta.clean_birth_geography(df)
    s = builder.session()
    wiki_mask = (
        df["wikipedia_url"].fillna("").astype(str).str.strip().ne("")
        & (
            pd.to_numeric(df["birth_year"], errors="coerce").isna()
            | df["birth_state"].fillna("").astype(str).str.strip().eq("")
            | df["birth_city"].fillna("").astype(str).str.strip().eq("")
            | df["birth_country"].fillna("").astype(str).str.strip().eq("")
        )
    )
    if wiki_mask.any():
        wiki_subset = df.loc[wiki_mask].copy()
        wiki_subset = wiki_subset.drop(
            columns=[
                "wp_page_title",
                "wp_birth_date",
                "wp_birth_year",
                "wp_birth_name",
                "wp_stage_name",
                "wp_birth_place_raw",
                "wp_aliases",
                "wp_genres_raw",
                "wp_categories",
            ],
            errors="ignore",
        )
        wiki_subset = builder.enrich_from_wikipedia_lead(
            s,
            wiki_subset,
            meta.INTERMEDIATE_DIR / "artist_universe_country_only_wikipedia_lead_residual.csv",
        )
        wiki_subset = builder.enrich_from_wikipedia(
            s,
            wiki_subset,
            meta.INTERMEDIATE_DIR / "artist_universe_country_only_wikipedia_enrichment_residual.csv",
        )
        remainder = df.loc[~wiki_mask].copy()
        df = pd.concat([remainder, wiki_subset], ignore_index=True, sort=False)
    df = meta.resolve_missing_musicbrainz_mbids(df, "artist_universe_country_only_name_to_musicbrainz.csv")
    df = builder.targeted_musicbrainz_bio_enrichment(
        s,
        df,
        meta.INTERMEDIATE_DIR / "artist_universe_country_only_targeted_musicbrainz_bio.csv",
    )
    df = meta.enrich_from_musicbrainz_generic(df, "artist_universe_country_only_generic_musicbrainz.csv")
    df = meta.enrich_from_wikipedia_origin_years(df, "artist_universe_country_only_wikipedia_origin_years.csv")
    df = meta.enrich_from_official_websites(df, "artist_universe_country_only_official_websites.csv")
    df = meta.apply_web_confirmed_overrides(df)
    df = meta.apply_group_and_occupation_annotations(df)
    for idx, row in df.iterrows():
        fallback = builder.apply_category_location_fallback(row)
        for key in ["birth_city", "birth_county", "birth_state", "birth_country"]:
            current = meta.nonempty(row.get(key, ""))
            candidate = meta.nonempty(fallback.get(key, ""))
            if not current and candidate:
                df.at[idx, key] = candidate
    df = meta.apply_place_parsing(df)
    df = meta.clean_birth_geography(df)
    df = meta.infer_state_from_existing_city_country(df)
    df = meta.refresh_country_only_derived_columns(df)
    df = df.drop(columns=[col for col in df.columns if col.startswith("wp_") or col.startswith("wpl_")], errors="ignore")
    meta.report_missingness(df, "country_only targeted residuals after")
    meta.save_country_only_dataset(df)


if __name__ == "__main__":
    main()
