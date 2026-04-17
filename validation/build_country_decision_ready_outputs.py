#!/usr/bin/env python3
"""Build decision-ready outputs from the narrowed Wikipedia validation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path("/Users/tommaso.colussi/Dropbox/Sound_of_Culture")
RESULTS_DIR = ROOT / "validation" / "results"
DATASET = ROOT / "data/processed_datasets/country_artists/artist_universe_country_only.dta"


DECISION_MAP = {
    "likely_country": "keep_country",
    "likely_non_country": "drop_non_country",
    "review": "manual_review",
}


def main() -> int:
    validated = pd.read_csv(RESULTS_DIR / "narrowed_country_candidates_wiki_validation.csv")
    full_df = pd.read_stata(DATASET, convert_categoricals=False)

    narrowed = validated.copy()
    narrowed["final_validation_decision"] = narrowed["wiki_validation_label"].map(DECISION_MAP)
    narrowed["final_validation_source"] = "reference_lists_plus_flags_plus_wikipedia"

    narrowed = narrowed.sort_values(
        ["final_validation_decision", "country_relevance_score", "name_primary"],
        ascending=[True, True, True],
    )
    narrowed.to_csv(RESULTS_DIR / "narrowed_country_candidates_decision_ready.csv", index=False)

    full_flags = full_df.copy()
    merge_cols = [
        "artist_id",
        "wiki_validation_label",
        "wiki_validation_evidence",
        "wiki_fetch_error",
        "wiki_lead_excerpt",
        "wiki_category_excerpt",
        "wikipedia_url",
        "final_validation_decision",
        "final_validation_source",
    ]
    full_flags = full_flags.merge(narrowed[merge_cols], on="artist_id", how="left", suffixes=("", "_validated"))
    full_flags["final_validation_decision"] = full_flags["final_validation_decision"].fillna("not_reviewed")
    full_flags["final_validation_source"] = full_flags["final_validation_source"].fillna("")
    full_flags.to_csv(RESULTS_DIR / "artist_universe_country_only_with_validation_flags.csv", index=False)

    summary = (
        narrowed["final_validation_decision"]
        .value_counts()
        .rename_axis("final_validation_decision")
        .reset_index(name="count")
    )
    summary.to_csv(RESULTS_DIR / "decision_ready_summary.csv", index=False)

    print(summary.to_string(index=False))
    print(f"\nFull dataset with validation flags written to: {RESULTS_DIR / 'artist_universe_country_only_with_validation_flags.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
