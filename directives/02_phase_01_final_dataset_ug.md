# Phase 1.2 Directive: Final Ultimate Guitar Dataset

## Goal

Build the final song-level dataset using Ultimate Guitar data and all required
recovery steps for missing or incomplete metadata.

## Main tasks

- use the `country-only` artist list as the main scraping input
- perform discovery and download on Ultimate Guitar
- supplement missing songs or artists where the existing workflow requires it
- build the final song-level dataset
- recover missing release years and attached metadata

## Canonical execution entrypoints to check first

- `execution/phase_01_dataset_construction/country_only_ultimate_guitar.py`
- `execution/phase_01_dataset_construction/supplement_billboard_country_chords.py`
- `execution/phase_01_dataset_construction/build_country_only_chords_final.py`
- `execution/phase_01_dataset_construction/backfill_restricted_final_v6_demographics.py`
- `execution/phase_01_dataset_construction/enrich_release_years_restricted_final_v6.py`
- `execution/phase_01_dataset_construction/run_country_merge_v6_replication.py`
- `execution/phase_01_dataset_construction/run_country_songs_replication.py`
- `execution/phase_01_dataset_construction/run_full_replication.py`

## Legacy implementation roots behind those entrypoints

- `execution/step1_download/`
- `execution/step2_digitalize/`
- `execution/step4_country_artists/`
- `execution/step5_replication/`

## Required outputs

- final dataset files in `csv` and `dta`
- intermediate recovery outputs when operationally useful
- a plain-English report in Overleaf:
  - `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/`

## Mandatory report content

- discovery logic
- scraping logic
- supplementation logic
- final merge logic
- release-year recovery logic
- all important code snippets
- all important generated artifacts
- kept vs discarded attempts and why

## Important distinction

Phase 1 requires two separate reports:

1. one report for `country-only` and `adjacent-only` universe construction
2. one report for the final Ultimate Guitar dataset and missing-data recovery
