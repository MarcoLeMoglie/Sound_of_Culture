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

## Existing code base to check first

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
