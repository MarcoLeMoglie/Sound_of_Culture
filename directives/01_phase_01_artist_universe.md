# Phase 1.1 Directive: Country-Only And Adjacent-Only Artist Universes

## Goal

Construct the artist-universe layer that defines who belongs in the country
sample, who belongs in the adjacent sample, and how those universes are
documented.

## Main tasks

- build or refresh the `country-only` universe
- build or refresh the `adjacent-only` universe
- define inclusion and exclusion logic
- document all metadata recovery steps for artist-level variables

## Canonical execution entrypoints to check first

- `execution/phase_01_dataset_construction/build_country_artists_dataset.py`
- `execution/phase_01_dataset_construction/build_extended_artist_universe.py`
- `execution/phase_01_dataset_construction/enrich_artist_universe_missing_metadata.py`
- `execution/phase_01_dataset_construction/run_artist_universe_replication.py`

## Legacy implementation roots behind those entrypoints

- `execution/step4_country_artists/`
- `execution/step5_replication/`

## Required outputs

- final artist-universe datasets in `csv` and `dta`
- QC and methodological notes
- a plain-English report in Overleaf:
  - `phase_01_dataset_construction/01_country_only_and_adjacent_only/`

## Mandatory report content

- source list and source hierarchy
- inclusion and exclusion logic
- deduplication logic
- missing-metadata recovery logic
- manual overrides, if any
- kept vs discarded attempts
- code snippets for the main builder and enrichment passes

## Safety rule

Do not silently redefine the country or adjacent perimeter. Any substantive
sample rule change must be documented in both the report and `project_memory/`.
