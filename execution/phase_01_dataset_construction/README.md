# Execution Map: Phase 1

The canonical project phase is `phase_01_dataset_construction`.

Public phase-based entrypoints now live here, even though the underlying
implementation still runs from legacy `execution/step*` directories.

## Canonical entrypoints

- `execution/phase_01_dataset_construction/build_country_artists_dataset.py`
- `execution/phase_01_dataset_construction/build_extended_artist_universe.py`
- `execution/phase_01_dataset_construction/augment_country_only_universe_from_billboard.py`
- `execution/phase_01_dataset_construction/build_billboard_country_supplemental_targets.py`
- `execution/phase_01_dataset_construction/enrich_artist_universe_missing_metadata.py`
- `execution/phase_01_dataset_construction/country_only_ultimate_guitar.py`
- `execution/phase_01_dataset_construction/scraper_client.py`
- `execution/phase_01_dataset_construction/supplement_billboard_country_chords.py`
- `execution/phase_01_dataset_construction/build_country_only_chords_final.py`
- `execution/phase_01_dataset_construction/music_indices.py`
- `execution/phase_01_dataset_construction/backfill_restricted_final_v6_demographics.py`
- `execution/phase_01_dataset_construction/enrich_release_years_restricted_final_v6.py`
- `execution/phase_01_dataset_construction/final_merge_restricted_v2.py`
- `execution/phase_01_dataset_construction/validate_country_primary_artists.py`
- `execution/phase_01_dataset_construction/run_artist_universe_replication.py`
- `execution/phase_01_dataset_construction/run_country_merge_v6_replication.py`
- `execution/phase_01_dataset_construction/run_country_songs_replication.py`
- `execution/phase_01_dataset_construction/run_full_replication.py`
- `execution/phase_01_dataset_construction/do/run_country_artists_replication.do`
- `execution/phase_01_dataset_construction/do/run_artist_universe_replication.do`
- `execution/phase_01_dataset_construction/do/run_country_merge_v6_replication.do`
- `execution/phase_01_dataset_construction/do/run_country_songs_replication.do`

## Current legacy implementation roots

- `execution/step1_download/`
- `execution/step2_digitalize/`
- `execution/step4_country_artists/`
- `execution/step5_replication/`

This folder is the stable landing point for future code migration once path
rewrites are completed.
