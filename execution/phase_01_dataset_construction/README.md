# Execution Map: Phase 1

The canonical project phase is `phase_01_dataset_construction`.

Public phase-based entrypoints now live here, even though the underlying
implementation still runs from legacy `execution/step*` directories.

## Canonical entrypoints

- `execution/phase_01_dataset_construction/build_country_artists_dataset.py`
- `execution/phase_01_dataset_construction/build_extended_artist_universe.py`
- `execution/phase_01_dataset_construction/augment_country_only_universe_from_billboard.py`
- `execution/phase_01_dataset_construction/build_billboard_country_supplemental_targets.py`
- `execution/phase_01_dataset_construction/build_billboard_augmented_targets_from_cache.py`
- `execution/phase_01_dataset_construction/enrich_artist_universe_missing_metadata.py`
- `execution/phase_01_dataset_construction/targeted_residual_artist_metadata.py`
- `execution/phase_01_dataset_construction/country_only_ultimate_guitar.py`
- `execution/phase_01_dataset_construction/scraper_client.py`
- `execution/phase_01_dataset_construction/supplement_billboard_country_chords.py`
- `execution/phase_01_dataset_construction/build_country_only_chords_final.py`
- `execution/phase_01_dataset_construction/music_indices.py`
- `execution/phase_01_dataset_construction/music_indices_bass.py`
- `execution/phase_01_dataset_construction/backfill_restricted_final_v6_demographics.py`
- `execution/phase_01_dataset_construction/enrich_release_years_restricted_final_v6.py`
- `execution/phase_01_dataset_construction/complete_country_only_release_years_parallel.py`
- `execution/phase_01_dataset_construction/complete_country_only_release_years_sequential.py`
- `execution/phase_01_dataset_construction/create_country_expansion_dataset.py`
- `execution/phase_01_dataset_construction/final_merge_restricted_v2.py`
- `execution/phase_01_dataset_construction/validate_country_primary_artists.py`
- `execution/phase_01_dataset_construction/run_artist_universe_replication.py`
- `execution/phase_01_dataset_construction/run_country_merge_v6_replication.py`
- `execution/phase_01_dataset_construction/run_country_songs_replication.py`
- `execution/phase_01_dataset_construction/run_full_replication.py`
- `execution/phase_01_dataset_construction/restore_top100_jsons_by_id.py`
- `execution/phase_01_dataset_construction/do/run_country_artists_replication.do`
- `execution/phase_01_dataset_construction/do/run_artist_universe_replication.do`
- `execution/phase_01_dataset_construction/do/run_country_merge_v6_replication.do`
- `execution/phase_01_dataset_construction/do/run_country_songs_replication.do`

Bridge coverage now also extends across the remaining `step1_download` and
`step2_digitalize` Python utilities, so the canonical Phase 1 folder now
exposes wrappers for the full active Python surface of those two legacy
subtrees.

As of block 15, bridge coverage also spans the active Python entrypoints in
`step4_country_artists` and `step5_replication`, so collaborators can now
treat `execution/phase_01_dataset_construction/` as the canonical operational
surface for Phase 1 work. The legacy folders remain in place as the
implementation backend and as historical packaging roots.

## Current legacy implementation roots

- `execution/step1_download/`
- `execution/step2_digitalize/`
- `execution/step4_country_artists/`
- `execution/step5_replication/`

This folder is the stable landing point for future code migration once path
rewrites are completed.
