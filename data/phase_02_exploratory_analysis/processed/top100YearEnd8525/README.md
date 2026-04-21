# top100YearEnd8525

This folder collects the final Year-End Top 100 deliverables in one place.

Included files:

- `dataset_top100yearend.csv` / `.dta`
- `dataset_chords_top100yearend.csv` / `.dta`
- `dataset_bass_top100yearend.csv` / `.dta`
- `songs_tracklist_top100yearend.json`
- `raw_json_chords/`
- `raw_json_bass/`

Notes:

- `dataset_top100yearend` is currently the historical alias of the chords dataset.
- `raw_json_chords` is the restored snapshot of chord JSONs recovered from project git history when available.
- `raw_json_bass` is the restored snapshot of bass JSONs recovered deterministically from the final dataset `id` values.
- The main orchestration logic for rebuilding these files is documented in:
  `data/processed_datasets/country_artists/replication_package_2026-03-27/code/stata/run_full_replication.do`
