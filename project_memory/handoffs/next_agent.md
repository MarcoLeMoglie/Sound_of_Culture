# Next Agent Handoff

## Where to start

1. read `AGENTS.md`
2. read `directives/README.md`
3. read `project_memory/status/current_status.md`
4. read `workspace_maps/restructure_block2_map_2026-04-18.md`

## Current situation

- archival branch exists
- phase-based skeleton exists
- directives and SOPs were rewritten around the new project goals
- `supermemory` is configured and operational in-session for local Codex
- a save/recall test has been validated inside the `sound-of-culture` project
  scope
- repo MCP files are now secret-free; local auth relies on
  `SUPERMEMORY_API_KEY`
- `caveman-compression` is installed locally for Codex under
  `~/.codex/vendor/caveman-compression`
- `code-review-graph` has been standardized across shared clients and rebuilt
- local Antigravity config was manually corrected to point at this repo
- graph coverage is currently limited to `python` and `bash`
- repo root now has a full onboarding `README.md`
- block 6 created canonical Overleaf report templates for the missing phase
  reports and standardized output subfolders
- the restructured branch is now the GitHub default branch
- a repo banner image exists at `assets/github-social-preview.png`
- block 7 documented why destructive legacy cleanup is still premature
- block 8 created phase-based wrappers for the main Phase 1 workflows
- block 8 created phase-based Stata wrapper entrypoints for exploratory
  analysis
- active directives now point first to phase-based execution entrypoints
- block 9 added the remaining high-value Phase 1 wrapper entrypoints for
  `final_merge_restricted_v2.py` and `validate_country_primary_artists.py`
- block 9 added phase-based Stata wrapper entrypoints for Phase 1 replication
  launchers
- active replication READMEs and Antigravity historical notes now point to the
  phase-based project-root entrypoints first
- key replication orchestrators now prefer phase-based paths when they launch
  project-root scripts
- block 10 turned key Phase 1 wrappers into importable bridge modules rather
  than script-only launchers
- block 10 added bridge modules for `music_indices`, `scraper_client`, and
  `augment_country_only_universe_from_billboard`
- block 10 rerooted a first active set of internal imports from
  `execution.step*` to `execution.phase_01_dataset_construction`
- `AGENTS.md` and `README.md` now explicitly say that every new modification
  must remember the coexistence rule until the migration is fully complete
- block 11 added a phase-based wrapper for
  `build_billboard_country_supplemental_targets.py`
- block 11 runtime-validated
  `execution/phase_01_dataset_construction/build_billboard_country_supplemental_targets.py`
  successfully using cached local inputs
- block 11 runtime-validated
  `execution/phase_01_dataset_construction/run_country_songs_replication.py`
  successfully; the run refreshed the bundled replication snapshot
- block 12 fixed the broken GitHub links in the main `README.md` by replacing
  local absolute links with repo-relative links
- block 12 runtime-validated
  `execution/phase_01_dataset_construction/run_artist_universe_replication.py`
  successfully; the run refreshed the bundled artist-universe replication
  snapshot
- block 13 added bridge modules for:
  `music_indices_bass.py`,
  `complete_country_only_release_years_parallel.py`,
  `complete_country_only_release_years_sequential.py`,
  `create_country_expansion_dataset.py`,
  `targeted_residual_artist_metadata.py`,
  `build_billboard_augmented_targets_from_cache.py`
- block 13 rerooted a set of `step1_download` scripts away from local
  `scraper_client` imports toward
  `execution.phase_01_dataset_construction.scraper_client`
- block 13 rerooted the former legacy `create_dataset.py` workflow to the
  phase-based `music_indices` and `music_indices_bass` bridges
- block 13 runtime-validated
  `execution/phase_01_dataset_construction/build_billboard_augmented_targets_from_cache.py`
  successfully; it reported `1515` augmented missing song targets with `1490`
  existing-artist matches and `25` missing-artist cases
- block 14 added explicit phase-based bridge wrappers for the remaining active
  `step1_download` and `step2_digitalize` Python modules
- inventory check after block 14 showed no remaining unbridged Python files in
  those two legacy subtrees
- block 14 runtime-validated
  `execution/phase_01_dataset_construction/prepare_bulk_input.py`
  successfully; it recreated `data/intermediate/json/input_songs_bulk.json`
  with `994` queries
- block 15 added the last missing active replication-side Phase 1 wrapper:
  `execution/phase_01_dataset_construction/restore_top100_jsons_by_id.py`
- inventory after block 15 showed no remaining missing active Python bridges in
  `step4_country_artists` or `step5_replication`, and no missing exploratory
  `.do` wrappers relative to `step3_analysis/do`
- after block 15, the operational interpretation is now:
  use `execution/phase_01_dataset_construction/` and
  `execution/phase_02_exploratory_analysis/` as the canonical execution
  surface; that statement described the pre-cutover architecture and is now
  superseded by the destructive cutover recorded below
- canonical Overleaf reports for all project phases must be written in English
- Phase 1 reporting has now been split into two canonical Overleaf reports:
  - `phase_01_dataset_construction/01_country_only_and_adjacent_only/main.tex`
  - `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/main.tex`
- the former root-level and Phase 1.2 `pipeline_report.tex` /
  `pipeline_report.pdf` files have been removed from Overleaf after the split
- for Phase 1 reports, Python should be cited via script paths and line
  references rather than copied as snippets; Stata should appear only via
  narrowly targeted snippets when a real retained Stata workflow exists
- a destructive-cutover plan now exists in
  `workspace_maps/destructive_cutover_plan_2026-04-18.md`
- destructive cutover point 5 has now been executed on the active branch after
  the validated pre-cutover snapshot branch was created and pushed

## Recommended next action

Do not start new migration work immediately. First, review the completed
destructive cutover:

- points 1-5 of the recommended cutover order are now complete
- the active branch no longer contains `execution/step*` trees

Important validation outcomes already obtained:

- `execution/phase_01_dataset_construction/build_country_artists_dataset.py`
  completed successfully after adding defensive defaults for missing
  `birth_year`-family columns
- `execution/phase_01_dataset_construction/build_extended_artist_universe.py`
  completed successfully after making targeted live lookup degrade cleanly when
  DNS/network resolution is unavailable
- `execution/phase_01_dataset_construction/build_country_only_chords_final.py`
  completed successfully
- `execution/phase_01_dataset_construction/run_country_merge_v6_replication.py`
  completed successfully from native phase-based packaged paths
- `execution/phase_01_dataset_construction/run_country_songs_replication.py`
  completed successfully
- `execution/phase_01_dataset_construction/run_artist_universe_replication.py`
  completed successfully
- `execution/phase_02_exploratory_analysis/do/eda.do` now reads
  `data/processed_datasets/top100YearEnd8525/dataset_chords_top100yearend.dta`
  and produces PDFs in
  `execution/phase_02_exploratory_analysis/output_figures/`
- `execution/phase_02_exploratory_analysis/do/eda_bass.do` now reads
  `data/processed_datasets/top100YearEnd8525/dataset_bass_top100yearend.dta`
  and produces PDFs in
  `execution/phase_02_exploratory_analysis/output_figures_bass/`
- the `r(199)` lines in `eda.log` and `eda_bass.log` are harmless
  environment noise from the local Stata `profile.do`, not failures in the
  project scripts

Most immediate scientific-reporting follow-up:

- update future Phase 1 report revisions directly in the two canonical
  `main.tex` files under the shared Overleaf Phase 1 folders, not in any
  resurrected `pipeline_report.*` file

Keep replication-package / archival paths untouched unless explicitly
requested, except when a validated replication run intentionally refreshes its
bundled snapshot.

## New Phase 1 metadata block

A new retained metadata-repair block has now been executed on the final
country-only song dataset.

Completed and retained:

- `execution/phase_01_dataset_construction/backfill_country_only_final_artist_metadata.py`
  now resets suspicious artist matches, applies stricter lookup rules, and
  uses a short set of web-confirmed artist/group overrides
- `execution/phase_01_dataset_construction/backfill_country_only_final_song_metadata.py`
  now preserves `genre_ug_original`, tracks provenance with
  `genre_source` / `genre_official_raw` / `genre_is_official` /
  `bpm_source`, and supports targeted gap-only runs
- the final dataset and `.dta` were updated
- both canonical Phase 1 reports were updated in the repo mirror, synchronized
  to Overleaf, and compiled successfully

Current final-dataset counts:

- rows: `44,058`
- `bpm` missing: `21,515`
- `genre` missing: `109`
- official-genre rows: `20,752`
- `birth_state` missing rows: `132`
- `us_macro_region` missing or `Unknown` rows: `121`
- rows explicitly coded as `Non-US` in `us_macro_region`: `2,768`
- `birth_country` missing rows: `121`

Important residual interpretation:

- non-US artists are now explicitly coded as `Non-US` rather than `Unknown`
- no US-origin artist currently lacks a state assignment
- BPM remains the largest unresolved song-level gap
- many songs still retain `genre_source = ug_selected`; the attempted
  song-by-song officialization sweep was not retained as a production run
  because iTunes exact queries returned `403` and Discogs exact queries quickly
  hit `429` rate limits
- on 2026-04-20, the BPM-only chunked mode was extended into an automatic
  loop-until-stall run; together with the earlier retained chunks, it reduced
  BPM missingness from `21,775` to `21,515`

Recommended next scientific action:

- continue Phase 2 exploratory work on the improved final dataset
- if Phase 1 cleanup continues, prioritize:
  - investigating additional BPM sources for the large unresolved residual
    block rather than rerunning the same currently stalled loop
  - a chunked official-genre upgrade workflow for the large residual
    `ug_selected` block
