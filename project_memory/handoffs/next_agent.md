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

## Recommended next action

Continue with the next migration/testing pass, choosing between:

- expanding bridge coverage to more remaining Phase 1 legacy modules
- validating another higher-value end-to-end workflow, such as the artist
  universe replication path

Keep replication-package / archival paths untouched unless explicitly
requested, except when a validated replication run intentionally refreshes its
bundled snapshot.
