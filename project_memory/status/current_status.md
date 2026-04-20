# Current Status

## Date

2026-04-18

## Active working branch

`codex/restructure-workspace-2026-04-18`

## Completed restructuring blocks

- block 1: archival snapshot created and pushed
- block 2: phase-based workspace skeleton created and pushed
- block 3: directives and SOP rewrite completed on the working branch
- block 4: Supermemory configured, authenticated, and validated in-session
- block 5: code-review-graph standardized across clients and rebuilt
- block 6: repository onboarding README and phase-based Overleaf/report
  templates standardized
- block 7: GitHub-facing cutover preparation completed safely
- block 8: initial legacy-reference migration completed for active project
  surfaces
- block 9: operational launchers and replication instructions shifted further
  toward phase-based entrypoints
- block 10: first internal-import migration moved active Python dependencies
  toward phase-based bridge modules
- block 11: runtime validation executed through the new phase-based bridge
  layer
- block 12: GitHub README links fixed and artist-universe replication
  validated through the phase-based surface
- block 13: additional Phase 1 helper modules bridged and legacy helper
  imports reduced further
- block 14: remaining step1/step2 Phase 1 wrappers completed and helper
  coverage extended across the full active Python surface
- block 15: remaining active Phase 1 replication-side wrapper coverage
  completed, closing the operational restructuring pass

## Current block

- destructive cutover point 5 was executed on 2026-04-19 after the validated
  pre-cutover snapshot branch was created and pushed
- the active branch now contains only the `execution/phase_*` execution trees

## Plugin memory status

- `supermemory` MCP configured in repo MCP files
- `supermemory` MCP configured in local Codex config
- local Codex now uses bearer-token authentication via
  `SUPERMEMORY_API_KEY`
- tracked repo MCP files are secret-free; authentication now lives only in the
  local Codex config and local environment
- live in-session MCP access is working
- `whoAmI`, `memory(save)`, and `recall` were successfully tested in the
  `sound-of-culture` project scope
- repo-side memory remains mandatory as a redundancy layer and for clients
  that may not yet be authenticated

## Local tooling status

- `caveman-compression` installed locally at
  `~/.codex/vendor/caveman-compression`
- dedicated virtual environment created
- NLP mode dependencies installed
- spaCy model download failed twice with upstream GitHub `504` errors
- local install patched with a blank-pipeline fallback so NLP compression is
  still usable offline

## Review graph status

- `code-review-graph` confirmed in Codex, Cursor, OpenCode, and Kiro
- local Antigravity config manually realigned to this workspace
- graph rebuilt on 2026-04-18
- current indexed languages: `python`, `bash`
- important non-indexed project surfaces: Stata, LaTeX, data artifacts
- tracked archival snapshot folders still create graph-search noise, so path
  filtering remains important

## Next recommended step

Return to scientific work and report-writing on top of the now-clean
phase-based architecture.

## Phase 1 metadata-repair status

On 2026-04-19, a new retained metadata-repair block was run on the final
country-only song dataset:

- `execution/phase_01_dataset_construction/backfill_country_only_final_artist_metadata.py`
- `execution/phase_01_dataset_construction/backfill_country_only_final_song_metadata.py`

Current final-dataset status after the retained completed passes:

- rows: `44,058`
- `bpm` missing: `21,775`
- `genre` missing: `109`
- rows with official genre source (`genre_is_official = 1`): `20,261`
- `birth_state` missing rows: `158`
- `us_macro_region` missing or `Unknown` rows: `2,360`
- `birth_country` missing rows: `121`

Current artist-level residuals inside the final song dataset:

- unique artists missing `birth_state`: `42`
- unique artists missing or `Unknown` in `us_macro_region`: `125`
- unique artists missing `birth_country`: `35`
- US-origin artists still missing `birth_state`: `Jim Hurst`, `The Pinetoppers`,
  `The Wreckers`

Interpretation:

- BPM recovery improved substantially but remains incomplete because the
  scalable public sources do not cover the whole catalogue.
- Genre missingness is now much smaller, but many rows still retain
  `genre_source = ug_selected` because a universal official replacement could
  not yet be obtained from the currently responding public endpoints.
- Artist-origin metadata improved materially, and most residual
  `us_macro_region = Unknown` cases are now non-US artists rather than US
  artists with a missing state.

Retained reporting update completed:

- `reports/phase_01_dataset_construction/01_country_only_and_adjacent_only/main.tex`
  updated with a downstream artist-origin repair addendum
- `reports/phase_01_dataset_construction/02_final_dataset_ultimate_guitar/main.tex`
  updated with a new metadata-repair section covering BPM, genre, and
  artist-origin backfills
- both Overleaf reports were synchronized and compiled successfully on
  2026-04-19

## Phase 1 reporting status

- the old Overleaf `pipeline_report.tex` / `pipeline_report.pdf` narrative was
  split into two canonical Phase 1 reports
- `phase_01_dataset_construction/01_country_only_and_adjacent_only/main.tex`
  now documents the retained construction workflow for the country-only and
  adjacent-only artist universes
- `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/main.tex`
  now documents the retained Ultimate Guitar download, supplementation, and
  final-dataset workflow
- both reports were synchronized to the shared Overleaf folder and compiled
  successfully there on 2026-04-19
- the legacy root-level and Phase 1.2 `pipeline_report.tex` /
  `pipeline_report.pdf` files were removed from Overleaf after the split
- the retained reporting rule for Phase 1 is now:
  - reports remain English-only
  - Python code is referenced by exact script path and line ranges, not pasted
    as full snippets
  - Stata code, when substantively used, should appear only as targeted
    snippets after the explanatory text, never as full-script dumps

## Reporting language rule

Canonical Overleaf reports for all phases are now explicitly English-only.

## Destructive cutover status

A destructive-cutover plan has now been written in
`workspace_maps/destructive_cutover_plan_2026-04-18.md`.

Current conclusion:

- the recommended-order points 1-5 are complete
- the active branch runs from `execution/phase_*` only
- the pre-cutover snapshot branch
  `codex-archive-pre-destructive-cutover-2026-04-19` preserves the validated
  last state before deletion
- the validation set succeeded for all required Python paths and for the two
  required exploratory Stata paths before deletion
- the `r(199)` lines in the Stata logs came from the user's personal
  `profile.do` (`panelwhiz` missing), not from project code
