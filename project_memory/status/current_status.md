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
