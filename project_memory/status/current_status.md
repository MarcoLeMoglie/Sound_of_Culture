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

## Current block

- block 12 completed as the second runtime-validation pass plus GitHub README
  link repair

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

Proceed to the next migration/testing pass that decides whether to migrate more
remaining legacy helper modules into bridge form or to validate a higher-cost
Phase 1 path such as a richer artist-side workflow before touching more
implementation internals.
