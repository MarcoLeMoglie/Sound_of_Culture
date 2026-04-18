# Current Status

## Date

2026-04-18

## Active working branch

`codex/restructure-workspace-2026-04-18`

## Completed restructuring blocks

- block 1: archival snapshot created and pushed
- block 2: phase-based workspace skeleton created and pushed
- block 3: directives and SOP rewrite completed on the working branch

## Current block

- block 4 completed: Supermemory is authenticated and operational

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

## Next recommended step

Proceed to block 5 and standardize the review-graph / multi-agent operating
setup for the newly restructured project.
