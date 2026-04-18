# Restructure Block History

Date: 2026-04-18

Branch:

- `codex/restructure-workspace-2026-04-18`

## Purpose

This file records what was actually done during the restructuring program that
redefined the project around the new "music as culture" agenda.

## Block 1: Archive and freeze the old project

What was done:

- created a dedicated archival branch from the pre-restructure project state
- preserved dirty working-tree material instead of losing it during cleanup
- pushed the archive branch to GitHub

Why it mattered:

- the user explicitly required that no historical material be destroyed before
  being safely preserved on a separate branch

Result:

- archival branch: `codex-archive-pre-restructure-2026-04-18`

## Block 2: Create the new phase-based skeleton

What was done:

- created phase-based repo folders under `reports/`, `execution/`, and
  `project_memory/`
- created the Overleaf phase folder skeleton
- copied the existing exploratory and pipeline reports into their new canonical
  phase destinations
- preserved legacy `execution/step*` paths in place

Why it mattered:

- the project needed a canonical architecture aligned with the new research
  agenda without breaking currently working code

## Block 3: Rewrite directives and operating instructions

What was done:

- rewrote `AGENTS.md`, `Gemini.md`, and `CLAUDE.md`
- replaced the old directives with the new phase-based SOP layer
- created explicit memory and reporting rules for the project

Why it mattered:

- the old instruction layer was tied to the previous project framing and did
  not encode the new four-phase workflow

## Block 4: Configure and validate Supermemory

What was done:

- configured `supermemory` in the repo MCP files
- configured local Codex authentication via
  `bearer_token_env_var = "SUPERMEMORY_API_KEY"`
- validated `whoAmI`, `memory(save)`, and `recall` in-session
- removed credentials from tracked repo files and kept auth local-only

Why it mattered:

- cross-conversation and cross-agent continuity is central to this project

## Block 5: Standardize code-review-graph

What was done:

- rebuilt the graph for the current workspace
- added a Kiro MCP config
- corrected local Antigravity config so it points at this repo
- documented graph limits and path-noise issues in a dedicated SOP

Why it mattered:

- the graph is the first-line exploration tool for active Python and shell code
- without a shared, correct setup different clients would have been reading
  different repositories or stale graph states

Important limit documented in block 5:

- current graph coverage is `python` and `bash` only
- Stata, LaTeX, Overleaf content, and data artifacts still require direct file
  reads

## Block 6: Standardize the Overleaf/report system

What was done:

- added a full repository `README.md` for collaborator onboarding
- created a root Overleaf README index
- created phase-report entry templates for the missing phase reports
- created standardized report subfolders for `code`, `generated`, and
  `analysis_outputs`
- updated repo-side report and memory inventories to reflect the new system

Why it mattered:

- the project needed a stable, explicit reporting architecture that makes it
  easy for collaborators to understand where each phase report lives, how it is
  supposed to be updated, and where supporting outputs should be stored

## Block 7: Public-facing cutover preparation

What was done:

- set the restructured branch as the GitHub default branch
- added the repository About description on GitHub
- generated a repository banner image for the new landing page
- updated the root README so GitHub now opens on the restructured project
  framing
- created a cutover inventory documenting what still blocks destructive legacy
  cleanup

Why it mattered:

- the restructured branch is now the public-facing entry point for the project
- collaborators landing on GitHub now see the redefined project rather than the
  old one
- the cutover inventory prevents accidental deletion of still-referenced legacy
  paths

## Block 8: Initial legacy-reference migration

What was done:

- created phase-based Python entrypoint wrappers for the main Phase 1 workflows
- created phase-based Stata wrapper entrypoints for the exploratory analysis
- switched active directives and README files to point first to the new
  phase-based execution surface
- updated one active hardcoded code reference so it now resolves the country
  builder through the phase-based wrapper instead of the old direct step path

Why it mattered:

- the project needed a public execution surface aligned with the new
  architecture before any destructive cleanup of `execution/step*`
- this reduces user-facing dependence on legacy paths without touching archival
  or replication-package material

What is still not done:

- the underlying implementation still lives in `execution/step*`
- replication-package copies and archival snapshots still preserve old paths by
  design
- a later migration block is still needed before legacy folders can be removed

## Block 9: Operational entrypoint migration

What was done:

- added the remaining high-value Phase 1 Python wrappers for
  `final_merge_restricted_v2.py` and `validate_country_primary_artists.py`
- added phase-based Stata wrapper launchers for the Phase 1 replication
  scripts
- updated the main replication READMEs so project-root reruns point first to
  `execution/phase_01_dataset_construction/`
- updated historical Antigravity notes so they no longer instruct collaborators
  to launch the old `execution/step4_country_artists/` entrypoint directly
- adjusted key replication orchestrators to prefer phase-based project-root
  entrypoints when those wrappers exist

Why it mattered:

- after block 8, the biggest remaining practical confusion was that the
  project's public documentation and launcher surface still mixed new and old
  path conventions
- this block makes the phase-based surface more complete for both Python and
  Stata while still preserving audited historical package layouts

What is still not done:

- internal module imports still frequently use `execution.step*`
- replication-package internals intentionally preserve the old directory names
- destructive cleanup of legacy folders is still not safe

## Block 10: First internal-import migration

What was done:

- turned key Phase 1 wrapper files into importable bridge modules instead of
  script-only launchers
- added bridge modules for `music_indices`, `scraper_client`, and
  `augment_country_only_universe_from_billboard`
- rerooted a first active set of Python imports from `execution.step*` toward
  `execution.phase_01_dataset_construction`
- documented explicitly in the repo instructions that every new modification
  must remember the coexistence rule until the migration is fully complete

Why it mattered:

- after block 9, the biggest remaining architectural inconsistency was inside
  active Python imports, not only in launcher instructions
- importable bridge modules let the project converge toward the phase-based
  architecture without forcing immediate deletion of the legacy implementation

What is still not done:

- many local-only imports still exist inside the legacy subpackages
- bundled replication packages still preserve historical names by design
- destructive cleanup of `execution/step*` remains unsafe

## Block 11: Runtime validation of the bridge layer

What was done:

- added a phase-based wrapper for
  `build_billboard_country_supplemental_targets.py`
- ran the Billboard supplemental-target builder through the phase-based
  surface using cached local inputs
- ran the country-songs replication wrapper through the phase-based surface
- confirmed that the country-songs replication package refreshed correctly
  from the current project state

Why it mattered:

- after block 10, the main unresolved question was whether the new bridge
  layer held up during real execution rather than only during import checks
- this block validates both a local data-building workflow and a replication
  wrapper workflow through the new canonical Phase 1 surface

What is still not done:

- no full live enrichment or scraping rebuild was attempted in this block
- the artist-universe replication path was not yet rerun through the new
  surface
- destructive cleanup of legacy folders remains unsafe

## Transitional rule that still applies

The project is not yet in the final cutover state.

That means:

- legacy `execution/step*` folders are still active
- phase-based folders are canonical for organization and reporting
- final destructive cleanup should happen only after the relevant migration
  block, not before
