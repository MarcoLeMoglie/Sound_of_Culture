# Destructive Cutover Plan

Date: 2026-04-18

Branch baseline:

- `codex/restructure-workspace-2026-04-18`

## Purpose

This document defines the safe path from the current transitional architecture
to a future state in which the legacy `execution/step*` folders can be reduced
or removed without breaking active workflows.

The key distinction is:

- the operational restructuring is complete
- the destructive cutover is **not** complete

Today, the canonical execution surface is phase-based, but the legacy
`execution/step*` folders still act as the implementation backend.

## Current conclusion

The recommended-order points 1-5 are now complete on the active branch.

Point 5 was executed on 2026-04-19 after the fresh archival snapshot branch
`codex-archive-pre-destructive-cutover-2026-04-19` was created and pushed.

## Progress update after implementation

The following recommended-order items are now complete on the working branch:

1. Phase 2 exploratory `.do` files and output paths were migrated natively into
   `execution/phase_02_exploratory_analysis/do/` and
   `execution/phase_02_exploratory_analysis/output_figures*`
2. Phase 1 replication scripts were rerooted so the source-of-truth packaged
   paths are phase-based
3. The remaining active Phase 1 backend implementations were copied natively
   into `execution/phase_01_dataset_construction/`
4. The full required validation set was executed successfully
5. The active `execution/step*` trees were removed from the working branch

Validation note:

- the only `r(199)` lines remaining in the Stata logs come from the user's
  local `profile.do` because `panelwhiz` is missing there; the project `.do`
  files themselves now run and export their PDFs correctly

## Post-cutover result

The active branch now runs from:

- `execution/phase_01_dataset_construction/`
- `execution/phase_02_exploratory_analysis/`
- `execution/phase_03_validation/`
- `execution/phase_04_causal_shocks/`

Legacy execution layouts are no longer part of the active branch and survive
only in archival and packaged historical material.

## Classification

### Group A: Safe to remove now

Only non-canonical transient material is safely removable now.

Examples:

- `__pycache__/`
- temporary validation artifacts created during local testing
- conflict copies or editor residue outside the tracked workflow

This group does **not** include any tracked `execution/step*` code file.

### Group B: Must be migrated before removal

These are currently still required by active wrappers or active execution
paths.

#### Phase 1 backend trees

- `execution/step1_download/`
- `execution/step2_digitalize/`
- `execution/step4_country_artists/`
- `execution/step5_replication/`

Why they cannot be deleted yet:

- `execution/phase_01_dataset_construction/*.py` still uses
  `run_legacy_script(...)` or `export_legacy_module(...)` pointing into these
  folders
- replication scripts still package legacy-named code snapshots
- some legacy scripts still contain local path assumptions that have not yet
  been rerooted into native `phase_01_dataset_construction` implementations

#### Phase 2 backend tree

- `execution/step3_analysis/do/`
- `execution/step3_analysis/output_figures/`
- `execution/step3_analysis/output_figures_bass/`

Why it cannot be deleted yet:

- `execution/phase_02_exploratory_analysis/do/*.do` currently delegates to the
  legacy `.do` files
- the legacy `.do` files still write output into the legacy `step3_analysis`
  output folders

### Group C: Preserve even after cutover

These should be retained for historical reproducibility even after the active
backend migration is complete.

Examples:

- archival Git branches such as
  `codex-archive-pre-restructure-2026-04-18`
- bundled replication-package layouts under `data/processed_datasets/...`
- cutover maps and restructuring histories under `workspace_maps/`
- old output snapshots that are part of documented replication bundles

## Required work before destructive cleanup

### Stage 1: Native implementation migration

Goal:

- stop using legacy files as the runtime backend

Required actions:

- replace `run_legacy_script(...)` wrappers in
  `execution/phase_01_dataset_construction/` with native implementations or
  native imports that live directly under the phase folder
- move reusable helper logic from `step1_download`, `step2_digitalize`,
  `step4_country_artists`, and `step5_replication` into stable phase-based
  modules
- reroot replication orchestrators so their source-of-truth code paths are
  phase-based rather than legacy-path based
- create native Phase 2 exploratory `.do` files under
  `execution/phase_02_exploratory_analysis/do/` instead of pass-through
  launchers to `step3_analysis/do/`
- reroot exploratory output paths away from `execution/step3_analysis/`

Exit criterion:

- no active file under `execution/phase_*` contains direct references to
  `execution/step*` as runtime dependencies

### Stage 2: Reference cleanup

Goal:

- remove project-facing references that would keep collaborators using old
  paths by accident

Required actions:

- update all active documentation to describe legacy `step*` folders as
  archival/backend only
- update any remaining README, SOP, or report mirror that still presents
  `step*` paths as runnable entrypoints
- review Antigravity-facing notes and collaboration prompts again after native
  migration is done

Exit criterion:

- no active onboarding or execution instructions tell collaborators to run
  `execution/step*` files directly

### Stage 3: Runtime validation

Goal:

- prove that the native phase-based backend works without falling back to the
  legacy tree

Minimum validation set:

- Phase 1 dataset build path:
  `build_country_artists_dataset.py`
- Phase 1 artist-universe extension path:
  `build_extended_artist_universe.py`
- Phase 1 song-build / merge path:
  `build_country_only_chords_final.py`
- Phase 1 merge-v6 path:
  `run_country_merge_v6_replication.py`
- Phase 1 country-songs replication path:
  `run_country_songs_replication.py`
- Phase 1 artist-universe replication path:
  `run_artist_universe_replication.py`
- Phase 2 exploratory path:
  `eda.do`
- Phase 2 exploratory bass path:
  `eda_bass.do`

Exit criterion:

- all required paths run successfully from the phase-based surface without
  runtime dependence on the legacy `step*` folders

### Stage 4: Destructive cutover

Goal:

- remove the legacy backend from the active tree after proof is complete

Required actions:

- create a fresh archival snapshot branch immediately before deletion
- delete or archive the legacy execution trees from the active branch
- run a final repo-wide search for `execution/step`
- rebuild the code-review graph
- update `README.md`, `AGENTS.md`, directives, and `project_memory/` to record
  the post-cutover architecture

Exit criterion:

- the active branch runs entirely from `execution/phase_*`
- repo documentation no longer describes the project as architecturally
  transitional

## Recommended order

1. Migrate Phase 2 exploratory `.do` files and output paths first.
2. Migrate Phase 1 replication scripts next.
3. Migrate the remaining Phase 1 backend implementations.
4. Run the full validation set.
5. Only then perform the destructive deletion pass.

## What we can do immediately without risk

If the user wants cleanup before the full cutover, keep it limited to:

- cache cleanup
- temporary-file cleanup
- conflict-copy cleanup
- documentation cleanup

Do not delete tracked legacy execution files yet.
