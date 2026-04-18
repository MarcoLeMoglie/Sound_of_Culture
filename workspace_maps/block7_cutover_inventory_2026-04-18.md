# Block 7 Cutover Inventory

Date: 2026-04-18

## Purpose

This file records the safe pre-cutover inventory carried out after the
restructured branch became the GitHub default branch.

The goal of this block is not destructive cleanup yet. The goal is to make
clear what still blocks a full legacy-path cutover.

## What was completed safely

- the restructured branch was set as the GitHub default branch
- the repository About description was added on GitHub
- a repository banner image was generated at `assets/github-social-preview.png`
- the root README was updated so the repository landing page is immediately
  informative and visual

## Why full destructive cleanup is still blocked

Direct search across the repository shows many active references to legacy
execution paths:

- `execution/step1_download/`
- `execution/step2_digitalize/`
- `execution/step3_analysis/`
- `execution/step4_country_artists/`
- `execution/step5_replication/`

These references appear in:

- active directives
- repo READMEs
- replication wrappers
- Stata scripts
- packaged replication snapshots under `data/processed_datasets/country_artists/`

## Main blockers

### 1. Active code still depends on legacy paths

Examples include:

- `execution/step5_replication/run_full_replication.py`
- `execution/step5_replication/run_country_merge_v6_replication.py`
- `execution/step2_digitalize/backfill_restricted_final_v6_demographics.py`

### 2. Analysis and reporting still reference `step3_analysis`

Examples include:

- `reports/phase_02_exploratory_analysis/README.md`
- `execution/phase_02_exploratory_analysis/README.md`
- `directives/03_phase_02_exploratory_analysis.md`
- Stata files under `execution/step3_analysis/do/`

### 3. Replication packages embed old path references by design

Multiple packaged replication directories under
`data/processed_datasets/country_artists/replication_package_*` intentionally
preserve historical paths and should not be rewritten casually.

## Safe interpretation

At this stage:

- the new branch is the public-facing working branch
- the repository landing page is already aligned with the new project identity
- the project is **not** yet ready for destructive deletion of legacy execution
  folders

## Recommended next cutover step

Before deleting or renaming legacy execution folders, perform a targeted
reference migration block that:

1. inventories only the active non-archival references
2. rewrites those references to phase-based destinations where appropriate
3. preserves replication-package and archival snapshots untouched
4. verifies that scripts and reports still run after path changes
