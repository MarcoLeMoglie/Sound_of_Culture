# Block 2 Workspace Map

Date: 2026-04-18

## Goal

Introduce a phase-based project architecture without breaking the currently
working legacy code layout.

## Canonical phase structure

- `reports/phase_01_dataset_construction/`
- `reports/phase_02_exploratory_analysis/`
- `reports/phase_03_validation/`
- `reports/phase_04_causal_shocks/`
- `project_memory/`
- `execution/phase_01_dataset_construction/`
- `execution/phase_02_exploratory_analysis/`
- `execution/phase_03_validation/`
- `execution/phase_04_causal_shocks/`
- `data/external_sources/`
- `data/project_outputs/`

## Legacy-to-phase map

- `execution/step1_download/` -> `phase_01_dataset_construction`
- `execution/step2_digitalize/` -> `phase_01_dataset_construction`
- `execution/step4_country_artists/` -> `phase_01_dataset_construction`
- `execution/step5_replication/` -> `phase_01_dataset_construction`
- `execution/step3_analysis/` -> `phase_02_exploratory_analysis`

## Overleaf alignment

Overleaf project root:

- `/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Applicazioni/Overleaf/Sound of culture`

Target Overleaf phase layout:

- `phase_01_dataset_construction/01_country_only_and_adjacent_only/`
- `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/`
- `phase_02_exploratory_analysis/`
- `phase_03_validation/`
- `phase_04_causal_shocks/01_war_deaths/`
- `phase_04_causal_shocks/02_china_shock/`

## Transitional rule

During block 2, legacy `execution/step*` directories remain in place because
they are referenced by scripts, imports, and report instructions. The phase
folders created now are the canonical structure for the new project and the
future landing points for migrated code and reports.
