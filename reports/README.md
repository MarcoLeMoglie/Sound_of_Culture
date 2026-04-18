# Reports

This folder is the phase-based index for all project reports.

Canonical phase layout:

- `phase_01_dataset_construction/`
- `phase_02_exploratory_analysis/`
- `phase_03_validation/`
- `phase_04_causal_shocks/`

The editable report projects and copied compilation assets live in the synced
Overleaf folder:

- `/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Applicazioni/Overleaf/Sound of culture`

This local `reports/` tree documents what belongs in each phase and keeps the
repo structure aligned with the Overleaf structure.

## Standard reporting convention

Each phase report should have:

- a main report entry point in Overleaf
- copied code snippets or full code files when they are essential to explain
  the workflow
- generated tables in a dedicated `generated/` or equivalent phase subfolder
- figures and exported outputs under `analysis_outputs/`
- explicit documentation of retained attempts, discarded attempts, and next
  steps

## Current canonical Overleaf entry points

- `phase_01_dataset_construction/01_country_only_and_adjacent_only/main.tex`
- `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/pipeline_report.tex`
- `phase_02_exploratory_analysis/main.tex`
- `phase_03_validation/main.tex`
- `phase_04_causal_shocks/01_war_deaths/main.tex`
- `phase_04_causal_shocks/02_china_shock/main.tex`

## Important note

The root-level historical Overleaf files are being preserved for compatibility,
but the phase-based directories above are now the canonical destinations for
future reporting work.
