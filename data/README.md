# Data Layout

The `data/` directory follows the same phase structure as the research design.
Legacy roots such as `processed_datasets/`, `intermediate/`, `raw_tabs_country/`,
and `casualties_uswars/` have been folded into the phase folders below.

## Phase 1: Dataset Construction

- `phase_01_dataset_construction/raw/`
  Raw Ultimate Guitar JSON downloads used to construct the country-song data.
- `phase_01_dataset_construction/intermediate/`
  Discovery files, scrape plans, caches, and temporary construction outputs.
- `phase_01_dataset_construction/processed/`
  Artist universes, final country-song datasets, data dictionaries, and bundled
  replication packages.
- `phase_01_dataset_construction/validation_reference_filter/`
  Reference-list validation caches and outputs used while auditing the artist
  universe.
- `phase_01_dataset_construction/logs/`
  Legacy logs retained for provenance.
- `phase_01_dataset_construction/archive/`
  Archived cold-start replication material retained for provenance.

## Phase 2: Exploratory Analysis

- `phase_02_exploratory_analysis/processed/`
  Processed Top 100 / year-end datasets used by the exploratory Stata scripts.
- `phase_02_exploratory_analysis/raw/`
  Raw Top 100 chord and bass JSON folders.
- `phase_02_exploratory_analysis/project_outputs/`
  Phase-specific output placeholders and generated material.

## Phase 3: Validation

- `phase_03_validation/external/`
  External culture-measure sources used to validate the music-based measure.
- `phase_03_validation/project_outputs/`
  Phase-specific validation outputs.

## Phase 4: Causal Shocks

- `phase_04_causal_shocks/01_war_deaths/`
  War-deaths raw, combined, and external-source data.
- `phase_04_causal_shocks/02_china_shock/`
  China-shock external-source data.
- `phase_04_causal_shocks/project_outputs/`
  Shared causal-shock output placeholders.
