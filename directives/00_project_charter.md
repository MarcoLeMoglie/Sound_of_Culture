# Project Charter

## Core question

How can music be used as a measure of culture in the United States?

## Canonical phase structure

### Phase 1: Dataset construction

- build `country-only` and `adjacent-only` artist universes
- construct the Ultimate Guitar country dataset using the country artist list
- recover missing metadata in final datasets
- document every pipeline decision in two separate reports

### Phase 2: Exploratory analysis

- audit the built dataset
- detect construction problems
- describe broad temporal and spatial patterns

### Phase 3: Validation

- validate the music-based culture measure against external culture measures
  across US states

### Phase 4: Causal applications

- estimate the effect of social shocks on culture
- first shock: post-1946 war deaths
- second shock: China shock on manufacturing

## Canonical report destination

All editable and compilable reports live in:

- `/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Applicazioni/Overleaf/Sound of culture`

## Required report structure

- `phase_01_dataset_construction/01_country_only_and_adjacent_only/`
- `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/`
- `phase_02_exploratory_analysis/`
- `phase_03_validation/`
- `phase_04_causal_shocks/01_war_deaths/`
- `phase_04_causal_shocks/02_china_shock/`

## Shared-memory rule

The project must remain understandable across agents and conversations.

Until plugin-based memory is installed, all agents must use:

- `project_memory/status/`
- `project_memory/decisions/`
- `project_memory/handoffs/`
- `project_memory/inventories/`

## Documentation rule

Every serious attempt must be documented.

This includes:

- successful workflows kept in the final project
- failed or discarded workflows that meaningfully informed the project

Each report entry must explain:

- what was tried
- why it was tried
- how it was implemented
- whether it was retained
- if not retained, why it was rejected
