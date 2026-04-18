# Sound of Culture

This repository studies how music can be used as a measure of culture in the
United States.

The current project design has four canonical phases:

1. dataset construction
2. exploratory analysis
3. validation against external culture measures
4. causal applications using war deaths and the China shock

This README is the main onboarding file for collaborators and agents. It
explains the project logic, the folder structure, the installed shared tools,
the memory system, and the restructuring work already completed on the current
branch.

## Current project status

Active working branch:

- `codex/restructure-workspace-2026-04-18`

Archival branch that preserves the pre-restructure project state:

- `codex-archive-pre-restructure-2026-04-18`

Current restructuring status:

- block 1 completed: archival snapshot created and pushed
- block 2 completed: phase-based workspace skeleton created
- block 3 completed: directives, SOPs, and agent instructions rewritten
- block 4 completed: Supermemory configured and validated in-session
- block 5 completed: code-review-graph standardized and rebuilt
- block 6 completed: phase-based Overleaf/report system standardized with
  canonical templates and folder conventions

For a block-by-block history, see:

- [workspace_maps/restructure_block_history_2026-04-18.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/workspace_maps/restructure_block_history_2026-04-18.md)

## Research roadmap

### Phase 1: Dataset construction

The project first builds the artist and song-level raw material needed to turn
music into a cultural measure.

This phase has two distinct report objects:

- `country-only` and `adjacent-only` artist-universe construction
- final Ultimate Guitar dataset construction and missing-data recovery

### Phase 2: Exploratory analysis

The constructed dataset is then audited to:

- detect construction problems
- describe temporal patterns
- describe spatial patterns
- document early regularities that matter for later validation

### Phase 3: Validation

The music-based culture measure is then compared with external culture measures
across US states.

### Phase 4: Causal applications

The validated measure is then used in causal applications:

- war deaths from 1946 onward
- the China shock in US manufacturing

## Repository structure

The repository is in a transition state.

The canonical conceptual organization is phase-based, but much of the active
code still lives in legacy `execution/step*` folders. This is intentional. We
are avoiding breaking hardcoded paths before the final migration step.

High-level structure:

- `AGENTS.md`
  Shared operating instructions for Codex and other agents.
- `directives/`
  Canonical SOP and phase directives.
- `execution/`
  Working code. Legacy `step*` folders remain active. New `phase_*` folders are
  the canonical future landing points.
- `reports/`
  Repo-side mirror of the canonical Overleaf report structure.
- `project_memory/`
  Shared written memory for cross-agent continuity.
- `workspace_maps/`
  Restructuring maps and transition documentation.
- `data/`
  Project data and replication packages.

Important subtrees:

- `execution/step1_download/`
  Ultimate Guitar discovery and download logic.
- `execution/step2_digitalize/`
  Song-level processing and final dataset assembly logic.
- `execution/step3_analysis/`
  Legacy exploratory analysis code and report generation.
- `execution/step4_country_artists/`
  Country artist-universe construction and metadata enrichment.
- `execution/step5_replication/`
  Replication wrappers and cold-start packaging logic.
- `execution/phase_*`
  Canonical phase directories created during restructuring.

## Canonical report structure

The canonical narrative outputs live in Overleaf, not in the repo.

Overleaf root:

- `/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Applicazioni/Overleaf/Sound of culture`

Canonical report layout:

- `phase_01_dataset_construction/01_country_only_and_adjacent_only/`
- `phase_01_dataset_construction/02_final_dataset_ultimate_guitar/`
- `phase_02_exploratory_analysis/`
- `phase_03_validation/`
- `phase_04_causal_shocks/01_war_deaths/`
- `phase_04_causal_shocks/02_china_shock/`

Repo-side mirrors:

- `reports/phase_01_dataset_construction/`
- `reports/phase_02_exploratory_analysis/`
- `reports/phase_03_validation/`
- `reports/phase_04_causal_shocks/`

Block 6 standardized the Overleaf structure by creating phase-specific report
entry points and output folders for the phases that did not yet have a real
report file.

## Shared memory system

This project must be readable and operable by:

- Codex in this thread
- Antigravity
- a coauthor's Codex
- other MCP-aware coding tools configured on the same machine

There are two memory layers.

### 1. Repo-side written memory

This is mandatory and lives in:

- `project_memory/status/`
- `project_memory/decisions/`
- `project_memory/handoffs/`
- `project_memory/inventories/`

At minimum, each meaningful work unit should update:

- what changed
- what remains unresolved
- what the next agent should do
- whether a result was kept, rejected, or remains provisional

### 2. Supermemory

Supermemory is now operational for the project scope:

- `sound-of-culture`

Important rule:

- repo MCP files are intentionally secret-free
- authentication lives only in local client configuration
- the API key must never be committed

Current local Codex pattern:

- `~/.codex/config.toml`
- `bearer_token_env_var = "SUPERMEMORY_API_KEY"`
- header `x-sm-project = "sound-of-culture"`

For more detail, see:

- [project_memory/inventories/memory_system.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/project_memory/inventories/memory_system.md)

## Shared tools installed for this project

### code-review-graph

This is the primary structural exploration tool for Python and shell code.

Configured clients:

- Codex
- Cursor
- OpenCode
- Kiro
- local Antigravity

Important limit:

- current graph coverage is `python` and `bash` only
- it does not structurally index Stata, LaTeX, or data artifacts

That means graph-first is correct for code exploration, but direct file reads
remain necessary for Stata, Overleaf, and many outputs.

See:

- [directives/10_code_review_graph_sop.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/directives/10_code_review_graph_sop.md)
- [project_memory/inventories/review_graph_system.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/project_memory/inventories/review_graph_system.md)

### caveman-compression

Installed locally for Codex at:

- `~/.codex/vendor/caveman-compression`

Current install notes:

- dedicated virtual environment created
- NLP dependencies installed
- spaCy model download failed twice because of upstream GitHub `504` errors
- local install patched with a blank English pipeline fallback so it is still
  usable offline

See:

- [project_memory/inventories/local_tooling.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/project_memory/inventories/local_tooling.md)

## How a coauthor should start

1. Clone the repo and check out `codex/restructure-workspace-2026-04-18`.
2. Read:
   - `AGENTS.md`
   - `directives/README.md`
   - `project_memory/status/current_status.md`
   - `project_memory/handoffs/next_agent.md`
   - this `README.md`
3. Get access to the Overleaf/Dropbox project folder.
4. Configure local Supermemory authentication with the shared project scope
   `sound-of-culture`.
5. Never commit secrets.

## What must be documented every time

Every substantial new attempt must be reflected in the relevant phase report in
plain English, including:

- what was done
- why it was done
- how it was done
- what code was used
- what outputs were generated
- whether the attempt is retained in the final workflow
- if dropped, why it was dropped

This rule applies even to discarded attempts if they informed the final
project.

## Known transition rules

- Legacy `execution/step*` code is still active.
- Phase-based folders are the canonical organizational layer.
- Do not move legacy execution folders unless a task explicitly includes path
  migration.
- The repo contains archival / replication snapshots such as `.coldstart_*`.
  These may create duplicate results in graph search and should not be confused
  with active working paths.

## Main reference files

- [AGENTS.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/AGENTS.md:1)
- [directives/README.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/directives/README.md:1)
- [project_memory/status/current_status.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/project_memory/status/current_status.md:1)
- [project_memory/handoffs/next_agent.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/project_memory/handoffs/next_agent.md:1)
- [reports/README.md](/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture/reports/README.md:1)
