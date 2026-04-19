# Phase 2 Directive: Exploratory Analysis

## Goal

Audit the dataset and describe broad temporal and spatial patterns before any
validation or causal use.

## Main tasks

- detect construction issues
- describe temporal patterns
- describe spatial patterns
- identify suspicious missingness, coverage problems, or sample shifts

## Canonical execution entrypoints to check first

- `execution/phase_02_exploratory_analysis/do/eda.do`
- `execution/phase_02_exploratory_analysis/do/eda_bass.do`

## Architecture note

The active branch now runs this phase natively from
`execution/phase_02_exploratory_analysis/`. Historical `step3_analysis`
layouts remain only in archival branches and transition records.

## Required outputs

- tables and figures
- clearly documented Stata and Python code when used
- a plain-English report in Overleaf:
  - `phase_02_exploratory_analysis/`

## Mandatory report content

- sample definitions
- coverage and missingness notes
- interpretation of the main descriptive patterns
- warnings about what the figures do and do not imply
- code snippets for key graphs and tables
- record of discarded diagnostics and why they were not kept

## Stata standard

If Stata is used, the report must explain each analytical block and link the
code to the tables and figures it generates.
