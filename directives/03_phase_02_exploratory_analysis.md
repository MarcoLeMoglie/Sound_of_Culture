# Phase 2 Directive: Exploratory Analysis

## Goal

Audit the dataset and describe broad temporal and spatial patterns before any
validation or causal use.

## Main tasks

- detect construction issues
- describe temporal patterns
- describe spatial patterns
- identify suspicious missingness, coverage problems, or sample shifts

## Existing code base to check first

- `execution/step3_analysis/`

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
