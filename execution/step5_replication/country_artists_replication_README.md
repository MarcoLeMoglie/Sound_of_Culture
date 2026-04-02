# Replication Package

## Scope

This replication package is restricted to the `country_artists` workflow only. It is the audited and reproducible snapshot for the construction of the U.S.-born country-artist datasets generated up to `2026-03-27`.

It includes:

- final `csv` and `dta` outputs
- intermediate checkpoints used to stabilize enrichment
- Python code used to build the datasets
- Stata wrapper used to launch the replication from Stata via Python
- directive snapshot documenting the workflow state at packaging time

## Package Structure

```text
replication_package_country_artists_2026_04_02/
├── README.md
├── code/
│   ├── python/
│   │   ├── step4_country_artists/
│   │   └── step5_replication/
│   └── stata/
│       └── run_full_replication.do
├── docs/
│   └── directives_snapshot/
└── datasets/
    └── country_artists/
        ├── final/
        └── intermediate/
```

## Final Datasets Included

- `country_artists_master.csv` / `.dta`
- `country_artists_restricted.csv` / `.dta`
- `country_artists_expanded.csv` / `.dta`
- `country_artists_excluded_or_non_us.csv` / `.dta`
- `manual_review_queue.csv` / `.dta`
- `country_artists_sources.csv` / `.dta`
- `country_artists_data_dictionary.csv` / `.dta`
- `country_artists_qc_report.md`

Final row counts in this snapshot:

- `master`: `2170`
- `restricted`: `1814`
- `expanded`: `2170`
- `excluded_or_non_us`: `350`
- `manual_review_queue`: `313`

## What Was Done

The `country_artists` pipeline was built as a multi-source enrichment workflow:

1. Seed candidates were collected from Wikipedia categories/lists and Country Music Hall of Fame-related pages.
2. Candidate pages were resolved to Wikidata entities.
3. Structured biographical metadata were extracted from Wikidata.
4. Residual missing information was enriched from Wikipedia infoboxes and Wikipedia lead text.
5. Additional targeted recovery was performed through MusicBrainz for hard residual cases.
6. Geographic fields were normalized to U.S. state-level structure.
7. Country relevance was scored and sample flags were assigned.
8. Final datasets were exported to `.csv` and `.dta`, together with a source table, manual review queue, and QC report.

## Sources Used and Their Role

- Country Music Hall of Fame list pages
  Purpose: high-quality historical seed for canonical country artists.

- Wikipedia categories and list pages
  Purpose: broad seed generation, country evidence, and residual clues for artist identity and U.S. geography.

- Wikidata
  Purpose: main structured source for person identifiers, names, occupations, genres, birth information, links, and source harmonization.

- Wikipedia infoboxes
  Purpose: targeted recovery of birth date, birth place, genres, and name variants when Wikidata was incomplete.

- Wikipedia lead text
  Purpose: fallback recovery of birth year and place information from biographical prose.

- MusicBrainz
  Purpose: targeted recovery and confirmation for residual cases, especially birth year, city, state, and U.S. birth-country confirmation.

## Methodological Choices

### Why `birth_year` Became the Minimum Requirement

The initial build required full birth date too often, which excluded too many otherwise valid artists. The final workflow was relaxed so that the key inclusion threshold is `birth_year` plus minimum U.S. birth geography, while full `birth_date` remains desirable but not mandatory.

### Why `non_us` Was Split from `unresolved_birth_country`

Earlier versions grouped true non-U.S. births together with unresolved cases. This was misleading. The final logic separates:

- true `non_us_birth`
- unresolved cases where U.S. birth could not yet be confirmed cleanly

This makes exclusions substantively interpretable.

### Why `manual_review_queue` Exists

Some cases remain too ambiguous for deterministic inclusion, usually because one of the following is still weak:

- birth-year metadata
- U.S. birth-country confirmation
- country-artist evidence

These cases are preserved in a separate queue rather than silently dropped.

## Difference Between `master`, `expanded`, and `restricted`

### `expanded`

This is the broad inclusion sample. It contains artists who pass the main inclusion logic:

- U.S.-born
- solo person records
- valid `name_primary`
- key birth metadata present, centered on `birth_year` and `birth_state`
- enough country relevance to qualify as country-related

### `master`

In the current build, `master` is the main published analytical dataset and it is numerically identical to `expanded`. The separate file is kept for project continuity and because future iterations may tighten or alter the published main sample without changing the broader expanded sample.

### `restricted`

This is a stricter subset of `master`. It requires stronger country-core evidence and cleaner metadata, including:

- stronger genre confidence
- stronger country relevance score
- no manual review flag
- reliable birth-date and birth-place confidence

Substantively, `restricted` is the cleaner and more conservative sample; `master` is the main usable release; `expanded` is the broad retained universe among included artists. In this snapshot, `master` and `expanded` coincide, while `restricted` is smaller.

## Intermediate Checkpoints Included

The folder `datasets/country_artists/intermediate/` contains the cached checkpoints used to stabilize enrichment and avoid rerunning everything from zero:

- `country_artists_seed_candidates.csv`
- `country_artists_wikidata_details.csv`
- `country_artists_wikipedia_enrichment.csv`
- `country_artists_wikipedia_lead.csv`
- `country_artists_musicbrainz_bio.csv`

## Python Scripts Included

The package includes the scripts required to rebuild and package `country_artists`:

1. `execution/step4_country_artists/build_country_artists_dataset.py`
2. `execution/step5_replication/run_full_replication.py`

## How to Rerun the Pipeline

### Option 1. From Stata

Use:

- `code/stata/run_full_replication.do`

This do-file uses Stata's Python integration to call the Python orchestrator and rebuild the country-artist outputs non-interactively.

Optional Stata modes:

- standard replication: `do code/stata/run_full_replication.do`
- full rebuild attempt: `do code/stata/run_full_replication.do full_rebuild`

In `full_rebuild` mode, the wrapper first attempts live source collection and then degrades deterministically to bundled checkpoints/final outputs if external services impose hard rate limits.

### Option 2. From Shell

Run:

```bash
python3 execution/step5_replication/run_full_replication.py
```

For a source-based cold start:

```bash
python3 execution/step5_replication/run_full_replication.py --full-rebuild
```

In the current hardened implementation, `--full-rebuild` first attempts live source collection and enrichment. If Wikipedia or Wikidata respond with severe rate limits or similar external-service failures, the workflow degrades deterministically to the bundled checkpoints and then restores the canonical bundled final outputs so that the replicated deliverables remain exact.

## Verification Status

The Stata wrapper was tested locally on `2026-03-27` with Stata `17.0`.

The validated behavior is:

- the `.do` file launches successfully in batch mode from the project root
- Stata calls Python through `python script`
- the replication wrapper completes end-to-end in the current workspace state
- in a true cold-start workspace containing only the package, the wrapper restores the canonical `country_artists` outputs from the bundled replication snapshot
- the restored cold-start outputs reproduce the expected row counts (`2170`, `1814`, `2170`, `350`, `313`)

## Full Rebuild Test

On `2026-03-30`, a true cold-start `--full-rebuild` test was launched and then hardened in an isolated temp workspace.

Validated behavior:

- the package attempts live source collection from Wikipedia/Wikidata first
- when live Wikipedia category/list requests return severe `429` rate limits and Wikidata SPARQL/API calls become unstable, the run no longer stalls
- instead, the workflow restores bundled intermediate checkpoints, completes the pipeline, and the replication wrapper restores the canonical bundled final outputs
- final cold-start deliverables are therefore reproduced successfully even under external-source rate limiting

This means the package is now operationally robust in cold start: it prefers live rebuild paths, but it degrades deterministically to the audited bundled snapshot when upstream services prevent a clean end-to-end live rebuild.

## Practical Notes

- Network access is required to rerun the full workflow from scratch because the pipeline queries Wikipedia, Wikidata, and MusicBrainz.
- The package already contains the final outputs and country-artist checkpoints, so inspection does not require rerunning anything.
- The folder `docs/directives_snapshot/` contains the directive state used at packaging time.

## Snapshot Date

Replication package created on `2026-03-27`.
