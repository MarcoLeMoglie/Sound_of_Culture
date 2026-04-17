# Country Artist Universe Replication

## What this package replicates

This replication package reproduces the three final artist-universe deliverables generated in the project:

- `artist_universe_country_only.csv` / `.dta`
- `artist_universe_adjacent_only.csv` / `.dta`
- `artist_universe_country_plus_adjacent.csv` / `.dta`

These files are the final artist-side staging datasets created before any new large-scale Ultimate Guitar download round. The package is designed to replicate the final canonical outputs exactly in a clean workspace.

Package folder:

- `data/processed_datasets/country_artists/replication_package_country_artist_universe_2026_04_08/`

Final output target during replication:

- `data/processed_datasets/country_artists/`

## Why this package exists

The project originally built a country-artist registry tied to the country song pipeline. That earlier registry was useful, but it mixed together two different problems:

1. identifying a broad enough universe of relevant artists
2. collecting clean demographic and geographic metadata for those artists

The new artist-universe build separated the artist problem from the song problem. The goal was to create a broader and better-documented artist frame before any further Ultimate Guitar expansion. This required:

- integrating artists already present in the current country song dataset
- integrating artists already present in the cleaned `country_artists_restricted` and `country_artists_master` files
- deduplicating this country core carefully
- expanding outward into adjacent but relevant genres
- enriching missing demographic fields with the same logic previously used for the country-artist datasets
- exporting the result in both `csv` and `dta` with stable labels

This package preserves the final outputs of that process and allows exact cold-start replication.

## Final datasets and counts

At the time this package was finalized, the canonical counts were:

- `artist_universe_country_only`: `2333`
- `artist_universe_adjacent_only`: `2493`
- `artist_universe_country_plus_adjacent`: `4824`

The arithmetic difference between `2333 + 2493` and `4824` is explained by normalization and final deduplication in the combined file. A small number of variant-name cases collapse in the combined universe, for example accented and unaccented name variants.

## What was done to build the datasets

### 1. Built the country core

The country core was created by integrating three already-existing project assets:

- artists observed in the current song file `Sound_of_Culture_Country_Restricted_Final_v6.csv`
- artists in `country_artists_restricted.csv`
- artists in `country_artists_master.csv`

These sources were stacked into a common artist schema, normalized on a cleaned `name_key`, and deduplicated using a rule that preferred:

- more complete demographic information
- stronger country relevance
- stronger source support
- restricted-sample membership when ties remained

This produced the cleaned country-core universe that later became `artist_universe_country_only`.

### 2. Added adjacent genres

After the country core was stabilized, the build expanded into adjacent artist pools using genre-oriented seeds. The adjacent tags used in the build were:

- `folk`
- `americana`
- `bluegrass`
- `rockabilly`
- `western swing`
- `outlaw country`
- `roots rock`
- `country gospel`

The purpose of this expansion was not to redefine all these artists as country artists. Instead, it created a broader documented universe surrounding the country core, so that later song-discovery work would not be artificially limited by the original country-only registry.

### 3. Reused the country-artist enrichment logic

The demographic recovery strategy followed the same logic already used for the main country-artist build:

- Wikidata identifiers and structured fields when available
- Wikipedia infoboxes and lead text as semi-structured recovery
- MusicBrainz for artist identifiers, area, and life-span style metadata
- Discogs when useful as a complementary music database
- iTunes / Apple Music when helpful for artist confirmation or weak metadata support
- targeted web confirmation for high-value unresolved cases

This was not a single-pass enrichment. The dataset was improved iteratively:

- initial country-core integration
- adjacent-genre expansion
- bulk structured-source enrichment
- repeated recovery of missing `birth_year`, `birth_country`, and `birth_state`
- manual web-confirmed overrides for specific artists where structured sources were weak but public evidence was clear

### 4. Prioritized the most important demographic variables

Across the process, the highest-priority variables were:

- `name_primary`
- `birth_year`
- `birth_city`
- `birth_state`
- `birth_country`

The workflow explicitly pushed on these first because they are the most useful for later research design and for linking artists to downstream song and geography analysis.

### 5. Preserved a clean separation of outputs

The final outputs were split into:

- `country_only`: the country-core universe
- `adjacent_only`: artists added by the adjacent-genre expansion and not kept in the country-only universe
- `country_plus_adjacent`: the final deduplicated union

The project also kept QC and methodological companion files:

- `artist_universe_qc_report.md`
- `artist_universe_methodological_note.md`
- `artist_universe_coverage_estimate_2026_04_07.csv`
- `artist_universe_coverage_estimate_2026_04_07.md`

## Main source files used

The core source scripts behind the successful build were:

- `execution/step4_country_artists/build_extended_artist_universe.py`
- `execution/step4_country_artists/enrich_artist_universe_missing_metadata.py`

The first script created the integrated artist universe from the project’s existing country outputs plus the adjacent seed pool. The second script performed repeated metadata recovery and final export, including `.dta` writing with variable labels.

## Main sources used and what they were used for

### Internal project sources

- `Sound_of_Culture_Country_Restricted_Final_v6.csv`
  - used to extract the artist names already present in the current country song file
- `country_artists_restricted.csv`
  - used as high-confidence country-artist input
- `country_artists_master.csv`
  - used as broader country-artist input

### Wikidata

Used for:

- artist identifiers
- birth date or birth year
- birth place
- occupations
- genres
- citizenship
- structured artist-level metadata

### Wikipedia

Used for:

- infobox fields
- lead-text recovery
- birthplace text
- origin for groups
- category evidence
- disambiguation and confirmation when structured databases were incomplete

### MusicBrainz

Used for:

- MusicBrainz artist IDs
- area/origin style metadata
- life-span and date cues
- extra confirmation for artist identity

### Discogs

Used for:

- complementary artist identification
- geographic cues when structured birth/origin data were partially available

### iTunes / Apple Music

Used for:

- artist confirmation
- weak-support matching and disambiguation in some adjacent cases

### Targeted web research

Used only when necessary for unresolved but important cases. This was especially useful for artists where:

- the country-core dataset would materially benefit from one more reliable demographic recovery
- public biographies clearly stated birthplace or year information
- structured databases were incomplete or inconsistent

## Key methodological choices

### Country core is not the same as the full historical universe

The country-only file is intentionally broader than a narrow canon, but it is still not a census of all country artists since the early 1920s. The coverage estimate files in the package explain this with three denominators:

- canon-restricted denominator
- encyclopedic operational denominator
- historical-total denominator

The short interpretation is:

- `2333` is far larger than a narrow official canon
- it captures about half of the broad documented artist universe assembled inside the project
- it is still smaller than the full historical universe of all U.S. country artists

### Adjacent genres are included as a staging universe, not as a claim of equal genre identity

The adjacent expansion is pragmatic. It is meant to widen later song discovery and avoid missing plausible country-relevant repertoires that sit near country, americana, bluegrass, roots, rockabilly, western swing, and related traditions.

### Missingness was reduced aggressively, but not by inventing data

Repeated efforts were made to fill demographic variables, especially `birth_year` and `birth_state`. However, when public evidence was too weak or ambiguous, missing values were preserved. This is deliberate. The build prefers reliable incompleteness over fabricated precision.

## Final quality snapshot

The QC report bundled with the package records the final missingness profile. At package finalization the full universe reported:

- `name_primary`: `0` missing
- `birth_year`: `1675` missing
- `birth_city`: `1917` missing
- `birth_state`: `2141` missing
- `birth_country`: `1719` missing
- `wikidata_qid`: `699` missing
- `musicbrainz_mbid`: `2916` missing

This means the build succeeded completely on identity at the final `name_primary` level, but demographic completeness remains much stronger in the country core than in the broader adjacent universe.

## Package structure

```text
replication_package_country_artist_universe_2026_04_08/
├── README.md
├── code/
│   ├── python/
│   │   ├── step4_country_artists/
│   │   │   └── enrich_artist_universe_missing_metadata.py
│   │   └── step5_replication/
│   │       └── run_full_replication.py
│   └── stata/
│       └── run_full_replication.do
├── datasets/
│   └── artist_universe/
│       └── final/
└── docs/
    └── directives_snapshot/
```

## How the replication package works

This package is a cold-start replication package based on bundled final outputs.

The packaged Python launcher:

1. accepts a target project root
2. checks whether the three main final outputs already exist there
3. if they do not exist, restores them from the package’s bundled snapshot
4. refreshes the package tree with code and documentation

This means the package reproduces the final canonical outputs exactly and quickly, without rerunning live external enrichment.

## Stata launcher

From the main project root:

```stata
do execution/step5_replication/run_artist_universe_replication.do
```

Or directly from the packaged launcher:

```stata
do data/processed_datasets/country_artists/replication_package_country_artist_universe_2026_04_08/code/stata/run_full_replication.do
```

The Stata launcher uses `python script` and is compatible with Stata 17.

## Cold-start validation

This package was validated in two isolated cold-start environments on `2026-04-08`:

- isolated Python cold start
- isolated Stata cold start

Both tests passed and recreated the canonical outputs with the expected counts:

- `artist_universe_country_only.csv`: `2333`
- `artist_universe_adjacent_only.csv`: `2493`
- `artist_universe_country_plus_adjacent.csv`: `4824`

The Stata cold-start log was produced successfully, and the wrapper completed with exit code `0`.

## What this package does not do

- It does not rerun live web enrichment from scratch.
- It does not reproduce the entire upstream historical construction process from zero external sources.
- It does not include the song-side replication packages.

It is an exact-replication package for the final artist-universe deliverables.

## Python order in the last completed replication run

1. `python3 execution/step5_replication/run_artist_universe_replication.py`

## Practical interpretation

Use this package when you want:

- the final artist-universe datasets restored exactly
- the `.csv` and `.dta` files ready for Stata or downstream analysis
- the methodological notes and QC files preserved with the same snapshot

Do not use this package as if it were a live re-scraping or full historical rebuild of every external source. It is a reproducible final-deliverable package, not a raw-data recollection package.
