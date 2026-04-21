# Combined War Deaths Datasets

This folder contains harmonized individual-level datasets built from two different source families:

1. `iCasualties` web tables for Iraq and Afghanistan
2. Historical `DCAS` flat files for Vietnam and Korea

## Files in this folder

- `war_deaths_individual_level.csv`
  Combined dataset with Iraq, Afghanistan, Vietnam, and Korea.
- `vietnam_korea_war_deaths_individual_level.csv`
  Subset containing only Vietnam War and Korean War records.
- `iraq_afghanistan_war_deaths_individual_level.csv`
  Subset containing only Iraq War and Afghanistan War records.

## Source files

### Iraq and Afghanistan source

These records come from `iCasualties.org`, scraped from the HTML tables at:

- `https://icasualties.org/App/Fatalities`
- `https://icasualties.org/App/AfghanFatalities`

The raw scraped outputs are stored in:

- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/raw_data/icasualties/iraq_fatalities.csv`
- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/raw_data/icasualties/afghan_fatalities.csv`

They were generated with:

- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/scrape_icasualties.py`

### Vietnam and Korea source

These records come from local historical casualty files already present in the project:

- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/raw_data/causalties/DCAS.VN.EXT08.DAT.txt`
- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/raw_data/causalties/DCAS.KS.EXT08.DAT.txt`

There is also a reference PDF in the same folder:

- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/raw_data/causalties/DCAS.VN08.DOC copy.pdf`

The harmonized combined file was generated with:

- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/build_combined_casualties_dataset.py`

The two subset exports were generated with:

- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/build_vietnam_korea_dataset.py`
- `/Users/tommaso.colussi/Dropbox/codex_projects/sound_of_culture/build_iraq_afghan_dataset.py`

## Harmonized columns

The final CSVs use the following columns:

- `name`
- `death_information`
- `death_date`
- `war`
- `death_location`
- `age`
- `ethnicity`
- `birth_state`
- `birth_county`
- `birth_city`
- `source_dataset`
- `source_file`

## Dissimilarities between the two source families

### 1. Data format

- `iCasualties` is a modern HTML table scraped from a website.
- `DCAS` is a pipe-delimited historical flat file with many coded fields.

### 2. Variable richness

- `DCAS` contains substantially richer demographic and military detail, including birth city, birth county, birth state, multiple casualty-status fields, and race/ethnicity-related fields.
- `iCasualties` exposes a smaller, cleaner table with fewer columns.

### 3. Geography fields

- In `DCAS`, birthplace fields are explicit and separate.
- In `iCasualties`, the table provides `State` and `City`, but not an explicit birthplace label and not a county field. In the harmonized output these were mapped to `birth_state` and `birth_city`, while `birth_county` is left blank.

### 4. Ethnicity availability

- `DCAS` includes race/ethnicity-style fields that can be mapped into the harmonized `ethnicity` column.
- `iCasualties` does not provide an ethnicity field in the visible table, so `ethnicity` is blank for Iraq and Afghanistan.

### 5. Death information structure

- `DCAS` often splits death circumstances across multiple fields such as casualty status, cause, and special status flags; these were concatenated into `death_information`.
- `iCasualties` already provides a single human-readable `Cause` field.

### 6. Date handling

- `iCasualties` gives a directly readable death date in `MM/DD/YYYY` format.
- `DCAS` uses multiple date-like fields, and the harmonized file uses the most relevant death-related date available from the record.

### 7. Standardization level

- `iCasualties` is easier to read directly but less standardized for research-grade field mapping.
- `DCAS` is denser and more structured, but requires field interpretation to become analysis-ready.

## Important caveat

Because the two source families are not natively identical, the harmonized datasets are suitable for pooled analysis only with care. In particular:

- `ethnicity` is missing for Iraq/Afghanistan
- `birth_county` is missing for Iraq/Afghanistan
- `death_information` is assembled differently across the two sources
- `death_location` is more standardized in some wars than others
