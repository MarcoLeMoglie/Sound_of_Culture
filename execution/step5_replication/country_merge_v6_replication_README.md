# Replication Package - Country Merge To V6

Questo package replica il tratto finale della pipeline che parte dagli output `country_songs` e `country_artists` gia' costruiti e arriva a:

- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Restricted_Final_v6.csv`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Restricted_Final_v6.dta`

## Copertura

Il package ricostruisce:

1. merge iniziale tra `Sound_of_Culture_Country_Full_Enriched_v5.csv` e `country_artists_restricted.csv`
2. backfill demografico sul file `v6`
3. controllo e consolidamento finale dei `release_year`

## Fonti e materiali usati

- snapshot `country_songs` e `country_artists` gia' presenti nel progetto
- script Python deterministici in `execution/step2_digitalize/`
- direttive e materiali Antigravity presenti in:
  - `directives/costruzione dataset artisti country/PROMPT_ANTIGRAVITY.txt`
  - `directives/costruzione dataset artisti country/CHECKLIST_ANTIGRAVITY.md`
  - `directives/costruzione dataset artisti country/readme_artisti.md`

Nota: nel repository locale non sono emersi transcript aggiuntivi delle conversazioni Antigravity oltre ai file sopra; il package documenta quindi il tratto storico usando questi artefatti e gli script presenti.

## Strategia di replicazione

- gli input upstream vengono ripristinati dal package nella root del progetto
- il merge-to-v6 viene rieseguito davvero con gli script Python originali
- per evitare dipendenze web non stabili nella fase di backfill, il package include un lookup precomputato per `artist_name`, costruito dal risultato canonico finale

## Launcher Stata

- `do data/processed_datasets/country_artists/replication_package_country_merge_v6_2026_04_03/code/stata/run_full_replication.do`

## Python Utilizzati Nell'Ultima Esecuzione Completata

1. `python3 execution/step5_replication/run_country_merge_v6_replication.py`
2. `python3 execution/step2_digitalize/final_merge_restricted_v2.py`
3. `python3 execution/step2_digitalize/backfill_restricted_final_v6_demographics.py`
4. `python3 execution/step2_digitalize/enrich_release_years_restricted_final_v6.py`
