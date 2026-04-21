# Direttiva 06: Replication Package Merge-To-V6

## Obiettivo
Costruire un replication package dedicato all'ultimo tratto della pipeline `country_artists`, cioe' dal merge iniziale tra `country_songs` e `country_artists` fino al file finale `Sound_of_Culture_Country_Restricted_Final_v6`.

## Input
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v5.csv`
- `data/processed_datasets/country_artists/country_artists_restricted.csv`
- `data/processed_datasets/country_artists/country_artists_master.csv`
- `data/processed_datasets/country_artists/manual_review_queue.csv`
- `data/processed_datasets/country_artists/country_artists_excluded_or_non_us.csv`
- `data/processed_datasets/country_artists/country_artists_data_dictionary.csv`
- cache e checkpoint in `data/processed_datasets/country_artists/intermediate/restricted_final_v6_backfill/`
- cache dei `release_year` in `data/processed_datasets/country_artists/intermediate/json_caches/`
- script in `execution/step2_digitalize/` e `execution/step5_replication/`
- materiali Antigravity salvati nelle direttive del progetto

## Esecuzione
1. Ripristinare nel progetto gli input upstream bundled del package.
2. Rieseguire il merge con `final_merge_restricted_v2.py`.
3. Rieseguire il backfill demografico con `backfill_restricted_final_v6_demographics.py`.
4. Rieseguire il consolidamento dei `release_year` con `enrich_release_years_restricted_final_v6.py`.
5. Validare che il file finale `v6` abbia `name_primary` e `release_year` completi.
6. Aggiornare nel package gli snapshot degli input upstream, degli intermedi e degli output finali.
7. Fornire un wrapper `.do` Stata che lanci l'orchestratore Python senza passaggi manuali.

## Output
- `data/processed_datasets/country_artists/replication_package_country_merge_v6_2026_04_03/`

## Casi Limite & Note
- Il package deve essere cold-start replicabile senza dipendere dal fetch live di Wikipedia/Wikidata.
- Per questo il package deve includere un lookup precomputato per il backfill degli artisti indicizzati solo in `artist_name`.
- Il launcher Stata deve essere testato su workspace isolato.
- Il package finale deve convivere con:
  - `replication_package_country_songs_2026_04_01`
  - `replication_package_country_artists_2026_04_02`

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step5_replication/run_country_merge_v6_replication.py > log/run_country_merge_v6_replication.log`
2. `python3 execution/step2_digitalize/final_merge_restricted_v2.py > log/final_merge_restricted_v2.log`
3. `python3 execution/step2_digitalize/backfill_restricted_final_v6_demographics.py > log/backfill_restricted_final_v6_demographics.log`
4. `python3 execution/step2_digitalize/enrich_release_years_restricted_final_v6.py > log/enrich_release_years_restricted_final_v6.log`
