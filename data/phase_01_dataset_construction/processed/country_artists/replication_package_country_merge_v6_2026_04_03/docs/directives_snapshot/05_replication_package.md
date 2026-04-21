# Direttiva 05: Replication Package

## Obiettivo
Costruire e mantenere i replication package della pipeline `country_artists`, con snapshot dei dati, codice Python, wrapper Stata e documentazione metodologica. I package attivi coprono:
- `country_songs`
- `country_artists`
- `merge-to-v6`

## Input
- Output finali in `data/processed_datasets/country_artists/`
- Checkpoint e cache in `data/processed_datasets/country_artists/intermediate/`
- Script in `execution/step2_digitalize/`, `execution/step4_country_artists/` e `execution/step5_replication/`
- Direttive in `directives/`

## Esecuzione
1. Creare o aggiornare le cartelle fisse dei replication package dentro `data/processed_datasets/country_artists/`.
2. Copiare in ogni package gli script Python rilevanti e lo snapshot delle direttive.
3. Per il package `country_songs`, includere gli output canonici `Sound_of_Culture_Country_Full*` e le cache storiche sui `release_year`.
4. Per il package `country_artists`, mantenere la pipeline di build del dataset artisti con i suoi intermedi bundled.
5. Per il package `merge-to-v6`, configurare la pipeline finale:
    - **Fase 1 (MusicBrainz Strict)**: Corrispondenza esatta artista-brano.
    - **Fase 2 (MusicBrainz Fuzzy)**: Corrispondenza flessibile per gestire varianti di naming.
    - **Fase 3 (Discogs)**: Recupero complementare tramite API Discogs.
    - **Fase 4 (Internet/Wiki Recovery)**: Recupero finale tramite ricerca web assistita o cache Internet.
6. Copiare nei package tutti i dataset finali e i checkpoint intermedi necessari alla replica.
7. Scrivere per ogni package un `README` con metodo, fonti, scelte, naming, limiti e istruzioni di replica.
8. Aggiungere un file `.do` di Stata che usi l'integrazione Python di Stata per lanciare la pipeline end-to-end senza interruzioni manuali.
9. Verificare i file `.do` con una run reale in Stata e correggere eventuali incompatibilità di versione o sintassi prima di considerare il package completato.
10. Mantenere un launcher Stata complessivo che faccia partire in sequenza i tre package.

## Output
- `data/processed_datasets/country_artists/replication_package_country_songs_2026_04_01/`
- `data/processed_datasets/country_artists/replication_package_country_artists_2026_04_02/`
- `data/processed_datasets/country_artists/replication_package_country_merge_v6_2026_04_03/`

## Casi Limite & Note
- Il replication package deve includere sia i dati finali sia i checkpoint intermedi utili a ridurre i tempi di rerun.
- Il file Stata deve orchestrare la pipeline, non duplicare la logica di business già contenuta negli script Python.
- La validazione minima richiede almeno una esecuzione reale del wrapper Stata sul setup locale.
- Il package deve essere testato anche in cold start: in assenza degli output nella root del progetto, il wrapper deve saper ripristinare gli output `country_artists` dal proprio snapshot interno.
- Un test separato di `full rebuild` da fonti esterne deve essere provato in workspace isolato.
- Se Wikipedia/Wikidata impongono rate limit o timeout che impediscono una ricostruzione pulita, il workflow deve degradare in modo deterministico ai checkpoint bundled e poi ripristinare gli output finali canonici del package.
- I marker di fallback e i limiti delle fonti esterne vanno documentati esplicitamente nel `README` del package.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step5_replication/run_country_songs_replication.py > log/run_country_songs_replication.log`
2. `python3 execution/step5_replication/run_full_replication.py > log/run_full_replication.log`
3. `python3 execution/step4_country_artists/build_country_artists_dataset.py > log/build_country_artists_dataset.log`
4. `python3 execution/step5_replication/run_country_merge_v6_replication.py > log/run_country_merge_v6_replication.log`
5. `python3 execution/step2_digitalize/final_merge_restricted_v2.py > log/final_merge_restricted_v2.log`
6. `python3 execution/step2_digitalize/backfill_restricted_final_v6_demographics.py > log/backfill_restricted_final_v6_demographics.log`
7. `python3 execution/step2_digitalize/enrich_release_years_restricted_final_v6.py > log/enrich_release_years_restricted_final_v6.log`
