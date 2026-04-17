# Direttiva 07: Replication Package Artist Universe

## Obiettivo
Costruire e mantenere il replication package snapshot-based dell'universo artisti finale, cioe`:
- `artist_universe_country_only`
- `artist_universe_adjacent_only`
- `artist_universe_country_plus_adjacent`

Il package deve replicare da cold start i file finali `csv` e `dta`, con launcher Python e launcher Stata.

## Input
- Output finali in `data/processed_datasets/country_artists/`
- Script in `execution/step4_country_artists/` e `execution/step5_replication/`
- Direttive e regole in `directives/` e `Gemini.md`

## Esecuzione
1. Creare o aggiornare la cartella del package con nome fisso datato dentro `data/processed_datasets/country_artists/`.
2. Copiare nel package i tre dataset finali dell'universo artisti in `csv` e `dta`.
3. Copiare nel package anche `README`, note metodologiche, stima di copertura e report QC.
4. Copiare nel package il codice Python di orchestrazione e lo script di build artist universe necessario a documentare la pipeline.
5. Copiare nel package uno snapshot delle direttive rilevanti e di `Gemini.md`.
6. Implementare un launcher Python che:
   - accetti una project root target
   - rilevi se i deliverable finali sono assenti
   - in cold start ripristini i deliverable dal bundle interno
   - aggiorni lo snapshot interno del package
7. Implementare un `.do` di Stata che lanci il launcher Python via `python script`.
8. Testare davvero il package in cold start Python e in cold start Stata, in workspace isolato, e verificare i conteggi finali.

## Output
- `data/processed_datasets/country_artists/replication_package_country_artist_universe_2026_04_08/`

## Casi Limite & Note
- Questo package e` di replica canonica dei deliverable finali, non di full rebuild live da fonti web.
- I path del package devono essere risolti in modo robusto sia da root progetto sia da package copiato in un workspace temporaneo.
- Il `.do` deve essere compatibile con Stata 17.
- Il package deve contenere solo i dataset finali rilevanti all'universo artisti, non l'intera pipeline `country_songs` o `merge-to-v6`.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step5_replication/run_artist_universe_replication.py > log/run_artist_universe_replication.log`
