# Direttiva 05: Replication Package

## Obiettivo
Costruire un replication package completo e leggibile solo per `country_artists`, con snapshot dei dati, codice Python, wrapper Stata e documentazione metodologica.

## Input
- Output finali in `data/processed_datasets/country_artists/`
- Checkpoint e cache in `data/processed_datasets/country_artists/intermediate/`
- Script in `execution/step4_country_artists/` e `execution/step5_replication/`
- Direttive in `directives/`

## Esecuzione
1. Creare o aggiornare la cartella fissa del replication package dentro `data/processed_datasets/country_artists/`.
2. Copiare nel package gli script Python rilevanti e lo snapshot delle direttive.
3. Copiare nel package tutti i dataset finali e i checkpoint intermedi necessari alla replica di `country_artists`.
4. Scrivere un `README` con metodo, fonti, scelte, naming, limiti e istruzioni di replica.
5. Aggiungere un file `.do` di Stata che usi l'integrazione Python di Stata per lanciare la pipeline end-to-end senza interruzioni manuali.
   Il `.do` deve supportare sia la replica standard sia una modalità opzionale `full_rebuild`.
6. Verificare il file `.do` con una run reale in Stata e correggere eventuali incompatibilità di versione o sintassi prima di considerare il package completato.

## Output
- `data/processed_datasets/country_artists/replication_package_country_songs_2026_04_01/`

## Casi Limite & Note
- Il replication package deve includere sia i dati finali sia i checkpoint intermedi utili a ridurre i tempi di rerun.
- Il file Stata deve orchestrare la pipeline, non duplicare la logica di business già contenuta negli script Python.
- La validazione minima richiede almeno una esecuzione reale del wrapper Stata sul setup locale.
- Il package deve essere testato anche in cold start: in assenza degli output nella root del progetto, il wrapper deve saper ripristinare gli output `country_artists` dal proprio snapshot interno.
- Un test separato di `full rebuild` da fonti esterne deve essere provato in workspace isolato.
- Se Wikipedia/Wikidata impongono rate limit o timeout che impediscono una ricostruzione pulita, il workflow deve degradare in modo deterministico ai checkpoint bundled e poi ripristinare gli output finali canonici del package.
- I marker di fallback e i limiti delle fonti esterne vanno documentati esplicitamente nel `README` del package.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step5_replication/run_full_replication.py`
2. `python3 execution/step4_country_artists/build_country_artists_dataset.py`
