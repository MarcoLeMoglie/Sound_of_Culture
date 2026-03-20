# Direttiva 02: Digitalizzazione Tablature

## Obiettivo
Estrarre ed elaborare le informazioni contenute nelle tablature per costruire un dataset strutturato.

## Input
- File grezzi in `data/raw_tabs/`.

## Esecuzione
- Analizzare il contenuto delle tablature (note, accordi, struttura).
- Associare ogni canzone alla data di uscita (utilizzando API esterne come Spotify, MusicBrainz, o scraping se necessario).
- Strutturare i dati in un formato tabellare.

## Output
- `data/processed_datasets/songs_dataset.csv`
- `data/processed_datasets/songs_dataset.dta` (Formato Stata)

## Casi Limite & Note
- Formati di tablatura non standard o corrotti.
- Ambiguità nel titolo della canzone o data di uscita.
