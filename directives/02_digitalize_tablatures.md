# Direttiva 02: Digitalizzazione Tablature

## Obiettivo
Estrarre ed elaborare le informazioni contenute nelle tablature `top100` per costruire dataset strutturati in formato `CSV` e `Stata`.

## Input
- File grezzi in `data/raw_tabs_chords/` e `data/raw_tabs_bass/`.

## Esecuzione
1.  **Filtro Strumento**: Lo script deve poter processare `Chords` e `Bass` a seconda dell'argomento passato.
2.  **Estrattore Avanzato**:
    *   `bpm`, `main_key`, `song_structure` (elenco sezioni).
    *   **Strumming**: Variabili dedicate per sezione.
    *   **Accordi**: Conteggio occorrenze dei tag `[ch]`.
    *   **Indicatori Sintetici**: 13 metriche (es. `complexity`, `repetition`, `energy`).

3.  **Aggregazione**: Per ogni combinazione `song_name + artist_name`, mantenere una sola osservazione aggregando le versioni disponibili con la logica deterministica già implementata.

4.  Eseguire lo script di elaborazione:
    ```bash
    python3 execution/step2_digitalize/create_dataset.py Chords
    python3 execution/step2_digitalize/create_dataset.py Bass
    ```

## Output
- `data/processed_datasets/top100YearEnd8525/dataset_chords_top100yearend.csv`
- `data/processed_datasets/top100YearEnd8525/dataset_chords_top100yearend.dta`
- `data/processed_datasets/top100YearEnd8525/dataset_bass_top100yearend.csv`
- `data/processed_datasets/top100YearEnd8525/dataset_bass_top100yearend.dta`
- `data/processed_datasets/top100YearEnd8525/dataset_top100yearend.csv`
- `data/processed_datasets/top100YearEnd8525/dataset_top100yearend.dta`

## Casi Limite & Note
- **Compatibilità**: Mantenere la logica di fallback per variabili missing se presenti versioni multiple (facoltativo se abbiamo scaricato solo il best).
- **Path**: I `csv` e `dta` finali non devono essere più creati direttamente in `data/processed_datasets/`, ma solo dentro `data/processed_datasets/top100YearEnd8525/`.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step2_digitalize/create_dataset.py Chords`
2. `python3 execution/step2_digitalize/create_dataset.py Bass`
