# Direttiva 02: Digitalizzazione Tablature

## Obiettivo
Estrarre ed elaborare le informazioni contenute nelle tablature per costruire un dataset strutturato.

## Input
- File grezzi in `data/raw_tabs/`.

## Esecuzione
1.  **Filtro Chords**: Leggere e processare esclusivamente i file `type == "Chords"`.
2.  **Estrattore Avanzato**:
    *   `bpm`, `main_key`, `song_structure` (elenco sezioni).
    *   **Strumming**: Variabili dedicate per sezione (es. `strumming_intro_details`).
    *   **Accordi**: Conteggio occorrenze dei tag `[ch]`.
    *   **Indicatori Sintetici**: 13 metriche (es. `complexity`, `repetition`, `energy`) basate su formule di scaling raw (senza limitazione a 1.0).

3.  **Aggregazione Media e Backfill**:
    *   Considera i **Primi 5** file ordinati per **Voti** (Decrescente).
    *   **Indicatori Sintetici**: Calcola la **Media** dei valori dei 5 file per compensare discrepanze.
    *   **Tutte le Altre Variabili (Numeriche e Stringa)**: Recupera il **primo valore non missing** scorrendo i file in ordine di **Rating** (dal più alto al più basso).


4.  Eseguire lo script di elaborazione:
    ```bash
    python3 execution/step2_digitalize/create_dataset.py
    ```

## Output
- `data/processed_datasets/dataset.csv`
- `data/processed_datasets/dataset.dta` (Formato Stata)

## Casi Limite & Note
- **Testi delle Canzoni**: Al momento sono **esclusi** (saranno integrati da altre fonti).
- **Tipizzazione per Stata**: Prima di esportare in DTA, forzare il cast a stringa delle colonne object/mixed per evitare errori di esportazione.

