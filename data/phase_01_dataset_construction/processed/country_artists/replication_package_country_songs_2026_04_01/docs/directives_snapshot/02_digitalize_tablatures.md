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

3.  **Consolidamento Release Year**:
    *   Eseguire lo script di consolidamento avanzato per arricchire il dataset con le date di rilascio recuperate da MusicBrainz, Wikipedia e Web Search.
    *   **Logica a 3 Livelli**:
        1.  **Exact Match**: Corrispondenza perfetta Artist/Song string.
        2.  **Super-Normalization**: Rimozione punteggiatura e junk words (es. "chords", "tab").
        3.  **Fuzzy Match (90%)**: Fallback con `difflib` limitato allo stesso artista normalizzato per evitare falsi positivi.

4.  Eseguire lo script di elaborazione:
    ```bash
    python3 execution/step2_digitalize/create_dataset.py Chords
    python3 execution/step2_digitalize/final_consolidate_internet.py
    ```

## Output
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v5.csv`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v5.dta`

## Casi Limite & Note
- **Soglia Fuzzy**: Mantenere il cutoff al **90%** per mantenere l'integrità accademica.
- **Compatibilità Stata**: Troncamento automatico delle stringhe a 244 caratteri implementato in `final_consolidate_internet.py`.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step2_digitalize/create_dataset.py Chords`
2. `python3 execution/step2_digitalize/final_consolidate_internet.py`
