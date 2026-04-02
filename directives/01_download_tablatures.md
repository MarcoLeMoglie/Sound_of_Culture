# Direttiva 01: Download Tablature

## Obiettivo
Sviluppare un algoritmo per il download in bulk delle **migliori versioni Chord** (chitarra) per canzoni identificate tramite tag **Country** o **Americana**.

## Input
- Tag di ricerca: `Country` (ID 49), `Americana` (Subgenre ID 72).
- Tipologia: Solo **Chords** (no basso, no drums, no tabs).
- Selezione: Solo la **migliore versione** per ogni canzone in termini di **Rating**.

## Esecuzione
1. **Discovery**: Identificare gli ID delle tablature filtrando per genere e tipo chords. Utilizzare una griglia di query (lettere/numeri) per massimizzare la copertura del genere.
2. **Selezione Best**: Per ogni combinazione artist-song, mantenere solo l'ID con il rating più alto.
3. **Download**: Eseguire lo script orchestratore per scaricare i JSON dettagliati in `data/raw_tabs_country/`.
   ```bash
   python3 execution/step1_download/main_scrape_best_chords.py
   ```

## Output
- File JSON salvati in `data/raw_tabs_country/` (es. `Oasis_Wonderwall_2223387.json`).

## Casi Limite & Note
- **Paginazione**: L'API potrebbe limitare a 100 pagine; utilizzare query grid per bypassare il limite.
- **Rating**: In caso di parità di rating, preferire quella con più voti.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step1_download/scraper_client.py`
2. `python3 execution/step1_download/discover_genre_best_chords.py`
3. `python3 execution/step1_download/main_scrape_best_chords.py`
