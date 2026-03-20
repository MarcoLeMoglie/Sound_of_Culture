# Direttiva 01: Download Tablature

## Obiettivo
Sviluppare un algoritmo per il download in bulk delle tablature (basso, chitarra, batteria, piano) per le canzoni degli ultimi 40 anni.

## Input
- File JSON in `data/input_songs.json` con la lista di canzoni/artisti da cercare.
  Formato: `[{"query": "Nome Artista/Canzone", "type": "Chords", "limit": 5}]`

## Esecuzione
1. Creare/Modificare `data/input_songs.json` con le query desiderate.
2. Eseguire lo script orchestratore:
   ```bash
   python3 execution/step1_download/main_scrape.py
   ```
- Lo script effettua la ricerca, scarica i dettagli (incluso il testo) e salva in `data/raw_tabs/` in formato JSON.
- Gestisce il rate limit con pause casuali.

## Output
- File JSON delle tablature salvati in `data/raw_tabs/` (es. `Oasis_Wonderwall_2223387.json`).

## Casi Limite & Note
- **Date**: La data nel JSON è quella di upload della tab. La data di uscita della canzone va integrata in seguito.
- **Blocchi IP**: Gestiti parzialmente dalle pause.

