# Direttiva 01: Download Tablature

## Obiettivo
Sviluppare un algoritmo per il download in bulk delle tablature (basso, chitarra, batteria, piano) per le canzoni degli ultimi 40 anni.

## Input
- Lista di canzoni o sorgenti web (es. Ultimate Guitar, Songsterr, ecc.) - *Da definire*

## Esecuzione
- Lo script deve iterare sulla lista ed effettuare lo scraping/download.
- Gestire rate limit e captcha.
- Salvare i file nella cartella di destinazione.

## Output
- File di testo o PDF delle tablature salvati in `data/raw_tabs/`.

## Casi Limite & Note
- Tablature mancanti.
- Errori di connessione o blocchi IP.
