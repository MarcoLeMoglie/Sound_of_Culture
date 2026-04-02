# Istruzioni Agente - Sound of Culture

## Obiettivi del Progetto

Costruire un dataset di canzoni degli ultimi 40 anni a partire dalle tablature (basso, chitarra, batteria, piano).

1. **Step 1**: Sviluppare un algoritmo per il download in bulk delle tablature.
2. **Step 2**: Digitalizzare le informazioni delle tablature per costruire un dataset (Canzone - Data di Uscita) in formato CSV e Stata `.dta`.
3. **Step 3**: Analisi del dataset con Stata.

Operi all'interno di un'architettura a 3 livelli che separa le responsabilità per massimizzare l'affidabilità. Gli LLM sono probabilistici, mentre la maggior parte della logica di business è deterministica e richiede coerenza. Questo sistema risolve il problema.

## Architettura a 3 Livelli

**Livello 1: Direttiva (Cosa fare)**

- Fondamentalmente SOP scritte in Markdown, che vivono in `directives/`
- Definiscono gli obiettivi, gli input, i tool/script da usare, gli output e i casi limite
- Istruzioni in linguaggio naturale, come le daresti a un dipendente di medio livello

**Livello 2: Orchestrazione (Decisioni)**

- Il tuo lavoro: routing intelligente.
- Leggi le direttive, chiama gli strumenti di esecuzione nell'ordine giusto, gestisci gli errori, chiedi chiarimenti, aggiorna le direttive con ciò che impari
- Sei il collante tra intenzione ed esecuzione. Per esempio, non provi a fare scraping di siti web tu stesso—leggi `directives/scrape_website.md` e definisci input/output e poi esegui `execution/scrape_single_site.py`

**Livello 3: Esecuzione (Fare il lavoro)**

- Script Python deterministici in `execution/`
- Variabili d'ambiente, token API, ecc sono salvati in `.env`
- Gestiscono chiamate API, elaborazione dati, operazioni su file, interazioni con database
- Affidabili, testabili, veloci. Usa script invece di lavoro manuale. Ben commentati.

**Perché funziona:** se fai tutto tu stesso, gli errori si sommano. 90% di accuratezza per step = 59% di successo su 5 step. La soluzione è spingere la complessità in codice deterministico. Così tu ti concentri solo sul decision-making.

## Principi Operativi

**1. Controlla prima i tool esistenti**
Prima di scrivere uno script, controlla `execution/` secondo la tua direttiva. Crea nuovi script solo se non ne esistono.

**2. Auto-correggiti quando qualcosa si rompe**

- Leggi il messaggio di errore e lo stack trace
- Correggi lo script e testalo di nuovo (a meno che non usi token/crediti a pagamento—in quel caso chiedi prima all'utente)
- Aggiorna la direttiva con ciò che hai imparato (limiti API, timing, casi limite)
- Esempio: hai un rate limit API → allora guardi nell'API → trovi un batch endpoint che risolverebbe → riscrivi lo script per adattarlo → testi → aggiorna la direttiva.

**3. Aggiorna le direttive mentre impari**
Le direttive sono documenti vivi. Quando scopri vincoli API, approcci migliori, errori comuni o aspettative di timing—aggiorna la direttiva. Ma non creare o sovrascrivere direttive senza chiedere, a meno che non ti venga esplicitamente detto. Le direttive sono il tuo set di istruzioni e devono essere preservate (e migliorate nel tempo, non usate estemporaneamente e poi scartate).

**5. Stabilità e Gestione delle Cartelle**
Per prevenire blocchi o hang dei comandi `run_command` (specialmente con `mkdir`), utilizza sempre la creazione implicita delle cartelle tramite il tool `write_to_file`. Invece di creare una directory esplicitamente, scrivi un file (anche vuoto) nel percorso di destinazione desiderato. Questa regola deve essere seguita in tutte le sessioni di questo progetto per massimizzare la stabilità dell'architettura.

## Loop di auto-correzione

Gli errori sono opportunità di apprendimento. Quando qualcosa si rompe:

1. Correggilo
2. Aggiorna il tool
3. Testa il tool, assicurati che funzioni
4. Aggiorna la direttiva per includere il nuovo flusso
5. Il sistema ora è più forte

## Organizzazione File

**Deliverable vs Intermedi:**

- **Deliverable**: file .dta di Stata e do file di Stata, pdf, tex file
- **Intermedi**: File temporanei necessari durante l'elaborazione

**Struttura directory:**

<!-- STRUCTURE_START -->
```text
├── .coldstart_country_artists_stata_v4
│   ├── data
│   └── run_full_replication.log
├── .gitignore
├── Gemini.md
├── backup.sh
├── data
│   ├── billboard_country_artists.json
│   ├── casualties_uswars
│   ├── discovery_progress_artists.json
│   ├── discovery_test_americana.json
│   ├── download_log.txt
│   ├── input_songs.json
│   ├── input_songs_bass.json
│   ├── input_songs_bulk.json
│   ├── input_songs_bulk_bass.json
│   ├── input_songs_bulk_country.json
│   ├── input_songs_bulk_country_expansion.json
│   ├── processed_datasets
│   ├── raw_tabs_bass
│   ├── raw_tabs_chords
│   ├── raw_tabs_country
│   └── seed_country_artists.json
├── debug_output.log
├── directives
│   ├── 01_download_tablatures.md
│   ├── 02_digitalize_tablatures.md
│   ├── 03_analyze_data.md
│   ├── 04_build_country_artists_dataset.md
│   ├── 05_replication_package.md
│   ├── MIGRATE_GITHUB.md
│   └── costruzione dataset artisti country
├── discovery_master_log.txt
├── download_country.log
├── download_expansion.log
├── error.log
├── error_full.log
├── execution
│   ├── step1_download
│   ├── step2_digitalize
│   ├── step3_analysis
│   ├── step4_country_artists
│   └── step5_replication
├── fuzzy_error.log
├── fuzzy_resumed_stderr.log
├── fuzzy_stderr.log
├── release_enrichment.log
├── release_enrichment_discogs.log
├── release_enrichment_fuzzy.log
├── release_enrichment_wiki.log
├── release_enrichment_wiki_recovery.log
├── release_enrichment_wiki_recovery2.log
├── run_full_replication.log
└── visualize_structure.py
```
<!-- STRUCTURE_END -->


- `.tmp/` - File intermedi e download temporanei. Mai committare.
- `directives/` - SOP in Markdown per ogni step del progetto.
- `execution/` - Script Python e Stata (`.do`) divisi per step:
  - `step1_download/`
  - `step2_digitalize/`
  - `step3_analysis/`
  - `step4_country_artists/`
  - `step5_replication/`
- `data/` - Archivio dati:
  - `raw_tabs/` - Tablature scaricate
  - `processed_datasets/` - Datasets CSV e DTA
- `.env` - Variabili d'ambiente e chiavi API.


**Principio chiave:** I file locali sono solo per l'elaborazione. I deliverable vivono nei servizi cloud (Google Sheets, Slides, ecc.) dove l'utente può accedervi. Tutto in `.tmp/` può essere cancellato e rigenerato.

## Riepilogo

Ti posizioni tra intenzione umana (direttive) ed esecuzione deterministica (script Python). Leggi le istruzioni, prendi decisioni, chiama i tool, gestisci gli errori, migliora continuamente il sistema.

Sii pragmatico. Sii affidabile. Auto-correggiti.
------------------------------------------------

## Linee Guida Generali del progetto

### 1. Protocollo di Autosave e Persistenza

- **Regola Mandatoria [TUTTI GLI AGENTI]**: Ogni agente operante su questo progetto DEVE assicurarsi che lo stato della conversazione, il workspace e i progressi siano salvati (tramite update degli artefatti e check della memoria) dopo ogni singola iterazione o interazione significativa.
- **Metodo**: Aggiornamento costante degli artefatti (`task.md`, `walkthrough.md`, `implementation_plan.md`) e creazione di Knowledge Items (KI) per catturare la memoria del progetto.
- **Check di Persistenza**: All'inizio di ogni nuova sessione, l'agente deve verificare lo stato di `task.md` per riprendere dal punto esatto in cui si è interrotto.
- **Backup GitHub**: Ogni volta che modifichi qualcosa della cartella progetto salverai la nuova versione su GitHub MarcoLeMoglie di cui ti ho fornito l'API

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step5_replication/run_full_replication.py`
2. `python3 execution/step4_country_artists/build_country_artists_dataset.py`
