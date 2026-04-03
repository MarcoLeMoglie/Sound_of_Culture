# Direttiva 03: Analisi del Dataset con Stata

## Obiettivo
Eseguire analisi statistiche ed econometriche sul dataset creato utilizzando Stata.

## Input
- `data/processed_datasets/dataset.dta` (Tablature digitalizzate in formato Stata)

## Esecuzione
1. **Dati**: Pulizia e aggregazione finale (se necessaria).
2. **Statistiche**: Analisi descrittive (distribuzione per anno, genere, strumento).
3. **Comandi**: Eseguire i file `.do` di Stata posizionati nella cartella dedicata.
   ```bash
   /Applications/Stata/StataMP.app/Contents/MacOS/stata-mp -b do execution/step3_analysis/do/eda.do
   ```

## Struttura File - `execution/step3_analysis/`
*   `do/` - Contiene tutti i file `.do` di Stata (es. `eda.do`, `eda_expanded.do`).
*   `log/` - File di log generati dall'esecuzione di Stata (es. `eda.log`).
*   `output_figures/` - Grafici esportati (in formato `.pdf`, `.eps` o `.tex`).

## Casi Limite & Note
- **Missing Values**: Gestione accurata dei missing su variabili musicali e BPM.
- **Path Assoolute**: Assicurarsi che nel file `.do` la directory di lavoro (`cd`) punti alla root del progetto.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `nessuno`
