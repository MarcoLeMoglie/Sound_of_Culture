# readme_artisti

## Obiettivo

Questo progetto serve a costruire un dataset storico dei **cantanti country degli Stati Uniti**, dalle origini del genere fino a oggi, a livello di **persona** e non di band.

Il dataset deve essere abbastanza strutturato da supportare:
- analisi storiche nel tempo
- analisi geografiche per stato di nascita
- costruzione di campioni a diversa severità
- visualizzazioni statiche e animate
- controlli di qualità e replicabilità

## Output attesi

Il workflow deve produrre almeno questi file:

1. `country_artists_master.csv`
2. `country_artists_restricted.csv`
3. `country_artists_expanded.csv`
4. `country_artists_excluded_or_non_us.csv`
5. `country_artists_sources.csv`
6. `country_artists_data_dictionary.csv`
7. `country_artists_qc_report.md`
8. `manual_review_queue.csv`

In aggiunta, questa cartella include:
- `PROMPT_ANTIGRAVITY.txt`
- `CHECKLIST_ANTIGRAVITY.md`
- `country_artists_schema.csv`

## Definizione dei due campioni

### Campione ristretto
Campione più pulito e affidabile, target circa **1200–2000 artisti**.

Include artisti che:
- sono persone fisiche
- sono nati negli Stati Uniti
- sono chiaramente singer / singer-songwriter / vocal performer country
- hanno forte evidenza di appartenenza al country
- hanno metadati biografici sufficientemente affidabili
- sono ben disambiguati

### Campione allargato
Campione più inclusivo, target **3000+ artisti/candidati**.

Include artisti che:
- sono persone fisiche
- sono nati negli Stati Uniti
- sono rilevanti per la country music USA anche se crossover o borderline
- possono avere dati più incompleti
- devono comunque avere identità ragionevolmente chiara

## Regole delle flag

Ogni artista deve avere:
- `flag_restricted_sample`
- `flag_expanded_sample`
- `sample_membership`

Regole:
- se un artista è nel campione ristretto:
  - `flag_restricted_sample = 1`
  - `flag_expanded_sample = 1`
  - `sample_membership = restricted`

- se un artista è solo nel campione allargato:
  - `flag_restricted_sample = 0`
  - `flag_expanded_sample = 1`
  - `sample_membership = expanded_only`

Il campione ristretto è sempre un sottoinsieme del campione allargato.

## Unità statistica

L’unità statistica è la **PERSONA**.

Non devono comparire come righe del dataset principale:
- band
- gruppi
- duo
- family acts
- entità collettive

Se un gruppo è rilevante, si possono recuperare i singoli membri come persone separate, se coerenti con il perimetro country.

## Perimetro geografico

Il dataset principale deve contenere solo artisti **nati negli Stati Uniti**.

Gli artisti country importanti ma **non nati negli USA** devono essere salvati in:
- `country_artists_excluded_or_non_us.csv`

e devono avere una motivazione in:
- `exclusion_reason`

## Fonti consigliate e priorità

Ordine di priorità delle fonti:
1. **Wikidata** — backbone principale
2. **MusicBrainz** — verifica e integrazione
3. **Wikipedia categorie/liste** — discovery layer
4. **Country Music Hall of Fame** — seed storico di alta qualità
5. **VIAF / LOC** — disambiguazione finale

### Uso pratico delle fonti

**Wikidata**
- fonte primaria per data di nascita
- fonte primaria per luogo di nascita
- fonte primaria per occupation, genre, identifiers

**MusicBrainz**
- verifica alias
- verifica identità artistica
- supporto a metadata musicali
- supporto a deduplica

**Wikipedia**
- utile per candidate generation
- non deve essere l’unica fonte per dati critici quando esiste una fonte strutturata migliore

**Country Music Hall of Fame**
- eccellente seed per il nucleo storico del genere

**VIAF / LOC**
- utili per casi ambigui, omonimie, authority control

## Colonne consigliate

### Identità
- `artist_id`
- `name_primary`
- `birth_name`
- `stage_name`
- `aliases`
- `wikidata_qid`
- `musicbrainz_mbid`
- `isni`
- `viaf_id`
- `wikipedia_url`

### Biografia
- `birth_date`
- `birth_year`
- `birth_place_raw`
- `birth_city`
- `birth_county`
- `birth_state`
- `birth_state_abbr`
- `birth_country`
- `death_date`
- `death_year`
- `death_place_raw`
- `citizenship`

### Musica / carriera
- `occupations`
- `genres_raw`
- `genres_normalized`
- `country_relevance_score`
- `instruments`
- `member_of`
- `record_labels`
- `awards`
- `official_website`

### Derivate
- `birth_decade`
- `us_macro_region`
- `is_deceased`
- `age_or_age_at_death`
- `is_us_born`
- `is_solo_person`
- `is_country_core`
- `is_country_broad`

### Campioni
- `flag_restricted_sample`
- `flag_expanded_sample`
- `sample_membership`
- `inclusion_reason`
- `exclusion_reason`

### Qualità e provenienza
- `source_primary`
- `source_secondary`
- `source_seed`
- `evidence_urls`
- `source_count`
- `birth_date_confidence`
- `birth_place_confidence`
- `genre_confidence`
- `manual_review_needed`
- `notes`

## Regole sulla nascita e sul luogo di nascita

- usare sempre `place of birth` come fonte primaria
- se il luogo è una città o contea, risalire allo stato federato USA
- non inferire mai lo stato di nascita da:
  - residenza
  - luogo di attività
  - etichetta discografica
  - luogo di morte
  - testo biografico non verificato
- se lo stato non è ricostruibile con sufficiente sicurezza:
  - lasciare vuoto `birth_state`
  - impostare `manual_review_needed = 1`
  - spiegare il problema in `notes`

## Regole sui generi

Mantenere sempre:
- una colonna originale `genres_raw`
- una colonna normalizzata `genres_normalized`

Normalizzare almeno in queste classi:
- `country`
- `country pop`
- `country rock`
- `outlaw country`
- `bluegrass`
- `americana`
- `western swing`
- `honky-tonk`
- `alt-country`
- `bro-country`
- `country gospel`

Inoltre creare:
- `is_country_core`
- `is_country_broad`

## Regole di deduplica

Deduplicare nell’ordine seguente:
1. `wikidata_qid`
2. `musicbrainz_mbid`
3. matching conservativo su:
   - nome principale
   - data di nascita
   - luogo di nascita
   - alias

Non fondere omonimi senza almeno due evidenze convergenti.

## Regole di qualità

- non inventare mai dati mancanti
- ogni dato compilato deve essere supportato da almeno una fonte tracciabile
- mantenere sempre `evidence_urls`
- mantenere sempre `source_count`
- usare solo questi livelli di confidenza:
  - `high`
  - `medium`
  - `low`

Se due fonti confliggono:
1. preferire Wikidata per data e luogo di nascita
2. usare MusicBrainz per alias e identità artistica
3. documentare il conflitto in `notes`

## Regole sugli strumenti

Estrarre strumenti solo se supportati da fonti.

Normalizzare almeno:
- `voice`
- `vocals`
- `guitar`
- `acoustic guitar`
- `electric guitar`
- `banjo`
- `fiddle`
- `violin`
- `mandolin`
- `piano`
- `pedal steel guitar`
- `dobro`

Non inferire strumenti non esplicitati.

## File presenti in questa cartella

### `PROMPT_ANTIGRAVITY.txt`
Prompt lungo e completo da dare direttamente all’agente.

### `CHECKLIST_ANTIGRAVITY.md`
Checklist operativa per verificare che la pipeline sia stata eseguita correttamente.

### `country_artists_schema.csv`
Dizionario minimo delle colonne con tipo dati, obbligatorietà, valori ammessi e descrizione.

## Controlli finali raccomandati

Prima di considerare finito il progetto, verificare che:
- non ci siano duplicati residui per QID o MBID
- ci sia una sola riga per artista
- il dataset sia in CSV UTF-8
- il campione ristretto sia sempre incluso nell’allargato
- nessun record sia privo di `name_primary`
- lo stato di nascita non sia stato inferito in modo improprio
- il file `manual_review_queue.csv` esista e contenga i casi dubbi
- il QC report documenti numeri, conflitti e copertura

## Uso consigliato

Ordine pratico consigliato:
1. leggere `readme_artisti.md`
2. eseguire `PROMPT_ANTIGRAVITY.txt`
3. verificare esecuzione con `CHECKLIST_ANTIGRAVITY.md`
4. validare struttura dati con `country_artists_schema.csv`
5. leggere `country_artists_qc_report.md`

## Nota finale

Questo progetto è pensato per ottenere un dataset:
- storicamente ampio
- geograficamente analizzabile
- riproducibile
- con tracciabilità delle fonti
- con separazione chiara tra campione ristretto e campione allargato

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/phase_01_dataset_construction/build_country_artists_dataset.py`
