# Direttiva 04: Costruzione Dataset Artisti Country USA

## Obiettivo
Costruire un dataset storico a livello di persona di artisti country nati negli Stati Uniti, con output finali in `CSV` e `Stata`.

## Input
- Direttive in `directives/costruzione dataset artisti country/`
- Schema in `directives/costruzione dataset artisti country/country_artists_schema.csv`
- Fonti esterne nell'ordine richiesto:
  - Country Music Hall of Fame
  - Wikipedia categorie/liste
  - Wikidata
  - MusicBrainz

## Esecuzione
1. Raccogliere i seed iniziali da Hall of Fame e pagine/categorie Wikipedia country USA.
2. Risolvere i candidati in entità Wikidata e ampliare il perimetro con query sui generi country-correlati.
3. Estrarre metadati biografici, musicali, geografici e identificativi da Wikidata.
4. Verificare una quota deterministica di record con MusicBrainz per alias e identificazione artistica.
5. Normalizzare generi, strumenti, geografia USA e flag di campionamento.
6. Deduplicare nell'ordine: `wikidata_qid`, `musicbrainz_mbid`, `name + birth_date + birth_place_raw`.
7. Esportare tutti gli output finali in `data/processed_datasets/country_artists/` in `csv` e `dta`.

## Output
- `data/processed_datasets/country_artists/country_artists_master.csv`
- `data/processed_datasets/country_artists/country_artists_master.dta`
- `data/processed_datasets/country_artists/country_artists_restricted.csv`
- `data/processed_datasets/country_artists/country_artists_restricted.dta`
- `data/processed_datasets/country_artists/country_artists_expanded.csv`
- `data/processed_datasets/country_artists/country_artists_expanded.dta`
- `data/processed_datasets/country_artists/country_artists_excluded_or_non_us.csv`
- `data/processed_datasets/country_artists/country_artists_excluded_or_non_us.dta`
- `data/processed_datasets/country_artists/country_artists_sources.csv`
- `data/processed_datasets/country_artists/country_artists_sources.dta`
- `data/processed_datasets/country_artists/country_artists_data_dictionary.csv`
- `data/processed_datasets/country_artists/country_artists_data_dictionary.dta`
- `data/processed_datasets/country_artists/manual_review_queue.csv`
- `data/processed_datasets/country_artists/manual_review_queue.dta`
- `data/processed_datasets/country_artists/country_artists_qc_report.md`
- File intermedi e cache in `data/processed_datasets/country_artists/intermediate/`

## Casi Limite & Note
- Non inferire mai `birth_state` da residenza, carriera o luogo di morte.
- Gli artisti non nati negli USA vanno esclusi dal master e salvati nel file dedicato agli esclusi.
- I seed Wikipedia non risolti in modo univoco devono andare in `manual_review_queue.csv`.
- Il campione ristretto deve essere sempre un sottoinsieme di quello allargato.

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/step4_country_artists/build_country_artists_dataset.py > log/build_country_artists.log`
