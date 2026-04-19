# Checklist operativa — dataset country artists USA

## A. Setup iniziale
- [ ] Creare cartella progetto
- [ ] Definire output folder
- [ ] Definire file di log
- [ ] Definire schema finale colonne
- [ ] Definire valori ammessi per confidence
- [ ] Definire mapping dei generi normalizzati
- [ ] Definire mapping delle macro-regioni USA

## B. Candidate generation
- [ ] Raccogliere seed da Country Music Hall of Fame
- [ ] Raccogliere candidate list da categorie/liste Wikipedia country USA
- [ ] Escludere subito band e gruppi
- [ ] Creare tabella candidati grezzi con source_seed

## C. Backbone e enrichment
- [ ] Interrogare Wikidata per candidati
- [ ] Recuperare QID, date, place of birth, occupation, genre, identifiers
- [ ] Interrogare MusicBrainz per verifica identità e alias
- [ ] Recuperare MBID, alias, artist metadata, begin date se utile
- [ ] Integrare eventuali authority IDs

## D. Geocoding amministrativo del luogo di nascita
- [ ] Estrarre città/contea/luogo raw
- [ ] Risalire allo stato USA quando possibile
- [ ] Popolare birth_state e birth_state_abbr
- [ ] Non inferire lo stato da segnali indiretti
- [ ] Marcare manual_review_needed se lo stato non è certo

## E. Filtri principali
- [ ] Tenere solo persone fisiche
- [ ] Tenere solo artisti nati negli USA nel dataset principale
- [ ] Spostare i non-US nel file esclusi
- [ ] Filtrare i casi non sufficientemente country dal ristretto
- [ ] Tenere i borderline nell’allargato con confidence adeguata

## F. Deduplica
- [ ] Deduplica per Wikidata QID
- [ ] Deduplica per MBID
- [ ] Deduplica conservativa per nome + nascita + luogo + alias
- [ ] Verificare omonimi
- [ ] Salvare log delle fusioni

## G. Normalizzazione
- [ ] Normalizzare generi
- [ ] Normalizzare strumenti
- [ ] Normalizzare occupations
- [ ] Normalizzare formati data
- [ ] Calcolare birth_year e birth_decade
- [ ] Calcolare is_deceased
- [ ] Calcolare age_or_age_at_death
- [ ] Calcolare us_macro_region

## H. Campionamento
- [ ] Definire criteri restricted
- [ ] Definire criteri expanded
- [ ] Valorizzare flag_restricted_sample
- [ ] Valorizzare flag_expanded_sample
- [ ] Valorizzare sample_membership
- [ ] Valorizzare inclusion_reason
- [ ] Valorizzare exclusion_reason

## I. Qualità
- [ ] Compilare source_primary
- [ ] Compilare source_secondary
- [ ] Compilare source_seed
- [ ] Compilare evidence_urls
- [ ] Calcolare source_count
- [ ] Attribuire birth_date_confidence
- [ ] Attribuire birth_place_confidence
- [ ] Attribuire genre_confidence
- [ ] Popolare notes
- [ ] Popolare manual_review_needed

## J. Output finali
- [ ] Scrivere country_artists_master.csv
- [ ] Scrivere country_artists_restricted.csv
- [ ] Scrivere country_artists_expanded.csv
- [ ] Scrivere country_artists_excluded_or_non_us.csv
- [ ] Scrivere country_artists_sources.csv
- [ ] Scrivere country_artists_data_dictionary.csv
- [ ] Scrivere manual_review_queue.csv
- [ ] Scrivere country_artists_qc_report.md

## K. Controlli finali
- [ ] Nessun duplicato residuo per QID/MBID
- [ ] Una riga per artista
- [ ] Encoding UTF-8
- [ ] Colonne coerenti tra file
- [ ] Nessun restricted con expanded = 0
- [ ] Nessun record senza nome principale
- [ ] Nessun uso di stato di nascita inferito in modo improprio
- [ ] QC report compilato

## Python Utilizzati Nell'Ultima Esecuzione Completata
1. `python3 execution/phase_01_dataset_construction/build_country_artists_dataset.py`
