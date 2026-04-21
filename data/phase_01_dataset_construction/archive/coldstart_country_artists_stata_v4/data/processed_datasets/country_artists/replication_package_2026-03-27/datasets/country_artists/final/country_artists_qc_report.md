# country_artists_qc_report

## Summary

- numero totale candidati raccolti: 3452
- numero totale artisti nel master: 2170
- numero nel campione ristretto: 1814
- numero nel campione allargato: 2170
- numero esclusi/non-US: 350
- numero casi in manual review: 313
- quota di record con birth_state valorizzato: 100.0%
- quota di record con birth_date valorizzata: 98.4%
- quota di record con Wikidata QID: 100.0%
- quota di record con MBID: 65.8%

## Principali motivi di esclusione

- missing_key_birth_metadata: 290
- unresolved_birth_country: 43
- non_us_birth: 13
- insufficient_country_evidence: 4

## Principali conflitti o note

- birth geography inferred from Wikipedia categories: 335
- low country confidence: 140
- birth data enriched from MusicBrainz: 59

## Pipeline

1. raccolta seed da Country Music Hall of Fame e pagine/categorie Wikipedia
2. espansione candidati con query Wikidata sui generi country-correlati
3. enrichment strutturato con Wikidata
4. verifica secondaria limitata con MusicBrainz
5. normalizzazione, deduplica, campionamento e export CSV/DTA
