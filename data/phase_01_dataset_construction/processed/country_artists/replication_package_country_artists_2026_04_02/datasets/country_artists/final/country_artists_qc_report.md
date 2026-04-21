# country_artists_qc_report

## Summary

- numero totale candidati raccolti: 3452
- numero totale artisti nel master: 1556
- numero nel campione ristretto: 1331
- numero nel campione allargato: 1556
- numero esclusi/non-US: 964
- numero casi in manual review: 834
- quota di record con birth_state valorizzato: 100.0%
- quota di record con birth_date valorizzata: 99.4%
- quota di record con Wikidata QID: 100.0%
- quota di record con MBID: 85.8%

## Principali motivi di esclusione

- unresolved_birth_country: 479
- missing_key_birth_metadata: 454
- insufficient_country_evidence: 18
- non_us_birth: 13

## Principali conflitti o note

- low country confidence: 173
- birth geography inferred from Wikipedia categories: 95
- birth data enriched from MusicBrainz: 39

## Pipeline

1. raccolta seed da Country Music Hall of Fame e pagine/categorie Wikipedia
2. espansione candidati con query Wikidata sui generi country-correlati
3. enrichment strutturato con Wikidata
4. verifica secondaria limitata con MusicBrainz
5. normalizzazione, deduplica, campionamento e export CSV/DTA
