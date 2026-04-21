# Replication Package - Country Songs

Questo package replica in modo deterministico gli output `country_songs` necessari alle fasi successive della pipeline `country_artists`.

## Scopo

- ripristinare gli output canonici `Sound_of_Culture_Country_Full*`
- ripristinare le cache storiche sui `release_year`
- rendere la parte `country_songs` lanciabile da Stata anche in cold start

## Modalita'

- `standard`: se gli output mancano nella root del progetto, il wrapper li ripristina dallo snapshot bundled
- `full_rebuild`: al momento degrada deterministicamente allo stesso snapshot bundled; il rebuild raw-to-v5 originario non e' stato ri-validato end-to-end in questa fase

## Output ripristinati in root

- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full.csv`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full.dta`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched.csv`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched.dta`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v2.csv`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v2.dta`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v3.csv`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v3.dta`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v5.csv`
- `data/processed_datasets/country_artists/Sound_of_Culture_Country_Full_Enriched_v5.dta`

## Launcher Stata

- `do data/processed_datasets/country_artists/replication_package_country_songs_2026_04_01/code/stata/run_full_replication.do`

## Python Utilizzati Nell'Ultima Esecuzione Completata

1. `python3 execution/phase_01_dataset_construction/run_country_songs_replication.py`

Questo e' l'entrypoint canonico nel progetto ristrutturato. Il package continua a
mantenere i nomi storici interni `step5_replication` per non alterare lo snapshot.
