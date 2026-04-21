version 17
clear all
set more off

display as text "Running country_songs replication package..."
do "data/phase_01_dataset_construction/processed/country_artists/replication_package_country_songs_2026_04_01/code/stata/run_full_replication.do"

display as text "Running country_artists replication package..."
do "data/phase_01_dataset_construction/processed/country_artists/replication_package_country_artists_2026_04_02/code/stata/run_full_replication.do"

display as text "Running country merge-to-v6 replication package..."
do "data/phase_01_dataset_construction/processed/country_artists/replication_package_country_merge_v6_2026_04_03/code/stata/run_full_replication.do"

display as text "All three country replication packages completed successfully."
