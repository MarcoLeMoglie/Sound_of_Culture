version 17
clear all
set more off

local project_root "`c(pwd)'"
local package_dirname "replication_package_country_artist_universe_2026_04_08"
local python_launcher "data/phase_01_dataset_construction/processed/country_artists/`package_dirname'/code/python/phase_01_dataset_construction/run_artist_universe_replication.py"

display as text "Project root: `project_root'"
display as text "Replication package folder: `package_dirname'"

python script `python_launcher'
