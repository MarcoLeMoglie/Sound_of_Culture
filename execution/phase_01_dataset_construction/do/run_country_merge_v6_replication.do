version 17
clear all
set more off

args rebuild_mode

local project_root "`c(pwd)'"
local package_dirname "replication_package_country_merge_v6_2026_04_03"
local python_launcher "data/phase_01_dataset_construction/processed/country_artists/`package_dirname'/code/python/phase_01_dataset_construction/run_country_merge_v6_replication.py"
local full_rebuild 0

if inlist("`rebuild_mode'", "full", "full_rebuild", "--full-rebuild") {
    local full_rebuild 1
}

display as text "Project root: `project_root'"
display as text "Replication package folder: `package_dirname'"
display as text "Full rebuild mode: `full_rebuild'"

if `full_rebuild' {
    python script `python_launcher', args("--full-rebuild")
}
else {
    python script `python_launcher'
}
