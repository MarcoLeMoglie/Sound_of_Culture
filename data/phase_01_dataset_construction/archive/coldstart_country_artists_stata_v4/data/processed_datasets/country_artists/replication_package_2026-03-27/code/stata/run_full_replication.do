version 17
clear all
set more off

args rebuild_mode

local project_root "`c(pwd)'"
local package_date "2026-03-27"
local python_launcher "data/processed_datasets/country_artists/replication_package_2026-03-27/code/python/step5_replication/run_full_replication.py"
local full_rebuild 0

if inlist("`rebuild_mode'", "full", "full_rebuild", "--full-rebuild") {
    local full_rebuild 1
}

display as text "Project root: `project_root'"
display as text "Replication package date: `package_date'"
display as text "Full rebuild mode: `full_rebuild'"

if `full_rebuild' {
    python script `python_launcher', args("--full-rebuild")
}
else {
    python script `python_launcher'
}
