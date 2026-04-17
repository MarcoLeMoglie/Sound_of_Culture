version 17
clear all
macro drop _all
set more off

* Analisi descrittiva esplorativa del dataset country-only chords.
* Il file produce:
* 1. labeling completo delle variabili principali e derivate;
* 2. tabelle CSV replicabili;
* 3. grafici PNG per trend temporali, stile, geografia, generazioni e indici.

cd "/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture"

local data_path  "data/processed_datasets/country_artists/Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.dta"
local out_root   "execution/step3_analysis/output_country_only_chords_2026_04_08"
local graph_dir  "`out_root'/graphs"
local table_dir  "`out_root'/tables"
local log_path   "`out_root'/eda_country_only_chords_2026_04_08.log"
local analysis_start_year = 1946
local analysis_end_year = 2026

capture mkdir "`out_root'"
capture mkdir "`graph_dir'"
capture mkdir "`table_dir'"

log using "`log_path'", replace text

set scheme s1color
use "`data_path'", clear

display as text "Dataset caricato: `data_path'"
display as text "Numero osservazioni iniziali: " _N
display as text "Analisi principale limitata a release_year >= `analysis_start_year'"
display as text "Grafici annuali estesi fino al `analysis_end_year'"

capture program drop label_if_exists
program define label_if_exists
    syntax varname, Text(string asis)
    capture confirm variable `varlist'
    if !_rc {
        label variable `varlist' `"`text'"'
    }
end

* ---------------------------------------------------------------------------
* 1. Label di tutte le variabili del dataset
* ---------------------------------------------------------------------------

* Song/tab metadata.
label_if_exists song_name, text("Song title")
label_if_exists artist_name, text("Artist name as scraped from Ultimate Guitar")
label_if_exists id, text("Ultimate Guitar internal song identifier")
label_if_exists type, text("Ultimate Guitar tab type")
label_if_exists version, text("Ultimate Guitar version number")
label_if_exists votes, text("Number of Ultimate Guitar votes")
label_if_exists rating, text("Average Ultimate Guitar rating")
label_if_exists difficulty, text("Ultimate Guitar difficulty label")
label_if_exists tuning, text("Instrument tuning reported in tab")
label_if_exists capo, text("Capo position reported in tab")
label_if_exists url_web, text("Ultimate Guitar source URL")
label_if_exists upload_date, text("Ultimate Guitar upload date")
label_if_exists upload_year, text("Ultimate Guitar upload year")
label_if_exists genre, text("Song genre")
label_if_exists main_key, text("Main key detected from tab")
label_if_exists bpm, text("Beats per minute")
label_if_exists song_structure, text("Song section structure")
label_if_exists has_intro, text("Indicator for intro section")
label_if_exists has_verse, text("Indicator for verse section")
label_if_exists has_chorus, text("Indicator for chorus section")
label_if_exists has_bridge, text("Indicator for bridge section")
label_if_exists has_outro, text("Indicator for outro section")
label_if_exists chord_1, text("Most common chord in song")
label_if_exists chord_1_count, text("Occurrences of first most common chord")
label_if_exists chord_2, text("Second most common chord in song")
label_if_exists chord_2_count, text("Occurrences of second most common chord")
label_if_exists chord_3, text("Third most common chord in song")
label_if_exists chord_3_count, text("Occurrences of third most common chord")
label_if_exists complexity, text("Chord complexity index")
label_if_exists repetition, text("Chord repetition index")
label_if_exists melodicness, text("Soft consonance ratio")
label_if_exists energy, text("Chord-switch energy index")
label_if_exists finger_movement, text("Chord movement proxy")
label_if_exists disruption, text("Harmonic disruption index")
label_if_exists root_stability, text("Consecutive same-root share")
label_if_exists intra_root_variation, text("Within-root variation index")
label_if_exists harmonic_palette, text("Unique chord palette")
label_if_exists loop_strength, text("Loop strength index")
label_if_exists structure_variation, text("Variation across sections")
label_if_exists playability, text("Ease of playing proxy")
label_if_exists harmonic_softness, text("Soft chord ratio")
label_if_exists release_year, text("Song release year")

* Artist metadata.
label_if_exists artist_id, text("Unique internal artist identifier")
label_if_exists name_primary, text("Primary display name")
label_if_exists birth_name, text("Birth or legal name")
label_if_exists stage_name, text("Stage name if distinct")
label_if_exists aliases, text("Pipe-separated aliases")
label_if_exists wikidata_qid, text("Wikidata artist identifier")
label_if_exists musicbrainz_mbid, text("MusicBrainz artist identifier")
label_if_exists isni, text("ISNI artist identifier")
label_if_exists viaf_id, text("VIAF identifier")
label_if_exists wikipedia_url, text("Wikipedia page URL")
label_if_exists birth_date, text("Birth date in ISO format")
label_if_exists birth_year, text("Birth year")
label_if_exists birth_place_raw, text("Raw birth place string")
label_if_exists birth_city, text("Birth city")
label_if_exists birth_county, text("Birth county")
label_if_exists birth_state, text("Birth state")
label_if_exists birth_state_abbr, text("Birth state abbreviation")
label_if_exists birth_country, text("Birth country")
label_if_exists death_date, text("Death date in ISO format")
label_if_exists death_year, text("Death year")
label_if_exists death_place_raw, text("Raw death place string")
label_if_exists citizenship, text("Citizenship")
label_if_exists occupations, text("Pipe-separated occupations")
label_if_exists genres_raw, text("Original source genre labels")
label_if_exists genres_normalized, text("Normalized genre labels")
label_if_exists country_relevance_score, text("Country relevance score")
label_if_exists instruments, text("Pipe-separated instruments")
label_if_exists member_of, text("Associated groups or duos")
label_if_exists record_labels, text("Pipe-separated record labels")
label_if_exists awards, text("Pipe-separated awards")
label_if_exists official_website, text("Official website URL")
label_if_exists birth_decade, text("Birth decade")
label_if_exists us_macro_region, text("US macro-region of birth")
label_if_exists is_deceased, text("Artist is deceased")
label_if_exists age_or_age_at_death, text("Current age or age at death")
label_if_exists is_us_born, text("Artist born in the United States")
label_if_exists is_solo_person, text("Row corresponds to a solo person")
label_if_exists is_country_core, text("Core country relevance flag")
label_if_exists is_country_broad, text("Broad country relevance flag")
label_if_exists flag_restricted_sample, text("Restricted sample flag")
label_if_exists flag_expanded_sample, text("Expanded sample flag")
label_if_exists sample_membership, text("Sample membership label")
label_if_exists inclusion_reason, text("Inclusion reason")
label_if_exists exclusion_reason, text("Exclusion reason")
label_if_exists source_primary, text("Primary source")
label_if_exists source_secondary, text("Secondary source")
label_if_exists source_seed, text("Seed source")
label_if_exists evidence_urls, text("Pipe-separated evidence URLs")
label_if_exists source_count, text("Number of supporting sources")
label_if_exists birth_date_confidence, text("Birth date confidence")
label_if_exists birth_place_confidence, text("Birth place confidence")
label_if_exists genre_confidence, text("Genre confidence")
label_if_exists manual_review_needed, text("Manual review needed")
label_if_exists notes, text("Free-text notes")
label_if_exists num_chord_json_used, text("Number of chord JSON files used")
label_if_exists billboard_country_year_end_flag, text("Billboard country year-end hit")
label_if_exists billboard_country_year_end_year, text("Billboard year-end chart year")
label_if_exists billboard_country_year_end_pos, text("Billboard year-end chart position")

* Value labels for core binary flags.
label define yesno_lbl 0 "No" 1 "Yes", replace
foreach var in has_intro has_verse has_chorus has_bridge has_outro ///
               is_deceased is_us_born is_solo_person is_country_core ///
               is_country_broad flag_restricted_sample flag_expanded_sample ///
               manual_review_needed billboard_country_year_end_flag {
    capture confirm variable `var'
    if !_rc {
        capture label values `var' yesno_lbl
    }
}

* ---------------------------------------------------------------------------
* 2. Pulizia minima e costruzione variabili analitiche
* ---------------------------------------------------------------------------

foreach var in votes rating capo upload_year bpm chord_1_count chord_2_count chord_3_count ///
               complexity repetition melodicness energy finger_movement disruption ///
               root_stability intra_root_variation harmonic_palette loop_strength ///
               structure_variation playability harmonic_softness release_year ///
               birth_year death_year country_relevance_score birth_decade ///
               is_deceased age_or_age_at_death is_us_born is_solo_person ///
               is_country_core is_country_broad flag_restricted_sample ///
               flag_expanded_sample source_count manual_review_needed ///
               num_chord_json_used billboard_country_year_end_flag ///
               billboard_country_year_end_year billboard_country_year_end_pos {
    capture confirm numeric variable `var'
    if _rc {
        capture destring `var', replace force
    }
}

gen long one = 1
label variable one "Unit count"

gen release_decade = floor(release_year / 10) * 10 if !missing(release_year)
label variable release_decade "Release decade"

gen upload_decade = floor(upload_year / 10) * 10 if !missing(upload_year)
label variable upload_decade "Upload decade"

gen birth_decade_analytic = birth_decade
replace birth_decade_analytic = floor(birth_year / 10) * 10 if missing(birth_decade_analytic) & !missing(birth_year)
label variable birth_decade_analytic "Analytical birth decade"

gen artist_age_at_release = release_year - birth_year if !missing(release_year) & !missing(birth_year)
label variable artist_age_at_release "Artist age at song release"

gen release_upload_gap = upload_year - release_year if !missing(upload_year) & !missing(release_year)
label variable release_upload_gap "Gap between upload year and release year"

gen byte billboard_top10 = .
replace billboard_top10 = 0 if !missing(billboard_country_year_end_flag)
replace billboard_top10 = 1 if billboard_country_year_end_flag == 1 ///
    & !missing(billboard_country_year_end_pos) ///
    & billboard_country_year_end_pos <= 10
label variable billboard_top10 "Billboard country year-end top 10"
label values billboard_top10 yesno_lbl

gen byte billboard_top20 = .
replace billboard_top20 = 0 if !missing(billboard_country_year_end_flag)
replace billboard_top20 = 1 if billboard_country_year_end_flag == 1 ///
    & !missing(billboard_country_year_end_pos) ///
    & billboard_country_year_end_pos <= 20
label variable billboard_top20 "Billboard country year-end top 20"
label values billboard_top20 yesno_lbl

gen byte generation_group = .
replace generation_group = 1 if !missing(birth_year) & birth_year <= 1927
replace generation_group = 2 if inrange(birth_year, 1928, 1945)
replace generation_group = 3 if inrange(birth_year, 1946, 1964)
replace generation_group = 4 if inrange(birth_year, 1965, 1980)
replace generation_group = 5 if inrange(birth_year, 1981, 1996)
replace generation_group = 6 if !missing(birth_year) & birth_year >= 1997
label define generation_lbl ///
    1 "Greatest/Silent" ///
    2 "Silent" ///
    3 "Boomer" ///
    4 "Gen X" ///
    5 "Millennial" ///
    6 "Gen Z+", replace
label values generation_group generation_lbl
label variable generation_group "Generation based on birth year"

gen str20 macro_region_clean = us_macro_region
replace macro_region_clean = "Missing" if trim(macro_region_clean) == ""
label variable macro_region_clean "Cleaned US macro-region"

gen str20 birth_country_clean = birth_country
replace birth_country_clean = "Missing" if trim(birth_country_clean) == ""
label variable birth_country_clean "Cleaned birth country"

gen str20 genre_clean = lower(trim(genre))
replace genre_clean = "missing" if genre_clean == ""
label variable genre_clean "Cleaned genre"

gen byte analysis_sample_country = !missing(release_year) ///
    & inrange(release_year, `analysis_start_year', `analysis_end_year') ///
    & genre_clean == "country"
label variable analysis_sample_country "Main analytical sample: country songs, 1946-2026"
label values analysis_sample_country yesno_lbl

gen str12 first_chord_group = upper(trim(chord_1))
replace first_chord_group = "MISSING" if first_chord_group == ""
gen byte first_chord_is_top = ///
    inlist(first_chord_group, "G", "C", "D", "A", "E", "F") | ///
    inlist(first_chord_group, "AM", "EM", "BB", "B")
replace first_chord_group = "OTHER" if first_chord_is_top == 0 & first_chord_group != "MISSING"
drop first_chord_is_top
label variable first_chord_group "Grouped most common chord"

gen byte genre_country = genre_clean == "country"
gen byte genre_rock = genre_clean == "rock"
gen byte genre_pop = genre_clean == "pop"
gen byte genre_folk = genre_clean == "folk"
gen byte genre_hiphop = genre_clean == "hip hop"
gen byte genre_rbsoul = genre_clean == "r&b funk & soul"
label variable genre_country "Genre is country"
label variable genre_rock "Genre is rock"
label variable genre_pop "Genre is pop"
label variable genre_folk "Genre is folk"
label variable genre_hiphop "Genre is hip hop"
label variable genre_rbsoul "Genre is R&B, funk, or soul"

gen byte chord_g = first_chord_group == "G"
gen byte chord_c = first_chord_group == "C"
gen byte chord_d = first_chord_group == "D"
gen byte chord_a = first_chord_group == "A"
gen byte chord_e = first_chord_group == "E"
gen byte chord_f = first_chord_group == "F"
label variable chord_g "First-chord group is G"
label variable chord_c "First-chord group is C"
label variable chord_d "First-chord group is D"
label variable chord_a "First-chord group is A"
label variable chord_e "First-chord group is E"
label variable chord_f "First-chord group is F"

foreach var in genre_country genre_rock genre_pop genre_folk genre_hiphop genre_rbsoul ///
               chord_g chord_c chord_d chord_a chord_e chord_f {
    label values `var' yesno_lbl
}

display as text "Osservazioni dopo cleaning: " _N
quietly summarize release_year, meanonly
display as text "Intervallo release_year: " %9.0f r(min) " - " %9.0f r(max)

* ---------------------------------------------------------------------------
* 3. Tavole di overview e copertura
* ---------------------------------------------------------------------------

quietly count
local N = r(N)

egen artist_tag_all = tag(artist_id)
quietly count if artist_tag_all == 1
local N_artists = r(N)

egen song_tag_all = tag(artist_name song_name)
quietly count if song_tag_all == 1
local N_songs = r(N)

quietly summarize release_year, meanonly
local release_min = r(min)
local release_max = r(max)

quietly summarize birth_year, meanonly
local birth_min = r(min)
local birth_max = r(max)

quietly summarize artist_age_at_release, meanonly
local mean_age_release = r(mean)

quietly summarize release_upload_gap, meanonly
local mean_release_upload_gap = r(mean)

quietly summarize complexity, meanonly
local mean_complexity = r(mean)

quietly summarize repetition, meanonly
local mean_repetition = r(mean)

quietly summarize playability, meanonly
local mean_playability = r(mean)

quietly summarize billboard_country_year_end_flag, meanonly
local share_billboard = r(mean)

preserve
clear
set obs 1
gen n_rows = `N'
gen n_unique_artists = `N_artists'
gen n_unique_artist_song_pairs = `N_songs'
gen release_year_min = `release_min'
gen release_year_max = `release_max'
gen birth_year_min = `birth_min'
gen birth_year_max = `birth_max'
gen mean_artist_age_at_release = `mean_age_release'
gen mean_release_upload_gap = `mean_release_upload_gap'
gen mean_complexity = `mean_complexity'
gen mean_repetition = `mean_repetition'
gen mean_playability = `mean_playability'
gen share_billboard_year_end = `share_billboard'
export delimited using "`table_dir'/sample_overview.csv", replace
restore

quietly count if !missing(release_year) & release_year >= `analysis_start_year'
local N_main = r(N)

egen artist_tag_main = tag(artist_id) if !missing(release_year) & release_year >= `analysis_start_year'
quietly count if artist_tag_main == 1
local N_artists_main = r(N)

egen song_tag_main = tag(artist_name song_name) if !missing(release_year) & release_year >= `analysis_start_year'
quietly count if song_tag_main == 1
local N_songs_main = r(N)

quietly summarize birth_year if !missing(release_year) & release_year >= `analysis_start_year', meanonly
local birth_min_main = r(min)
local birth_max_main = r(max)

quietly summarize artist_age_at_release if !missing(release_year) & release_year >= `analysis_start_year', meanonly
local mean_age_release_main = r(mean)

quietly summarize release_upload_gap if !missing(release_year) & release_year >= `analysis_start_year', meanonly
local mean_release_upload_gap_main = r(mean)

quietly summarize complexity if !missing(release_year) & release_year >= `analysis_start_year', meanonly
local mean_complexity_main = r(mean)

quietly summarize repetition if !missing(release_year) & release_year >= `analysis_start_year', meanonly
local mean_repetition_main = r(mean)

quietly summarize playability if !missing(release_year) & release_year >= `analysis_start_year', meanonly
local mean_playability_main = r(mean)

quietly summarize billboard_country_year_end_flag if !missing(release_year) & release_year >= `analysis_start_year', meanonly
local share_billboard_main = r(mean)

preserve
clear
set obs 1
gen analysis_start_year = `analysis_start_year'
gen n_rows_main = `N_main'
gen n_unique_artists_main = `N_artists_main'
gen n_unique_artist_song_pairs_main = `N_songs_main'
gen birth_year_min_main = `birth_min_main'
gen birth_year_max_main = `birth_max_main'
gen mean_artist_age_at_release_main = `mean_age_release_main'
gen mean_release_upload_gap_main = `mean_release_upload_gap_main'
gen mean_complexity_main = `mean_complexity_main'
gen mean_repetition_main = `mean_repetition_main'
gen mean_playability_main = `mean_playability_main'
gen share_billboard_year_end_main = `share_billboard_main'
export delimited using "`table_dir'/sample_overview_1946plus.csv", replace
restore

quietly count if analysis_sample_country == 1
local N_main_country = r(N)

egen artist_tag_country = tag(artist_id) if analysis_sample_country == 1
quietly count if artist_tag_country == 1
local N_artists_country = r(N)

egen song_tag_country = tag(artist_name song_name) if analysis_sample_country == 1
quietly count if song_tag_country == 1
local N_songs_country = r(N)

quietly summarize artist_age_at_release if analysis_sample_country == 1, meanonly
local mean_age_release_country = r(mean)

quietly summarize release_upload_gap if analysis_sample_country == 1, meanonly
local mean_release_upload_gap_country = r(mean)

quietly summarize complexity if analysis_sample_country == 1, meanonly
local mean_complexity_country = r(mean)

quietly summarize repetition if analysis_sample_country == 1, meanonly
local mean_repetition_country = r(mean)

quietly summarize playability if analysis_sample_country == 1, meanonly
local mean_playability_country = r(mean)

quietly summarize billboard_country_year_end_flag if analysis_sample_country == 1, meanonly
local share_billboard_country = r(mean)

preserve
clear
set obs 1
gen analysis_start_year = `analysis_start_year'
gen analysis_end_year = `analysis_end_year'
gen n_rows_main_country = `N_main_country'
gen n_unique_artists_main_country = `N_artists_country'
gen n_artist_song_pairs_country = `N_songs_country'
gen mean_age_release_country = `mean_age_release_country'
gen mean_release_upload_gap_country = `mean_release_upload_gap_country'
gen mean_complexity_main_country = `mean_complexity_country'
gen mean_repetition_main_country = `mean_repetition_country'
gen mean_playability_main_country = `mean_playability_country'
gen share_billboard_country = `share_billboard_country'
export delimited using "`table_dir'/sample_overview_country_only_1946plus.csv", replace
restore

drop artist_tag_all song_tag_all artist_tag_main song_tag_main artist_tag_country song_tag_country

local keyvars release_year upload_year birth_year birth_country us_macro_region genre ///
              bpm complexity repetition melodicness energy finger_movement ///
              disruption root_stability intra_root_variation harmonic_palette ///
              loop_strength structure_variation playability harmonic_softness

tempfile coverage_tmp
tempname coverage_post
postfile `coverage_post' str40 variable double share_nonmissing using `coverage_tmp', replace
foreach var of local keyvars {
    quietly count if !missing(`var')
    post `coverage_post' ("`var'") (r(N) / `N')
}
postclose `coverage_post'

preserve
use `coverage_tmp', clear
export delimited using "`table_dir'/coverage_key_variables.csv", replace
restore

* ---------------------------------------------------------------------------
* 4. Tavole temporali - analisi principale country-only
* ---------------------------------------------------------------------------

preserve
keep if analysis_sample_country == 1
egen artist_tag_decade = tag(release_decade artist_id)
collapse ///
    (sum) n_tabs = one n_artists = artist_tag_decade ///
    (mean) bpm complexity repetition melodicness energy finger_movement ///
           disruption root_stability intra_root_variation harmonic_palette ///
           loop_strength structure_variation playability harmonic_softness ///
           artist_age_at_release release_upload_gap billboard_country_year_end_flag ///
           billboard_top10 billboard_top20 has_intro has_bridge has_outro has_chorus, ///
    by(release_decade)
rename billboard_country_year_end_flag share_billboard_year_end
rename billboard_top10 share_billboard_top10
rename billboard_top20 share_billboard_top20
export delimited using "`table_dir'/temporal_summary_by_release_decade.csv", replace
restore

preserve
keep if analysis_sample_country == 1
collapse ///
    (sum) n_tabs = one ///
    (mean) bpm complexity repetition melodicness energy finger_movement ///
           disruption root_stability intra_root_variation harmonic_palette ///
           loop_strength structure_variation playability harmonic_softness ///
           billboard_country_year_end_flag release_upload_gap, ///
    by(release_year)
rename billboard_country_year_end_flag share_billboard_year_end
sort release_year
export delimited using "`table_dir'/temporal_summary_by_release_year.csv", replace

twoway ///
    (line n_tabs release_year, lcolor(navy) lwidth(medthick)), ///
    title("Country Chords Dataset: songs by release year") ///
    subtitle("Main analysis: genre = country, `analysis_start_year'-`analysis_end_year'") ///
    xtitle("Release year") ytitle("Number of observations") ///
    xlabel(`analysis_start_year'(5)`analysis_end_year', angle(45)) ///
    xscale(range(`analysis_start_year' `analysis_end_year'))
graph export "`graph_dir'/01_count_songs_by_release_year.png", replace width(2200)

twoway ///
    (line bpm release_year, lcolor(black) lwidth(medthick)), ///
    title("Average BPM by release year") ///
    subtitle("genre = country; BPM still has high missingness") ///
    xtitle("Release year") ytitle("Mean BPM") ///
    xlabel(`analysis_start_year'(5)`analysis_end_year', angle(45)) ///
    xscale(range(`analysis_start_year' `analysis_end_year'))
graph export "`graph_dir'/02_bpm_by_release_year.png", replace width(2200)

twoway ///
    (line complexity release_year, lcolor(navy) lwidth(medthick)) ///
    (line playability release_year, lcolor(maroon) lwidth(medthick)) ///
    (line finger_movement release_year, lcolor(forest_green) lwidth(medthick)) ///
    (line disruption release_year, lcolor(orange_red) lwidth(medthick)), ///
    title("Mechanics and difficulty indices over time") ///
    xtitle("Release year") ytitle("Mean index") ///
    xlabel(`analysis_start_year'(5)`analysis_end_year', angle(45)) ///
    xscale(range(`analysis_start_year' `analysis_end_year')) ///
    legend(order(1 "Complexity" 2 "Playability" 3 "Finger movement" 4 "Disruption"))
graph export "`graph_dir'/03_indices_mechanics_by_release_year.png", replace width(2200)

twoway ///
    (line repetition release_year, lcolor(navy) lwidth(medthick)) ///
    (line energy release_year, lcolor(cranberry) lwidth(medthick)) ///
    (line loop_strength release_year, lcolor(forest_green) lwidth(medthick)) ///
    (line structure_variation release_year, lcolor(gold) lwidth(medthick)), ///
    title("Rhythm and form indices over time") ///
    xtitle("Release year") ytitle("Mean index") ///
    xlabel(`analysis_start_year'(5)`analysis_end_year', angle(45)) ///
    xscale(range(`analysis_start_year' `analysis_end_year')) ///
    legend(order(1 "Repetition" 2 "Energy" 3 "Loop strength" 4 "Structure variation"))
graph export "`graph_dir'/04_indices_rhythm_form_by_release_year.png", replace width(2200)

twoway ///
    (line melodicness release_year, lcolor(navy) lwidth(medthick)) ///
    (line root_stability release_year, lcolor(cranberry) lwidth(medthick)) ///
    (line intra_root_variation release_year, lcolor(orange_red) lwidth(medthick)) ///
    (line harmonic_palette release_year, lcolor(forest_green) lwidth(medthick)) ///
    (line harmonic_softness release_year, lcolor(gs8) lwidth(medthick)), ///
    title("Harmony indices over time") ///
    xtitle("Release year") ytitle("Mean index") ///
    xlabel(`analysis_start_year'(5)`analysis_end_year', angle(45)) ///
    xscale(range(`analysis_start_year' `analysis_end_year')) ///
    legend(order(1 "Melodicness" 2 "Root stability" 3 "Intra-root variation" ///
                 4 "Harmonic palette" 5 "Harmonic softness") cols(2))
graph export "`graph_dir'/05_indices_harmony_by_release_year.png", replace width(2200)

twoway ///
    (line share_billboard_year_end release_year, lcolor(maroon) lwidth(medthick)) ///
    (line release_upload_gap release_year, yaxis(2) lcolor(navy) lpattern(dash) lwidth(medthick)), ///
    title("Billboard prominence and release-upload lag") ///
    xtitle("Release year") ///
    ytitle("Share in Billboard year-end chart", axis(1)) ///
    ytitle("Mean upload minus release gap", axis(2)) ///
    xlabel(`analysis_start_year'(5)`analysis_end_year', angle(45)) ///
    xscale(range(`analysis_start_year' `analysis_end_year')) ///
    legend(order(1 "Billboard share" 2 "Upload-release gap") cols(2))
graph export "`graph_dir'/06_billboard_and_release_upload_gap.png", replace width(2200)

foreach idx in complexity repetition melodicness energy finger_movement disruption ///
               root_stability intra_root_variation harmonic_palette loop_strength ///
               structure_variation playability harmonic_softness {
    twoway ///
        (line `idx' release_year, lcolor(navy) lwidth(medthick)), ///
        title("`idx' by release year") ///
        subtitle("Main analysis: country songs only") ///
        xtitle("Release year") ytitle("Mean index") ///
        xlabel(`analysis_start_year'(5)`analysis_end_year', angle(45)) ///
        xscale(range(`analysis_start_year' `analysis_end_year'))
    graph export "`graph_dir'/index_`idx'_by_release_year.png", replace width(2200)
}
restore

* ---------------------------------------------------------------------------
* 5. Diagnostica di genere nel campione largo e stile nel campione country-only
* ---------------------------------------------------------------------------

preserve
keep if !missing(release_year) & inrange(release_year, `analysis_start_year', `analysis_end_year')
contract genre_clean
rename _freq n_tabs
gsort -n_tabs
local keep_top_genres = min(15, _N)
keep in 1/`keep_top_genres'
export delimited using "`table_dir'/style_top_genres.csv", replace
restore

preserve
keep if !missing(release_decade) & !missing(release_year) & ///
    inrange(release_year, `analysis_start_year', `analysis_end_year')
collapse ///
    (mean) genre_country genre_rock genre_pop genre_folk genre_hiphop genre_rbsoul, ///
    by(release_decade)
sort release_decade
export delimited using "`table_dir'/diagnostic_genre_shares_by_release_decade_broad_sample.csv", replace

twoway ///
    (line genre_country release_decade, lcolor(forest_green) lwidth(medthick)) ///
    (line genre_rock release_decade, lcolor(navy) lwidth(medthick)) ///
    (line genre_pop release_decade, lcolor(cranberry) lwidth(medthick)) ///
    (line genre_folk release_decade, lcolor(gold) lwidth(medthick)) ///
    (line genre_hiphop release_decade, lcolor(black) lwidth(medthick)) ///
    (line genre_rbsoul release_decade, lcolor(orange_red) lwidth(medthick)), ///
    title("Diagnostic: genre mix among tabs by country artists") ///
    subtitle("Broad sample, before restricting to genre = country") ///
    xtitle("Release decade") ytitle("Share of observations") ///
    legend(order(1 "Country" 2 "Rock" 3 "Pop" 4 "Folk" 5 "Hip hop" 6 "R&B/Funk/Soul") cols(2))
graph export "`graph_dir'/07_diagnostic_genre_shares_by_release_decade_broad_sample.png", replace width(2200)
restore

preserve
keep if analysis_sample_country == 1
contract first_chord_group
rename _freq n_tabs
gsort -n_tabs
local keep_top_chords = min(15, _N)
keep in 1/`keep_top_chords'
export delimited using "`table_dir'/style_top_first_chords.csv", replace
restore

preserve
keep if analysis_sample_country == 1
collapse (mean) chord_g chord_c chord_d chord_a chord_e chord_f, by(release_decade)
sort release_decade
export delimited using "`table_dir'/style_first_chord_shares_by_release_decade.csv", replace

twoway ///
    (line chord_g release_decade, lcolor(forest_green) lwidth(medthick)) ///
    (line chord_c release_decade, lcolor(navy) lwidth(medthick)) ///
    (line chord_d release_decade, lcolor(cranberry) lwidth(medthick)) ///
    (line chord_a release_decade, lcolor(gold) lwidth(medthick)) ///
    (line chord_e release_decade, lcolor(orange_red) lwidth(medthick)) ///
    (line chord_f release_decade, lcolor(black) lwidth(medthick)), ///
    title("Most common starting chords over release decades") ///
    xtitle("Release decade") ytitle("Share of observations") ///
    legend(order(1 "G" 2 "C" 3 "D" 4 "A" 5 "E" 6 "F") cols(3))
graph export "`graph_dir'/08_first_chord_shares_by_release_decade.png", replace width(2200)
restore

* ---------------------------------------------------------------------------
* 6. Tavole e grafici geografici
* ---------------------------------------------------------------------------

preserve
keep if analysis_sample_country == 1
contract birth_country_clean
rename _freq n_tabs
gsort -n_tabs
local keep_top_countries = min(12, _N)
keep in 1/`keep_top_countries'
export delimited using "`table_dir'/geography_top_birth_countries.csv", replace
restore

preserve
keep if analysis_sample_country == 1
contract birth_state
rename _freq n_tabs
replace birth_state = "Missing" if trim(birth_state) == ""
gsort -n_tabs
local keep_top_states = min(15, _N)
keep in 1/`keep_top_states'
export delimited using "`table_dir'/geography_top_birth_states.csv", replace
restore

preserve
keep if analysis_sample_country == 1 & macro_region_clean != "Missing"
egen artist_tag_region = tag(macro_region_clean artist_id)
collapse ///
    (sum) n_tabs = one n_artists = artist_tag_region ///
    (mean) birth_year release_year artist_age_at_release complexity repetition ///
           energy playability harmonic_palette structure_variation ///
           billboard_country_year_end_flag, ///
    by(macro_region_clean)
rename billboard_country_year_end_flag share_billboard_year_end
gsort -n_tabs
export delimited using "`table_dir'/geography_summary_by_macro_region.csv", replace

graph hbar n_tabs, ///
    over(macro_region_clean, sort(1) descending) ///
    title("Geographic concentration by US macro-region") ///
    ytitle("Number of observations") blabel(bar, format(%9.0fc))
graph export "`graph_dir'/09_macro_region_distribution.png", replace width(2200)

graph bar complexity repetition playability harmonic_palette, ///
    over(macro_region_clean, sort(1) descending) ///
    title("Indices by macro-region of birth") ///
    ytitle("Mean index") ///
    legend(order(1 "Complexity" 2 "Repetition" 3 "Playability" 4 "Harmonic palette") cols(2))
graph export "`graph_dir'/10_indices_by_macro_region.png", replace width(2200)
restore

* ---------------------------------------------------------------------------
* 7. Tavole e grafici generazionali
* ---------------------------------------------------------------------------

preserve
keep if analysis_sample_country == 1 & !missing(generation_group)
egen artist_tag_generation = tag(generation_group artist_id)
collapse ///
    (sum) n_tabs = one n_artists = artist_tag_generation ///
    (mean) birth_year release_year artist_age_at_release complexity repetition ///
           energy playability harmonic_palette structure_variation ///
           billboard_country_year_end_flag release_upload_gap, ///
    by(generation_group)
rename billboard_country_year_end_flag share_billboard_year_end
label values generation_group generation_lbl
sort generation_group
decode generation_group, gen(generation_name)
export delimited using "`table_dir'/generation_summary.csv", replace

graph bar n_tabs, ///
    over(generation_group, relabel(1 "Greatest/Silent" 2 "Silent" 3 "Boomer" 4 "Gen X" 5 "Millennial" 6 "Gen Z+")) ///
    title("Observation count by artist generation") ///
    ytitle("Number of observations") blabel(bar, format(%9.0fc))
graph export "`graph_dir'/11_generation_distribution.png", replace width(2200)

graph bar complexity repetition playability structure_variation, ///
    over(generation_group, relabel(1 "Greatest/Silent" 2 "Silent" 3 "Boomer" 4 "Gen X" 5 "Millennial" 6 "Gen Z+")) ///
    title("Indices by artist generation") ///
    ytitle("Mean index") ///
    legend(order(1 "Complexity" 2 "Repetition" 3 "Playability" 4 "Structure variation") cols(2))
graph export "`graph_dir'/12_indices_by_generation.png", replace width(2200)
restore

* ---------------------------------------------------------------------------
* 8. Output supplementari utili per una lettura rapida
* ---------------------------------------------------------------------------

preserve
keep if analysis_sample_country == 1
keep artist_name song_name release_year upload_year birth_year artist_age_at_release ///
     macro_region_clean genre_clean first_chord_group complexity repetition ///
     playability harmonic_palette structure_variation billboard_country_year_end_flag ///
     billboard_country_year_end_pos
sort release_year artist_name song_name
export delimited using "`table_dir'/analysis_ready_extract.csv", replace
restore

* ---------------------------------------------------------------------------
* 9. Test di significativita'
* ---------------------------------------------------------------------------

tempfile trend_tests_tmp
tempname trend_tests_post
postfile `trend_tests_post' str40 variable double coef_year coef_decade p_value n_obs r_squared using `trend_tests_tmp', replace

foreach var in complexity repetition melodicness energy finger_movement disruption ///
               root_stability intra_root_variation harmonic_palette loop_strength ///
               structure_variation playability harmonic_softness bpm ///
               billboard_country_year_end_flag release_upload_gap {
    quietly regress `var' c.release_year if analysis_sample_country == 1, vce(robust)
    if !_rc {
        local b = _b[release_year]
        local n = e(N)
        local r2 = e(r2)
        test release_year
        local p = r(p)
        post `trend_tests_post' ("`var'") (`b') (`b' * 10) (`p') (`n') (`r2')
    }
}
postclose `trend_tests_post'

preserve
use `trend_tests_tmp', clear
export delimited using "`table_dir'/significance_time_trends_country_only.csv", replace
restore

tempfile count_trend_tmp
tempname count_trend_post
postfile `count_trend_post' str40 variable double coef_year coef_decade p_value n_obs r_squared using `count_trend_tmp', replace

preserve
keep if analysis_sample_country == 1
collapse (sum) n_tabs = one, by(release_year)
quietly regress n_tabs c.release_year, vce(robust)
local b = _b[release_year]
local n = e(N)
local r2 = e(r2)
test release_year
local p = r(p)
post `count_trend_post' ("n_tabs") (`b') (`b' * 10) (`p') (`n') (`r2')
restore
postclose `count_trend_post'

preserve
use `count_trend_tmp', clear
export delimited using "`table_dir'/significance_song_count_trend_country_only.csv", replace
restore

tempfile genre_trend_tmp
tempname genre_trend_post
postfile `genre_trend_post' str40 variable double coef_decade p_value n_obs r_squared using `genre_trend_tmp', replace

preserve
keep if !missing(release_decade) & !missing(release_year) & ///
    inrange(release_year, `analysis_start_year', `analysis_end_year')
collapse (mean) genre_country genre_rock genre_pop genre_folk genre_hiphop genre_rbsoul, by(release_decade)
foreach var in genre_country genre_rock genre_pop genre_folk genre_hiphop genre_rbsoul {
    quietly regress `var' c.release_decade, vce(robust)
    local b = _b[release_decade]
    local n = e(N)
    local r2 = e(r2)
    test release_decade
    local p = r(p)
    post `genre_trend_post' ("`var'") (`b') (`p') (`n') (`r2')
}
restore
postclose `genre_trend_post'

preserve
use `genre_trend_tmp', clear
export delimited using "`table_dir'/significance_genre_share_trends_broad_sample.csv", replace
restore

tempfile chord_trend_tmp
tempname chord_trend_post
postfile `chord_trend_post' str40 variable double coef_decade p_value n_obs r_squared using `chord_trend_tmp', replace

preserve
keep if analysis_sample_country == 1
collapse (mean) chord_g chord_c chord_d chord_a chord_e chord_f, by(release_decade)
foreach var in chord_g chord_c chord_d chord_a chord_e chord_f {
    quietly regress `var' c.release_decade, vce(robust)
    local b = _b[release_decade]
    local n = e(N)
    local r2 = e(r2)
    test release_decade
    local p = r(p)
    post `chord_trend_post' ("`var'") (`b') (`p') (`n') (`r2')
}
restore
postclose `chord_trend_post'

preserve
use `chord_trend_tmp', clear
export delimited using "`table_dir'/significance_first_chord_share_trends_country_only.csv", replace
restore

tempfile region_test_tmp
tempname region_test_post
postfile `region_test_post' str40 variable double p_value n_obs n_groups using `region_test_tmp', replace

foreach var in complexity repetition playability harmonic_palette ///
               structure_variation billboard_country_year_end_flag {
    preserve
    keep if analysis_sample_country == 1 & macro_region_clean != "Missing" & !missing(`var')
    encode macro_region_clean, gen(macro_region_code)
    quietly regress `var' i.macro_region_code, vce(robust)
    testparm i.macro_region_code
    local p = r(p)
    local n = e(N)
    quietly levelsof macro_region_clean, local(region_levels)
    local k : word count `region_levels'
    post `region_test_post' ("`var'") (`p') (`n') (`k')
    restore
}
postclose `region_test_post'

preserve
use `region_test_tmp', clear
export delimited using "`table_dir'/significance_macro_region_differences_country_only.csv", replace
restore

tempfile generation_test_tmp
tempname generation_test_post
postfile `generation_test_post' str40 variable double p_value n_obs n_groups using `generation_test_tmp', replace

foreach var in complexity repetition playability harmonic_palette ///
               structure_variation billboard_country_year_end_flag release_upload_gap {
    preserve
    keep if analysis_sample_country == 1 & !missing(generation_group) & !missing(`var')
    quietly regress `var' i.generation_group, vce(robust)
    testparm i.generation_group
    local p = r(p)
    local n = e(N)
    quietly levelsof generation_group, local(generation_levels)
    local k : word count `generation_levels'
    post `generation_test_post' ("`var'") (`p') (`n') (`k')
    restore
}
postclose `generation_test_post'

preserve
use `generation_test_tmp', clear
export delimited using "`table_dir'/significance_generation_differences_country_only.csv", replace
restore

display as text "Output salvati in: `out_root'"
log close
exit
