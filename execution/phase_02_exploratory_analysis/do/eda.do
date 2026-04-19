clear all
macro drop _all

* 0. Imposta Directory di Lavoro
cd "/Users/marcolemoglie_1_2/Library/CloudStorage/Dropbox/Sound_of_Culture"

* Sfondo bianco (White background) per grafici accademici
set scheme s1color

* 1. Carica il dataset
use "data/processed_datasets/top100YearEnd8525/dataset_chords_top100yearend.dta", clear

* 2. Assicura che variabili siano numeriche
destring chart_year, replace force
destring bpm, replace force

* 3. Label delle Variabili (Aggiornate)
label variable bpm "Beats Per Minute"
label variable complexity "Complexity Index"
label variable energy "Energy Index"
label variable melodicness "Melodicness Index"
label variable playability "Playability Index"
label variable repetition "Repetition Index"
label variable structure_variation "Structure Variation Index"
label variable finger_movement "Finger Movement Index"
label variable disruption "Disruption Index"
label variable root_stability "Root Stability Index"
label variable intra_root_variation "Intra-Root Variation Index"
label variable harmonic_palette "Harmonic Palette"
label variable loop_strength "Loop Strength"
label variable harmonic_softness "Harmonic Softness"
label variable genre "Music Genre"
label variable chart_year "Billboard List Year"

* 4. Creazione Variabili Dicotomiche per Accordi Principali
gen chord_G = (chord_1 == "G")
gen chord_D = (chord_1 == "D")
gen chord_C = (chord_1 == "C")
gen chord_Am = (chord_1 == "Am")

label variable chord_G "Share of Songs Starting on G"
label variable chord_D "Share of Songs Starting on D"
label variable chord_C "Share of Songs Starting on C"
label variable chord_Am "Share of Songs Starting on Am"

* 5. Creazione Variabili Dicotomiche per Genere Musicale
gen genre_pop = (genre == "pop")
gen genre_rock = (genre == "rock")
gen genre_hip_hop = (genre == "hip hop")
gen genre_rb_soul = (genre == "r&b funk & soul")
gen genre_country = (genre == "country")

label variable genre_pop "Share Pop"
label variable genre_rock "Share Rock"
label variable genre_hip_hop "Share Hip Hop"
label variable genre_rb_soul "Share R&B Funk & Soul"
label variable genre_country "Share Country"

* 6. Collasso e grafici
collapse (mean) bpm complexity energy melodicness repetition playability finger_movement disruption structure_variation loop_strength root_stability intra_root_variation harmonic_palette harmonic_softness chord_G chord_D chord_C chord_Am genre_pop genre_rock genre_hip_hop genre_rb_soul genre_country, by(chart_year)

* Ordina per anno
sort chart_year

* Cartella di Output
capture mkdir "execution/phase_02_exploratory_analysis/output_figures"
local out "execution/phase_02_exploratory_analysis/output_figures"

* --- Grafico 1: BPM medi per anno ---
twoway (line bpm chart_year, lcolor(black) lwidth(medium)), ///
    title("Average BPM by Year") xtitle("Year") ytitle("Mean BPM") ///
    xlabel(1980(5)2025) ylabel(#8)
graph export "`out'/bpm_by_year.pdf", replace

* --- Grafico 2: Trend Accordi Principali ---
twoway (line chord_G chart_year, lcolor(green) lwidth(medium)) ///
       (line chord_D chart_year, lcolor(red) lwidth(medium)) ///
       (line chord_C chart_year, lcolor(blue) lwidth(medium)) ///
       (line chord_Am chart_year, lcolor(purple) lwidth(medium)), ///
    title("Main Chord Frequencies Over Time") xtitle("Year") ytitle("Percentage Starting Chords") ///
    xlabel(1980(5)2025) ///
    legend(label(1 "G") label(2 "D") label(3 "C") label(4 "Am"))
graph export "`out'/chords_by_year.pdf", replace

* --- Grafico 3: Trend Generi Musicali ---
twoway (line genre_pop chart_year, lcolor(blue) lwidth(medium)) ///
       (line genre_rock chart_year, lcolor(red) lwidth(medium)) ///
       (line genre_hip_hop chart_year, lcolor(black) lwidth(medium)) ///
       (line genre_rb_soul chart_year, lcolor(orange) lwidth(medium)) ///
       (line genre_country chart_year, lcolor(green) lwidth(medium)), ///
    title("Main Genre Frequencies Over Time") xtitle("Year") ytitle("Percentage Share") ///
    xlabel(1980(5)2025) ///
    legend(label(1 "Pop") label(2 "Rock") label(3 "Hip Hop") label(4 "R&B/Soul") label(5 "Country"))
graph export "`out'/genres_by_year.pdf", replace

* --- Grafico 4: Indici - Meccanica & Difficoltà ---
twoway (line complexity chart_year, lcolor(blue) lwidth(medium)) ///
       (line playability chart_year, lcolor(red) lwidth(medium)) ///
       (line finger_movement chart_year, lcolor(green) lwidth(medium)) ///
       (line disruption chart_year, lcolor(black) lwidth(medium)), ///
    title("Mechanics & Difficulty Over Time") xtitle("Year") ytitle("Index Mean Value") ///
    xlabel(1980(5)2025) ///
    legend(label(1 "Complexity") label(2 "Playability") label(3 "Finger Movement") label(4 "Disruption"))
graph export "`out'/indices_mechanics_by_year.pdf", replace

* --- Grafico 5: Indici - Ritmo & Flusso ---
twoway (line repetition chart_year, lcolor(blue) lwidth(medium)) ///
       (line structure_variation chart_year, lcolor(red) lwidth(medium)) ///
       (line loop_strength chart_year, lcolor(green) lwidth(medium)) ///
       (line energy chart_year, lcolor(orange) lwidth(medium)), ///
    title("Rhythm & Flow Patterns Over Time") xtitle("Year") ytitle("Index Mean Value") ///
    xlabel(1980(5)2025) ///
    legend(label(1 "Repetition") label(2 "Structure Var") label(3 "Loop Strength") label(4 "Energy"))
graph export "`out'/indices_rhythm_by_year.pdf", replace

* --- Grafico 6: Indici - Armonia & Struttura ---
twoway (line melodicness chart_year, lcolor(green) lwidth(medium)) ///
       (line root_stability chart_year, lcolor(blue) lwidth(medium)) ///
       (line intra_root_variation chart_year, lcolor(red) lwidth(medium)) ///
       (line harmonic_palette chart_year, lcolor(orange) lwidth(medium)) ///
       (line harmonic_softness chart_year, lcolor(purple) lwidth(medium)), ///
    title("Harmony & Structure Patterns Over Time") xtitle("Year") ytitle("Index Mean Value") ///
    xlabel(1980(5)2025) ///
    legend(label(1 "Melodicness") label(2 "Root Stability") label(3 "Intra-Root Var") label(4 "Harmonic Pal") label(5 "Harmonic Softness"))
graph export "`out'/indices_harmony_by_year.pdf", replace

exit
