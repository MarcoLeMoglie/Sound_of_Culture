from __future__ import annotations

import math
import shutil
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = ROOT / "execution/step3_analysis/output_country_only_chords_2026_04_08"
DO_FILE = ROOT / "execution/step3_analysis/do/eda_country_only_chords_2026_04_08.do"
REPORT_DIR = ROOT / "execution/step3_analysis/report_sound_of_culture"
ANALYSIS_DIR = REPORT_DIR / "analysis_outputs"
GENERATED_DIR = REPORT_DIR / "generated"
CODE_DIR = REPORT_DIR / "code"


PRETTY = {
    "complexity": "Complexity",
    "repetition": "Repetition",
    "melodicness": "Melodicness",
    "energy": "Energy",
    "finger_movement": "Finger movement",
    "disruption": "Disruption",
    "root_stability": "Root stability",
    "intra_root_variation": "Intra-root variation",
    "harmonic_palette": "Harmonic palette",
    "loop_strength": "Loop strength",
    "structure_variation": "Structure variation",
    "playability": "Playability",
    "harmonic_softness": "Harmonic softness",
    "bpm": "BPM",
    "billboard_country_year_end_flag": "Billboard year-end share",
    "release_upload_gap": "Upload-release gap",
    "genre_country": "Country share",
    "genre_rock": "Rock share",
    "genre_pop": "Pop share",
    "genre_folk": "Folk share",
    "genre_hiphop": "Hip hop share",
    "genre_rbsoul": "R&B/Funk/Soul share",
    "chord_g": "G share",
    "chord_c": "C share",
    "chord_d": "D share",
    "chord_a": "A share",
    "chord_e": "E share",
    "chord_f": "F share",
    "n_tabs": "Song count",
}


def ensure_dirs() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    CODE_DIR.mkdir(parents=True, exist_ok=True)


def sync_outputs() -> None:
    if ANALYSIS_DIR.exists():
        shutil.rmtree(ANALYSIS_DIR)
    shutil.copytree(OUTPUT_DIR, ANALYSIS_DIR)
    shutil.copy2(DO_FILE, CODE_DIR / DO_FILE.name)


def latex_escape(text: str) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    out = str(text)
    for old, new in replacements.items():
        out = out.replace(old, new)
    return out


def fmt_int(value: float | int) -> str:
    return f"{int(round(float(value))):,}"


def fmt_num(value: float | int, digits: int = 3) -> str:
    if pd.isna(value):
        return ""
    return f"{float(value):,.{digits}f}"


def fmt_pct(value: float | int, digits: int = 1) -> str:
    if pd.isna(value):
        return ""
    return f"{100 * float(value):.{digits}f}\\%"


def fmt_p(value: float | int) -> str:
    if pd.isna(value):
        return ""
    value = float(value)
    if value < 0.001:
        return r"$<0.001$"
    return f"{value:.3f}"


def make_table_tex(df: pd.DataFrame, caption: str, label: str, landscape: bool = False) -> str:
    def tex_cell(value: object) -> str:
        text = str(value)
        if any(token in text for token in [r"\%", r"\&", r"\text", r"$", r"\_"]):
            return text
        return latex_escape(text)

    columns = list(df.columns)
    align = "l" + "c" * (len(columns) - 1)
    header = " & ".join(tex_cell(col) for col in columns) + r" \\"
    rows = []
    for _, row in df.iterrows():
        values = [tex_cell(row[col]) for col in columns]
        rows.append(" & ".join(values) + r" \\")
    body = "\n".join(rows)
    latex = (
        f"\\begin{{tabular}}{{{align}}}\n"
        "\\toprule\n"
        f"{header}\n"
        "\\midrule\n"
        f"{body}\n"
        "\\bottomrule\n"
        "\\end{tabular}"
    )
    if landscape:
        return (
            "\\begin{landscape}\n"
            "\\begin{table}[H]\n\\centering\n"
            f"\\caption{{{caption}}}\n"
            f"\\label{{{label}}}\n"
            f"{latex}\n"
            "\\end{table}\n"
            "\\end{landscape}\n"
        )
    return (
        "\\begin{table}[H]\n\\centering\n"
        f"\\caption{{{caption}}}\n"
        f"\\label{{{label}}}\n"
        f"{latex}\n"
        "\\end{table}\n"
    )


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def load_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(ANALYSIS_DIR / "tables" / name)


def extract_snippet(export_token: str, loop: bool = False) -> str:
    lines = DO_FILE.read_text(encoding="utf-8").splitlines()
    if loop:
        start = next(i for i, line in enumerate(lines) if line.strip().startswith("foreach idx in"))
        end = next(i for i in range(start, len(lines)) if lines[i].strip() == "}")
        return "\n".join(lines[start : end + 1]).strip()

    export_idx = next(i for i, line in enumerate(lines) if export_token in line)
    start = export_idx
    for i in range(export_idx, -1, -1):
        stripped = lines[i].strip()
        if stripped.startswith(("twoway", "graph hbar", "graph bar")):
            start = i
            break
    return "\n".join(lines[start : export_idx + 1]).strip()


def build_generated_tables() -> dict[str, str]:
    sample = load_csv("sample_overview_country_only_1946plus.csv").iloc[0]
    sample_long = pd.DataFrame(
        {
            "Metric": [
                "Analysis window",
                "Country-song observations",
                "Unique artists",
                "Unique artist-song pairs",
                "Mean age at release",
                "Mean upload-release gap",
                "Mean complexity",
                "Mean repetition",
                "Mean playability",
                "Billboard year-end share",
            ],
            "Value": [
                f"{int(sample.analysis_start_year)}--{int(sample.analysis_end_year)}",
                fmt_int(sample.n_rows_main_country),
                fmt_int(sample.n_unique_artists_main_country),
                fmt_int(sample.n_artist_song_pairs_country),
                fmt_num(sample.mean_age_release_country, 2),
                fmt_num(sample.mean_release_upload_gap_country, 2),
                fmt_num(sample.mean_complexity_main_country, 3),
                fmt_num(sample.mean_repetition_main_country, 3),
                fmt_num(sample.mean_playability_main_country, 3),
                fmt_pct(sample.share_billboard_country, 1),
            ],
        }
    )
    sample_tex = make_table_tex(sample_long, "Main country-only analytical sample", "tab:sample")

    coverage = load_csv("coverage_key_variables.csv")
    coverage = coverage[coverage["variable"].isin(["release_year", "birth_year", "genre", "bpm", "complexity", "repetition", "playability"])]
    coverage["Variable"] = coverage["variable"].map(PRETTY).fillna(coverage["variable"])
    coverage["Non-missing share"] = coverage["share_nonmissing"].map(lambda x: fmt_pct(x, 1))
    coverage = coverage[["Variable", "Non-missing share"]]
    coverage_tex = make_table_tex(coverage, "Coverage of key variables", "tab:coverage")

    trends = load_csv("significance_time_trends_country_only.csv")
    trends["Outcome"] = trends["variable"].map(PRETTY)
    trends["Slope per year"] = trends["coef_year"].map(lambda x: fmt_num(x, 4))
    trends["Slope per decade"] = trends["coef_decade"].map(lambda x: fmt_num(x, 4))
    trends["p-value"] = trends["p_value"].map(fmt_p)
    trends["N"] = trends["n_obs"].map(fmt_int)
    trends = trends[["Outcome", "Slope per year", "Slope per decade", "p-value", "N"]]
    trends_tex = make_table_tex(trends, "Time-trend tests in the country-only sample", "tab:time_trends", landscape=True)

    count_trend = load_csv("significance_song_count_trend_country_only.csv")
    count_trend["Outcome"] = "Song count"
    count_trend["Slope per year"] = count_trend["coef_year"].map(lambda x: fmt_num(x, 3))
    count_trend["Slope per decade"] = count_trend["coef_decade"].map(lambda x: fmt_num(x, 3))
    count_trend["p-value"] = count_trend["p_value"].map(fmt_p)
    count_trend["N years"] = count_trend["n_obs"].map(fmt_int)
    count_trend = count_trend[["Outcome", "Slope per year", "Slope per decade", "p-value", "N years"]]
    count_tex = make_table_tex(count_trend, "Trend test for annual song counts", "tab:count_trend")

    genre_trends = load_csv("significance_genre_share_trends_broad_sample.csv")
    genre_trends["Outcome"] = genre_trends["variable"].map(PRETTY)
    genre_trends["Slope per decade"] = genre_trends["coef_decade"].map(lambda x: fmt_num(x, 4))
    genre_trends["p-value"] = genre_trends["p_value"].map(fmt_p)
    genre_trends = genre_trends[["Outcome", "Slope per decade", "p-value"]]
    genre_tex = make_table_tex(genre_trends, "Trend tests for broad-sample genre shares", "tab:genre_trends")

    chord_trends = load_csv("significance_first_chord_share_trends_country_only.csv")
    chord_trends["Outcome"] = chord_trends["variable"].map(PRETTY)
    chord_trends["Slope per decade"] = chord_trends["coef_decade"].map(lambda x: fmt_num(x, 4))
    chord_trends["p-value"] = chord_trends["p_value"].map(fmt_p)
    chord_trends = chord_trends[["Outcome", "Slope per decade", "p-value"]]
    chord_tex = make_table_tex(chord_trends, "Trend tests for first-chord shares", "tab:chord_trends")

    geo = load_csv("geography_summary_by_macro_region.csv")
    geo["Macro-region"] = geo["macro_region_clean"].map(latex_escape)
    geo["Songs"] = geo["n_tabs"].map(fmt_int)
    geo["Artists"] = geo["n_artists"].map(fmt_int)
    geo["Complexity"] = geo["complexity"].map(lambda x: fmt_num(x, 3))
    geo["Repetition"] = geo["repetition"].map(lambda x: fmt_num(x, 3))
    geo["Playability"] = geo["playability"].map(lambda x: fmt_num(x, 3))
    geo["Billboard share"] = geo["share_billboard_year_end"].map(lambda x: fmt_pct(x, 1))
    geo = geo[["Macro-region", "Songs", "Artists", "Complexity", "Repetition", "Playability", "Billboard share"]]
    geo_tex = make_table_tex(geo, "Macro-region summary in the country-only sample", "tab:geo")

    geo_tests = load_csv("significance_macro_region_differences_country_only.csv")
    geo_tests["Outcome"] = geo_tests["variable"].map(PRETTY)
    geo_tests["p-value"] = geo_tests["p_value"].map(fmt_p)
    geo_tests["N"] = geo_tests["n_obs"].map(fmt_int)
    geo_tests = geo_tests[["Outcome", "p-value", "N"]]
    geo_tests_tex = make_table_tex(geo_tests, "Omnibus tests for macro-region differences", "tab:geo_tests")

    gen = load_csv("generation_summary.csv")
    gen["Generation"] = gen["generation_name"].map(latex_escape)
    gen["Songs"] = gen["n_tabs"].map(fmt_int)
    gen["Artists"] = gen["n_artists"].map(fmt_int)
    gen["Complexity"] = gen["complexity"].map(lambda x: fmt_num(x, 3))
    gen["Repetition"] = gen["repetition"].map(lambda x: fmt_num(x, 3))
    gen["Playability"] = gen["playability"].map(lambda x: fmt_num(x, 3))
    gen["Structure variation"] = gen["structure_variation"].map(lambda x: fmt_num(x, 3))
    gen["Billboard share"] = gen["share_billboard_year_end"].map(lambda x: fmt_pct(x, 1))
    gen = gen[["Generation", "Songs", "Artists", "Complexity", "Repetition", "Playability", "Structure variation", "Billboard share"]]
    gen_tex = make_table_tex(gen, "Generation summary in the country-only sample", "tab:generation", landscape=True)

    gen_tests = load_csv("significance_generation_differences_country_only.csv")
    gen_tests["Outcome"] = gen_tests["variable"].map(PRETTY)
    gen_tests["p-value"] = gen_tests["p_value"].map(fmt_p)
    gen_tests["N"] = gen_tests["n_obs"].map(fmt_int)
    gen_tests = gen_tests[["Outcome", "p-value", "N"]]
    gen_tests_tex = make_table_tex(gen_tests, "Omnibus tests for generation differences", "tab:generation_tests")

    tables = {
        "sample_table.tex": sample_tex,
        "coverage_table.tex": coverage_tex,
        "trend_table.tex": trends_tex,
        "count_trend_table.tex": count_tex,
        "genre_trend_table.tex": genre_tex,
        "chord_trend_table.tex": chord_tex,
        "geo_table.tex": geo_tex,
        "geo_tests_table.tex": geo_tests_tex,
        "generation_table.tex": gen_tex,
        "generation_tests_table.tex": gen_tests_tex,
    }
    for name, content in tables.items():
        write_text(GENERATED_DIR / name, content)
    return tables


def trend_comment(trend_df: pd.DataFrame, var: str) -> str:
    row = trend_df.loc[trend_df["variable"] == var].iloc[0]
    slope = float(row["coef_decade"])
    p = fmt_p(row["p_value"])
    direction = "increases" if slope > 0 else "decreases"
    return f"{PRETTY[var]} {direction} by {fmt_num(abs(slope), 4)} points per decade ({p})."


def figure_block(title: str, file_name: str, label: str, discussion: str, code: str, width: str = "0.92\\textwidth") -> str:
    path = f"analysis_outputs/graphs/{file_name}"
    return f"""
\\subsection{{{title}}}
\\begin{{figure}}[H]
\\centering
\\includegraphics[width={width}]{{{path}}}
\\caption{{{title}}}
\\label{{{label}}}
\\end{{figure}}
{discussion}

\\paragraph{{Stata code used for the figure.}}
\\begin{{lstlisting}}
{code}
\\end{{lstlisting}}
"""


def build_main_tex() -> None:
    broad_sample = load_csv("sample_overview_1946plus.csv").iloc[0]
    sample = load_csv("sample_overview_country_only_1946plus.csv").iloc[0]
    geo = load_csv("geography_summary_by_macro_region.csv")
    gen = load_csv("generation_summary.csv")
    top_country = load_csv("geography_top_birth_countries.csv").iloc[0]
    top_state = load_csv("geography_top_birth_states.csv").iloc[0]
    genre_diag = load_csv("diagnostic_genre_shares_by_release_decade_broad_sample.csv")
    chord_trends = load_csv("significance_first_chord_share_trends_country_only.csv")
    trend_df = load_csv("significance_time_trends_country_only.csv")
    region_tests = load_csv("significance_macro_region_differences_country_only.csv")
    generation_tests = load_csv("significance_generation_differences_country_only.csv")
    count_trend = load_csv("significance_song_count_trend_country_only.csv").iloc[0]
    genre_trend_tests = load_csv("significance_genre_share_trends_broad_sample.csv")

    retained_share = sample.n_rows_main_country / broad_sample.n_rows_main
    south_row = geo.iloc[0]
    app_row = geo.loc[geo["macro_region_clean"] == "Appalachia"].iloc[0]
    northeast_row = geo.loc[geo["macro_region_clean"] == "Northeast"].iloc[0]
    unknown_row = geo.loc[geo["macro_region_clean"] == "Unknown"].iloc[0]
    silent_row = gen.loc[gen["generation_name"] == "Silent"].iloc[0]
    millennial_row = gen.loc[gen["generation_name"] == "Millennial"].iloc[0]
    genz_row = gen.loc[gen["generation_name"] == "Gen Z+"].iloc[0]
    greatest_row = gen.loc[gen["generation_name"] == "Greatest/Silent"].iloc[0]

    snippets = {
        "fig1": extract_snippet("01_count_songs_by_release_year.png"),
        "fig2": extract_snippet("02_bpm_by_release_year.png"),
        "fig3": extract_snippet("03_indices_mechanics_by_release_year.png"),
        "fig4": extract_snippet("04_indices_rhythm_form_by_release_year.png"),
        "fig5": extract_snippet("05_indices_harmony_by_release_year.png"),
        "fig6": extract_snippet("06_billboard_and_release_upload_gap.png"),
        "fig7": extract_snippet("07_diagnostic_genre_shares_by_release_decade_broad_sample.png"),
        "fig8": extract_snippet("08_first_chord_shares_by_release_decade.png"),
        "fig9": extract_snippet("09_macro_region_distribution.png"),
        "fig10": extract_snippet("10_indices_by_macro_region.png"),
        "fig11": extract_snippet("11_generation_distribution.png"),
        "fig12": extract_snippet("12_indices_by_generation.png"),
        "loop": extract_snippet("index_`idx'_by_release_year.png", loop=True),
    }

    index_files = [
        ("complexity", "index_complexity_by_release_year.png"),
        ("repetition", "index_repetition_by_release_year.png"),
        ("melodicness", "index_melodicness_by_release_year.png"),
        ("energy", "index_energy_by_release_year.png"),
        ("finger_movement", "index_finger_movement_by_release_year.png"),
        ("disruption", "index_disruption_by_release_year.png"),
        ("root_stability", "index_root_stability_by_release_year.png"),
        ("intra_root_variation", "index_intra_root_variation_by_release_year.png"),
        ("harmonic_palette", "index_harmonic_palette_by_release_year.png"),
        ("loop_strength", "index_loop_strength_by_release_year.png"),
        ("structure_variation", "index_structure_variation_by_release_year.png"),
        ("playability", "index_playability_by_release_year.png"),
        ("harmonic_softness", "index_harmonic_softness_by_release_year.png"),
    ]

    index_gallery = []
    for var, file_name in index_files:
        comment = trend_comment(trend_df, var)
        index_gallery.append(
            f"""
\\subsection{{{PRETTY[var]}: individual annual trend}}
\\begin{{figure}}[H]
\\centering
\\includegraphics[width=0.88\\textwidth]{{analysis_outputs/graphs/{file_name}}}
\\caption{{Individual annual trend for {PRETTY[var].lower()}}}
\\end{{figure}}
{comment}
"""
        )

    main_body = f"""
\\documentclass[11pt]{{article}}
\\usepackage[utf8]{{inputenc}}
\\usepackage[T1]{{fontenc}}
\\usepackage{{geometry}}
\\usepackage{{graphicx}}
\\usepackage{{booktabs}}
\\usepackage{{longtable}}
\\usepackage{{array}}
\\usepackage{{float}}
\\usepackage{{pdflscape}}
\\usepackage{{hyperref}}
\\usepackage{{caption}}
\\usepackage{{listings}}
\\usepackage{{xcolor}}
\\geometry{{margin=1in}}
\\hypersetup{{colorlinks=true, linkcolor=blue, urlcolor=blue}}
\\lstset{{
  basicstyle=\\ttfamily\\scriptsize,
  breaklines=true,
  columns=fullflexible,
  frame=single,
  keepspaces=true
}}

\\title{{Sound of Culture\\\\Exploratory Report on the Country-Only Chords Dataset}}
\\author{{Automated analytical note built from Stata outputs}}
\\date{{\\today}}

\\begin{{document}}
\\maketitle
\\tableofcontents
\\clearpage

\\section{{Purpose of the report}}
This report documents the exploratory descriptive analysis of
\\path{{Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.dta}}.
The goal is threefold: first, to clarify exactly why the analytical sample was
restricted to songs released between 1946 and 2026 and tagged as \\texttt{{country}};
second, to read the temporal, stylistic, geographic and generational patterns in a
way that is statistically disciplined rather than purely visual; third, to leave a
fully reproducible paper trail by packaging the Stata do-file, all tables, all
figures, and the exact Stata code used for each plotted object.

The broad 1946+ sample contains {fmt_int(broad_sample.n_rows_main)} observations, but only
{fmt_pct(retained_share, 1)} of them, i.e. {fmt_int(sample.n_rows_main_country)} songs, are
explicitly tagged as \\texttt{{country}} by Ultimate Guitar. This is why the broad-sample
genre diagnostic is kept in the report, while the main inferential reading is based on
the stricter country-song sample.

\\section{{How the analysis was designed and how to read it}}
The logic of the analysis is intentionally sequential.

\\begin{{enumerate}}
\\item The do-file first relabels the dataset and builds analytical variables such as release decade, artist generation, upload-release gap, and grouped first chords.
\\item It then creates three nested samples: the full dataset, the 1946+ sample, and the main country-only sample used for inference.
\\item It exports descriptive tables and figures for temporal trends, genre diagnostics, chord usage, geography and generations.
\\item It finally estimates significance checks. For temporal plots the report uses robust linear trend regressions. For regional and generational contrasts it uses robust regressions with group dummies followed by omnibus \\texttt{{testparm}} tests. For decade-level genre and chord share figures it uses decade-level trend regressions, which are lower-power because they rely on only nine decade points.
\\end{{enumerate}}

The significance tests help decide whether a visible difference is plausibly systematic. They do not by themselves imply causality. With this sample size, tiny differences can be statistically significant, so the report always pairs p-values with effect size and substantive interpretation.

\\input{{generated/sample_table.tex}}
\\input{{generated/coverage_table.tex}}

\\section{{Structure of the Stata do-file}}
The Stata file \\texttt{{eda\\_country\\_only\\_chords\\_2026\\_04\\_08.do}} is organized in nine blocks.

\\begin{{enumerate}}
\\item Full relabeling of original song-level and artist-level variables.
\\item Minimal cleaning and creation of derived variables such as generation and first-chord groups.
\\item Overview and coverage tables for the full sample, the 1946+ sample, and the main country-only sample.
\\item Temporal tables and annual trend figures for the country-only sample.
\\item Genre diagnostics on the broad artist-based sample and chord-style figures on the country-only sample.
\\item Geographic summaries and region-level figures.
\\item Generational summaries and generation-level figures.
\\item Export of an analysis-ready extract.
\\item Significance tests for temporal trends and group differences.
\\end{{enumerate}}

\\section{{Main descriptive and inferential findings}}

{figure_block(
    "Figure 1. Annual song counts in the country-only sample",
    "01_count_songs_by_release_year.png",
    "fig:song_count",
    f"The number of country-tagged tabs rises strongly over time. The annual count trend is positive and statistically significant: {fmt_num(count_trend.coef_year, 3)} additional songs per year, or {fmt_num(count_trend.coef_decade, 3)} per decade, with p-value {fmt_p(count_trend.p_value)}. This figure should be read as a coverage and visibility graph rather than as a direct measure of production, because it reflects tab availability on Ultimate Guitar.",
    snippets['fig1']
)}

{figure_block(
    "Figure 2. Average BPM by release year",
    "02_bpm_by_release_year.png",
    "fig:bpm",
    f"BPM has much weaker evidence of secular change. The estimated slope is {fmt_num(trend_df.loc[trend_df.variable == 'bpm', 'coef_decade'].iloc[0], 3)} BPM per decade with p-value {fmt_p(trend_df.loc[trend_df.variable == 'bpm', 'p_value'].iloc[0])}. Because BPM is observed for only a minority of songs, the figure is informative but should be treated as suggestive rather than central.",
    snippets['fig2']
)}

{figure_block(
    "Figure 3. Mechanics and difficulty indices over time",
    "03_indices_mechanics_by_release_year.png",
    "fig:mechanics",
    f"Mechanically, the repertoire becomes easier and more standardized. Complexity decreases significantly ({fmt_p(trend_df.loc[trend_df.variable == 'complexity', 'p_value'].iloc[0])}), playability rises in almost mirror-image form ({fmt_p(trend_df.loc[trend_df.variable == 'playability', 'p_value'].iloc[0])}), finger movement increases modestly but significantly, and disruption also increases slightly. The joint reading is that modern country songs use easier-to-play chord systems but with somewhat more movement across the sequence.",
    snippets['fig3']
)}

{figure_block(
    "Figure 4. Rhythm and form indices over time",
    "04_indices_rhythm_form_by_release_year.png",
    "fig:rhythm",
    f"Repetition, loop strength and structure variation all trend upward and all three slopes are statistically significant at conventional levels. Energy also increases, but the effect is small in magnitude. This indicates a repertoire that becomes more loop-friendly and formally segmented over time, even while remaining harmonically accessible.",
    snippets['fig4']
)}

{figure_block(
    "Figure 5. Harmony indices over time",
    "05_indices_harmony_by_release_year.png",
    "fig:harmony",
    f"The harmony panel shows a mixed but coherent transition. Melodicness rises significantly, while root stability, intra-root variation, harmonic palette and harmonic softness all decline. Substantively, this suggests that the modern country sample relies on fewer distinct harmonic resources and less soft-chord coloring, even as consonant melodic motion becomes more common.",
    snippets['fig5']
)}

{figure_block(
    "Figure 6. Billboard prominence and upload-release gap",
    "06_billboard_and_release_upload_gap.png",
    "fig:billboard_gap",
    f"The Billboard share increases slightly but significantly ({fmt_p(trend_df.loc[trend_df.variable == 'billboard_country_year_end_flag', 'p_value'].iloc[0])}), while the upload-release gap collapses mechanically and statistically ({fmt_p(trend_df.loc[trend_df.variable == 'release_upload_gap', 'p_value'].iloc[0])}). The second result is especially strong and mostly reflects digital-era contemporaneity: newer songs are uploaded almost immediately relative to release.",
    snippets['fig6']
)}

{figure_block(
    "Figure 7. Broad-sample genre diagnostic",
    "07_diagnostic_genre_shares_by_release_decade_broad_sample.png",
    "fig:genre_diagnostic",
    f"This diagnostic explains why the report restricts the main sample. Even among country artists, the song-level metadata are not exclusively country. The broad sample retains a country share between about 59\\% and 69\\% by decade, while rock and pop remain substantial. The country share itself does not show a significant linear trend across decades (p-value {fmt_p(genre_trend_tests.loc[genre_trend_tests.variable == 'genre_country', 'p_value'].iloc[0])}), which means that the cross-genre contamination is persistent rather than concentrated in one period.",
    snippets['fig7']
)}

{figure_block(
    "Figure 8. First-chord shares over release decades",
    "08_first_chord_shares_by_release_decade.png",
    "fig:first_chords",
    f"The first-chord distribution is remarkably stable. G remains dominant throughout, C and D stay broadly flat, A declines at borderline significance (p-value {fmt_p(chord_trends.loc[chord_trends.variable == 'chord_a', 'p_value'].iloc[0])}), and E shows a clearer negative trend (p-value {fmt_p(chord_trends.loc[chord_trends.variable == 'chord_e', 'p_value'].iloc[0])}). This stability reinforces the idea that the country harmonic core is persistent even as other dimensions evolve.",
    snippets['fig8']
)}

{figure_block(
    "Figure 9. Geographic concentration by macro-region",
    "09_macro_region_distribution.png",
    "fig:geo_counts",
    f"The country-only sample is geographically concentrated. The South alone contributes {fmt_int(south_row.n_tabs)} songs, about {fmt_pct(south_row.n_tabs / sample.n_rows_main_country, 1)} of the analytical sample. Appalachia is the second major cluster with {fmt_int(app_row.n_tabs)} songs. At the country level, the dominant birth country is {latex_escape(top_country.birth_country_clean)}; at the state level the leading state is {latex_escape(top_state.birth_state)}. This figure is descriptive and should be interpreted together with Figure~\\ref{{fig:geo_indices}}, where the inferential group contrasts are tested directly.",
    snippets['fig9']
)}

{figure_block(
    "Figure 10. Index differences by macro-region",
    "10_indices_by_macro_region.png",
    "fig:geo_indices",
    f"Regional differences are large enough to survive omnibus tests for every reported measure. For instance, the omnibus p-values are {fmt_p(region_tests.loc[region_tests.variable == 'complexity', 'p_value'].iloc[0])} for complexity, {fmt_p(region_tests.loc[region_tests.variable == 'repetition', 'p_value'].iloc[0])} for repetition, and {fmt_p(region_tests.loc[region_tests.variable == 'billboard_country_year_end_flag', 'p_value'].iloc[0])} for Billboard share. Appalachia stands out for repetition and Billboard presence, the Northeast has the highest harmonic palette, and the 'Unknown' group mechanically tops playability and structure variation, likely reflecting metadata selection rather than a clear cultural region.",
    snippets['fig10']
)}

{figure_block(
    "Figure 11. Observation count by artist generation",
    "11_generation_distribution.png",
    "fig:generation_counts",
    f"Generationally, the sample is anchored by older cohorts: the Silent generation contributes {fmt_int(silent_row.n_tabs)} songs and Boomers contribute {fmt_int(gen.loc[gen.generation_name == 'Boomer', 'n_tabs'].iloc[0])}. Millennials and Gen Z+ are smaller in raw count but become disproportionately important in the most recent period. As with the regional count figure, this panel is descriptive and should be paired with the inferential contrast in Figure~\\ref{{fig:generation_indices}}.",
    snippets['fig11']
)}

{figure_block(
    "Figure 12. Index differences by artist generation",
    "12_indices_by_generation.png",
    "fig:generation_indices",
    f"Generational contrasts are extremely strong statistically. Complexity, repetition, playability, structure variation, Billboard share and upload-release gap all reject equality across generations with p-values effectively at zero. Substantively, older cohorts such as {latex_escape(greatest_row.generation_name)} are much more complex and much less playable, while Millennials and Gen Z+ are more repetitive, easier to play, and structurally more varied. Millennials also show the highest structure variation, whereas Gen Z+ combines high repetition with very low average age at release.",
    snippets['fig12']
)}

\\input{{generated/count_trend_table.tex}}
\\input{{generated/trend_table.tex}}
\\input{{generated/genre_trend_table.tex}}
\\input{{generated/chord_trend_table.tex}}
\\input{{generated/geo_table.tex}}
\\input{{generated/geo_tests_table.tex}}
\\input{{generated/generation_table.tex}}
\\input{{generated/generation_tests_table.tex}}

\\section{{Interpretive summary}}
Several regularities emerge repeatedly across the figures and tests.

\\begin{{enumerate}}
\\item Modern country songs in this tab-based sample become easier to play and more repetitive, not more harmonically expansive.
\\item The strongest secular movements are the rise in repetition, loop strength and structure variation, and the fall in complexity and harmonic softness.
\\item Geographic heterogeneity is not cosmetic: macro-regional differences are significant for all core indices shown in the report.
\\item Generational turnover matters at least as much as geography. Older generations encode a more complex and less playable harmonic language, while younger generations are associated with high repetition and contemporary platform timing.
\\item The broad artist-based sample is not enough for substantive country-song inference; the country-only genre filter is necessary because the broad sample retains substantial rock and pop content in every decade.
\\end{{enumerate}}

\\section{{Appendix A: Individual index figures}}
The following gallery reports the one-variable annual figures generated by the loop below. The code is shared because each figure differs only by the index substituted into the loop.

\\begin{{lstlisting}}
{snippets['loop']}
\\end{{lstlisting}}

{''.join(index_gallery)}

\\section{{Appendix B: Full do-file}}
The full Stata script used to create all outputs is reproduced below.

\\lstinputlisting{{code/{DO_FILE.name}}}

\\end{{document}}
"""
    write_text(REPORT_DIR / "main.tex", main_body)


def main() -> None:
    ensure_dirs()
    sync_outputs()
    build_generated_tables()
    build_main_tex()


if __name__ == "__main__":
    main()
