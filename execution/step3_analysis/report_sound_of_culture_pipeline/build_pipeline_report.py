from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
REPORT_DIR = ROOT / "execution/step3_analysis/report_sound_of_culture_pipeline"
GENERATED_DIR = REPORT_DIR / "pipeline_generated"
SNIPPETS_DIR = GENERATED_DIR / "snippets"
CODE_DIR = REPORT_DIR / "pipeline_code"
ASSETS_DIR = REPORT_DIR / "pipeline_assets"

COUNTRY_ARTISTS_DIR = ROOT / "data" / "processed_datasets" / "country_artists"
INTERMEDIATE_DIR = COUNTRY_ARTISTS_DIR / "intermediate"
JSON_DIR = ROOT / "data" / "intermediate" / "json"
JSON_CACHE_DIR = INTERMEDIATE_DIR / "json_caches"

CODE_FILES = {
    "country_only_ultimate_guitar.py": ROOT / "execution/step1_download/country_only_ultimate_guitar.py",
    "supplement_billboard_country_chords.py": ROOT / "execution/step1_download/supplement_billboard_country_chords.py",
    "build_country_only_chords_final.py": ROOT / "execution/step2_digitalize/build_country_only_chords_final.py",
    "build_country_artists_dataset.py": ROOT / "execution/step4_country_artists/build_country_artists_dataset.py",
    "enrich_artist_universe_missing_metadata.py": ROOT / "execution/step4_country_artists/enrich_artist_universe_missing_metadata.py",
    "run_full_replication.py": ROOT / "execution/step5_replication/run_full_replication.py",
    "run_country_songs_replication.py": ROOT / "execution/step5_replication/run_country_songs_replication.py",
    "run_country_merge_v6_replication.py": ROOT / "execution/step5_replication/run_country_merge_v6_replication.py",
    "run_artist_universe_replication.py": ROOT / "execution/step5_replication/run_artist_universe_replication.py",
}

ASSET_FILES = {
    "artist_universe_qc_report.md": COUNTRY_ARTISTS_DIR / "artist_universe_qc_report.md",
    "country_artists_qc_report.md": COUNTRY_ARTISTS_DIR / "country_artists_qc_report.md",
    "billboard_country_chords_supplement_report_2026_04_08.json": INTERMEDIATE_DIR / "billboard_country_chords_supplement_report_2026_04_08.json",
    "ug_country_only_download_plan.json": JSON_DIR / "ug_country_only_download_plan.json",
}


def ensure_dirs() -> None:
    for path in [REPORT_DIR, GENERATED_DIR, SNIPPETS_DIR, CODE_DIR, ASSETS_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def copy_inputs() -> None:
    for name, src in CODE_FILES.items():
        if src.exists():
            shutil.copy2(src, CODE_DIR / name)
    for name, src in ASSET_FILES.items():
        if src.exists():
            shutil.copy2(src, ASSETS_DIR / name)


def latex_escape(text: object) -> str:
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


def fmt_int(value: int | float) -> str:
    return f"{int(value):,}"


def make_table_tex(df: pd.DataFrame, caption: str, label: str) -> str:
    columns = list(df.columns)
    align = "l" + "c" * (len(columns) - 1)
    header = " & ".join(latex_escape(col) for col in columns) + r" \\"
    rows: list[str] = []
    for _, row in df.iterrows():
        rows.append(" & ".join(latex_escape(row[col]) for col in columns) + r" \\")
    body = "\n".join(rows)
    return (
        "\\begin{table}[H]\n\\centering\n"
        f"\\caption{{{latex_escape(caption)}}}\n"
        f"\\label{{{label}}}\n"
        f"\\begin{{tabular}}{{{align}}}\n"
        "\\toprule\n"
        f"{header}\n"
        "\\midrule\n"
        f"{body}\n"
        "\\bottomrule\n"
        "\\end{tabular}\n"
        "\\end{table}\n"
    )


def extract_md_count(path: Path, label: str) -> int:
    text = path.read_text(encoding="utf-8")
    match = re.search(rf"- {re.escape(label)}: ([0-9]+)", text)
    if not match:
        raise ValueError(f"Could not find label '{label}' in {path}")
    return int(match.group(1))


def extract_top_level_block(path: Path, start_token: str, end_token: str | None = None) -> str:
    lines = path.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith(start_token))
    if end_token is not None:
        end = next(i for i in range(start + 1, len(lines)) if lines[i].startswith(end_token))
        return "\n".join(lines[start:end]).rstrip() + "\n"

    end = len(lines)
    for i in range(start + 1, len(lines)):
        if lines[i].startswith("def ") or lines[i].startswith("class ") or lines[i].startswith("if __name__ =="):
            end = i
            break
    return "\n".join(lines[start:end]).rstrip() + "\n"


def write_snippet(name: str, content: str) -> None:
    (SNIPPETS_DIR / name).write_text(content, encoding="utf-8")


def build_snippets() -> None:
    build_artists = CODE_FILES["build_country_artists_dataset.py"]
    enrich_artists = CODE_FILES["enrich_artist_universe_missing_metadata.py"]
    ug = CODE_FILES["country_only_ultimate_guitar.py"]
    supplement = CODE_FILES["supplement_billboard_country_chords.py"]
    final_builder = CODE_FILES["build_country_only_chords_final.py"]
    repl = CODE_FILES["run_full_replication.py"]

    write_snippet("artist_prepare_dataset.py", extract_top_level_block(build_artists, "def prepare_dataset"))
    write_snippet(
        "artist_resolve_birth_geography.py",
        extract_top_level_block(build_artists, "def resolve_birth_geography", end_token="def normalize_country_genres"),
    )
    write_snippet(
        "artist_targeted_musicbrainz.py",
        extract_top_level_block(build_artists, "def targeted_musicbrainz_bio_enrichment", end_token="def page_links"),
    )
    write_snippet(
        "artist_country_only_direct.py",
        extract_top_level_block(enrich_artists, "def enrich_country_only_direct", end_token="def write_qc_report"),
    )
    write_snippet(
        "artist_resolve_missing_qids.py",
        extract_top_level_block(enrich_artists, "def resolve_missing_qids_from_names", end_token="def enrich_from_wikipedia_sources"),
    )
    write_snippet(
        "artist_discogs_official.py",
        extract_top_level_block(enrich_artists, "def enrich_with_discogs_and_itunes", end_token="def apply_discogs_itunes_merge")
        + "\n"
        + extract_top_level_block(enrich_artists, "def enrich_from_official_websites", end_token="def generic_musicbrainz_area_context"),
    )
    write_snippet("ug_discovery.py", extract_top_level_block(ug, "def run_discovery", end_token="def extract_existing_ids"))
    write_snippet(
        "ug_plan_download_cli.py",
        extract_top_level_block(ug, "def build_plan", end_token="def main")
        + "\n"
        + extract_top_level_block(ug, "def main", end_token='if __name__ == "__main__":'),
    )
    write_snippet("billboard_supplement_run.py", extract_top_level_block(supplement, "def run", end_token="def main"))
    write_snippet(
        "final_build_new_song_rows.py",
        extract_top_level_block(final_builder, "def build_new_song_rows", end_token="def deduplicate_new_song_cache"),
    )
    write_snippet(
        "final_enrich_release_years.py",
        extract_top_level_block(final_builder, "def enrich_release_years", end_token="def wikipedia_search_titles"),
    )
    write_snippet(
        "final_build_dataset.py",
        extract_top_level_block(final_builder, "def build_chart_cache", end_token="def save_dta_with_labels")
        + "\n"
        + extract_top_level_block(final_builder, "def build_final_dataset", end_token="def save_dta_with_labels"),
    )
    write_snippet(
        "replication_run_full.py",
        extract_top_level_block(repl, "def run_country_pipeline", end_token="def main")
        + "\n"
        + extract_top_level_block(repl, "def main", end_token='if __name__ == "__main__":'),
    )


def build_metrics() -> dict[str, object]:
    artist_universe_report = COUNTRY_ARTISTS_DIR / "artist_universe_qc_report.md"

    base_country_core = extract_md_count(artist_universe_report, "core country-union artists")
    adjacent_only = extract_md_count(artist_universe_report, "adjacent-only artists added")
    full_universe = extract_md_count(artist_universe_report, "full artist universe")

    country_only_df = pd.read_csv(COUNTRY_ARTISTS_DIR / "artist_universe_country_only.csv", low_memory=False)
    final_df = pd.read_csv(COUNTRY_ARTISTS_DIR / "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv", low_memory=False)
    new_songs_df = pd.read_csv(INTERMEDIATE_DIR / "country_only_chords_new_songs_2026_04_08.csv", low_memory=False)

    ug_plan = json.loads((JSON_DIR / "ug_country_only_download_plan.json").read_text(encoding="utf-8"))
    supplement_report = json.loads((INTERMEDIATE_DIR / "billboard_country_chords_supplement_report_2026_04_08.json").read_text(encoding="utf-8"))

    current_missing = {
        "birth_year": int(pd.to_numeric(country_only_df["birth_year"], errors="coerce").isna().sum()),
        "birth_city": int(country_only_df["birth_city"].fillna("").astype(str).str.strip().eq("").sum()),
        "birth_state": int(country_only_df["birth_state"].fillna("").astype(str).str.strip().eq("").sum()),
        "birth_country": int(country_only_df["birth_country"].fillna("").astype(str).str.strip().eq("").sum()),
        "wikidata_qid": int(country_only_df["wikidata_qid"].fillna("").astype(str).str.strip().eq("").sum()),
        "musicbrainz_mbid": int(country_only_df["musicbrainz_mbid"].fillna("").astype(str).str.strip().eq("").sum()),
    }

    return {
        "base_country_core": base_country_core,
        "adjacent_only": adjacent_only,
        "full_universe": full_universe,
        "current_country_only_rows": int(len(country_only_df)),
        "current_added_billboard_artists": int(len(country_only_df) - base_country_core),
        "final_rows": int(len(final_df)),
        "new_song_rows": int(len(new_songs_df)),
        "current_missing": current_missing,
        "ug_plan": ug_plan,
        "supplement_report": supplement_report,
        "release_cache_files": sorted(path.name for path in JSON_CACHE_DIR.glob("*release*cache*.json"))
        + sorted(path.name for path in JSON_CACHE_DIR.glob("artist_release_track_cache_country_only*.json")),
    }


def build_macros(metrics: dict[str, object]) -> None:
    ug_plan = metrics["ug_plan"]
    supplement = metrics["supplement_report"]
    current_missing = metrics["current_missing"]

    macros = {
        "BaseCountryCoreArtists": fmt_int(metrics["base_country_core"]),
        "AdjacentOnlyArtists": fmt_int(metrics["adjacent_only"]),
        "FullArtistUniverse": fmt_int(metrics["full_universe"]),
        "CurrentCountryOnlyArtists": fmt_int(metrics["current_country_only_rows"]),
        "BillboardAddedArtists": fmt_int(metrics["current_added_billboard_artists"]),
        "CurrentFinalChordRows": fmt_int(metrics["final_rows"]),
        "CurrentNewSongRows": fmt_int(metrics["new_song_rows"]),
        "UgArtistsTargeted": fmt_int(ug_plan["artists_targeted"]),
        "UgChordUniqueSongs": fmt_int(ug_plan["chords"]["unique_songs"]),
        "UgChordJsonTotal": fmt_int(ug_plan["chords"]["json_total"]),
        "UgChordJsonExisting": fmt_int(ug_plan["chords"]["json_existing"]),
        "UgChordJsonMissing": fmt_int(ug_plan["chords"]["json_missing"]),
        "UgTabUniqueSongs": fmt_int(ug_plan["tabs"]["unique_songs"]),
        "UgTabJsonTotal": fmt_int(ug_plan["tabs"]["json_total"]),
        "UgTabJsonExisting": fmt_int(ug_plan["tabs"]["json_existing"]),
        "UgTabJsonMissing": fmt_int(ug_plan["tabs"]["json_missing"]),
        "SupplementAddedArtists": fmt_int(supplement["added_artists"]),
        "SupplementTargetSongs": fmt_int(supplement["target_songs"]),
        "SupplementExistingRows": fmt_int(supplement["existing_discovery_rows"]),
        "SupplementRows": fmt_int(supplement["supplement_discovery_rows"]),
        "SupplementFinalRows": fmt_int(supplement["final_discovery_rows"]),
        "SupplementNewDownloads": fmt_int(supplement["new_download_ids"]),
        "SupplementAttemptedTargetKeys": fmt_int(supplement["attempted_target_keys"]),
        "MissingBirthYear": fmt_int(current_missing["birth_year"]),
        "MissingBirthCity": fmt_int(current_missing["birth_city"]),
        "MissingBirthState": fmt_int(current_missing["birth_state"]),
        "MissingBirthCountry": fmt_int(current_missing["birth_country"]),
        "MissingWikidataQid": fmt_int(current_missing["wikidata_qid"]),
        "MissingMusicbrainzMbid": fmt_int(current_missing["musicbrainz_mbid"]),
    }

    content = "\n".join(
        f"\\newcommand{{\\{name}}}{{{value}}}" for name, value in macros.items()
    ) + "\n"
    (GENERATED_DIR / "macros.tex").write_text(content, encoding="utf-8")


def build_tables(metrics: dict[str, object]) -> None:
    ug_plan = metrics["ug_plan"]
    supplement = metrics["supplement_report"]
    current_missing = metrics["current_missing"]

    script_map = pd.DataFrame(
        [
            {
                "Script": "build_country_artists_dataset.py",
                "Role": "Builds the base country-only, adjacent-only and union artist universes",
                "Main outputs": "country_artists_*.csv/.dta and QC report",
            },
            {
                "Script": "enrich_artist_universe_missing_metadata.py",
                "Role": "Top-up pass for residual missing birth metadata in artist universes",
                "Main outputs": "artist_universe_country_only.csv/.dta plus checkpoints",
            },
            {
                "Script": "country_only_ultimate_guitar.py",
                "Role": "Discovery, planning and raw JSON download from Ultimate Guitar",
                "Main outputs": "discovery JSONs, plan JSON, raw_tabs_country, raw_country_tabs",
            },
            {
                "Script": "supplement_billboard_country_chords.py",
                "Role": "Adds Billboard artists and missing chart songs not covered by broad UG discovery",
                "Main outputs": "supplement discovery JSON and run report",
            },
            {
                "Script": "build_country_only_chords_final.py",
                "Role": "Merges v6 base, new UG chord rows, chart flags and release-year backfill",
                "Main outputs": "Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv/.dta",
            },
            {
                "Script": "run_full_replication.py",
                "Role": "Cold-start or snapshot-assisted replication wrapper for country artists",
                "Main outputs": "replication package with code, directives and datasets",
            },
            {
                "Script": "run_country_songs_replication.py",
                "Role": "Restores or snapshots the upstream country-song deliverables and caches",
                "Main outputs": "song replication package",
            },
            {
                "Script": "run_country_merge_v6_replication.py",
                "Role": "Restores or rebuilds the restricted final v6 bridge used upstream",
                "Main outputs": "merge-v6 replication package",
            },
        ]
    )
    (GENERATED_DIR / "script_map_table.tex").write_text(
        make_table_tex(script_map, "Key scripts in the country-only pipeline", "tab:script_map"),
        encoding="utf-8",
    )

    counts = pd.DataFrame(
        [
            {"Artifact": "Base country-only artist universe", "Rows / objects": fmt_int(metrics["base_country_core"]), "Comment": "Country-union core before Billboard additions"},
            {"Artifact": "Adjacent-only artist universe", "Rows / objects": fmt_int(metrics["adjacent_only"]), "Comment": "Adjacent-genre expansion kept separate from the core country list"},
            {"Artifact": "Full artist universe", "Rows / objects": fmt_int(metrics["full_universe"]), "Comment": "Country core plus adjacent-only union"},
            {"Artifact": "Current artist_universe_country_only.csv", "Rows / objects": fmt_int(metrics["current_country_only_rows"]), "Comment": "Current country-only file after Billboard additions and direct missing-data top-up"},
            {"Artifact": "Current new chord-song cache", "Rows / objects": fmt_int(metrics["new_song_rows"]), "Comment": "Songs prepared for append after deduplication and raw JSON parsing"},
            {"Artifact": "Current final country-only chord dataset", "Rows / objects": fmt_int(metrics["final_rows"]), "Comment": "Final chord-level dataset used in the exploratory report"},
        ]
    )
    (GENERATED_DIR / "artifact_counts_table.tex").write_text(
        make_table_tex(counts, "Current main artifacts produced by the pipeline", "tab:artifact_counts"),
        encoding="utf-8",
    )

    missing_df = pd.DataFrame(
        [
            {"Variable": "birth_year", "Remaining missing in current country-only universe": fmt_int(current_missing["birth_year"])},
            {"Variable": "birth_city", "Remaining missing in current country-only universe": fmt_int(current_missing["birth_city"])},
            {"Variable": "birth_state", "Remaining missing in current country-only universe": fmt_int(current_missing["birth_state"])},
            {"Variable": "birth_country", "Remaining missing in current country-only universe": fmt_int(current_missing["birth_country"])},
            {"Variable": "wikidata_qid", "Remaining missing in current country-only universe": fmt_int(current_missing["wikidata_qid"])},
            {"Variable": "musicbrainz_mbid", "Remaining missing in current country-only universe": fmt_int(current_missing["musicbrainz_mbid"])},
        ]
    )
    (GENERATED_DIR / "missingness_table.tex").write_text(
        make_table_tex(missing_df, "Current residual missingness in artist_universe_country_only.csv", "tab:missingness"),
        encoding="utf-8",
    )

    ug_df = pd.DataFrame(
        [
            {
                "Layer": "UG chords",
                "Artists / songs / JSON": fmt_int(ug_plan["artists_targeted"]) + " artists",
                "Coverage snapshot": f"{fmt_int(ug_plan['chords']['unique_songs'])} songs; {fmt_int(ug_plan['chords']['json_total'])} JSONs total; {fmt_int(ug_plan['chords']['json_missing'])} missing",
            },
            {
                "Layer": "UG tabs",
                "Artists / songs / JSON": fmt_int(ug_plan["artists_targeted"]) + " artists",
                "Coverage snapshot": f"{fmt_int(ug_plan['tabs']['unique_songs'])} songs; {fmt_int(ug_plan['tabs']['json_total'])} JSONs total; {fmt_int(ug_plan['tabs']['json_missing'])} missing",
            },
        ]
    )
    (GENERATED_DIR / "ug_plan_table.tex").write_text(
        make_table_tex(ug_df, "Saved Ultimate Guitar discovery/download plan snapshot", "tab:ug_plan"),
        encoding="utf-8",
    )

    supplement_df = pd.DataFrame(
        [
            {"Metric": "Added artists in supplement", "Value": fmt_int(supplement["added_artists"])},
            {"Metric": "Target songs in executed run report", "Value": fmt_int(supplement["target_songs"])},
            {"Metric": "Existing discovery rows before supplement", "Value": fmt_int(supplement["existing_discovery_rows"])},
            {"Metric": "Rows added by supplement", "Value": fmt_int(supplement["supplement_discovery_rows"])},
            {"Metric": "Discovery rows after merge", "Value": fmt_int(supplement["final_discovery_rows"])},
            {"Metric": "New raw chord JSON downloads", "Value": fmt_int(supplement["new_download_ids"])},
            {"Metric": "Attempted target keys saved", "Value": fmt_int(supplement["attempted_target_keys"])},
        ]
    )
    (GENERATED_DIR / "supplement_table.tex").write_text(
        make_table_tex(supplement_df, "Saved Billboard supplement run report", "tab:supplement"),
        encoding="utf-8",
    )

    cache_rows = [{"Release-year cache file": name} for name in metrics["release_cache_files"]]
    cache_df = pd.DataFrame(cache_rows)
    (GENERATED_DIR / "release_caches_table.tex").write_text(
        make_table_tex(cache_df, "Release-year caches consulted by the final chord builder", "tab:release_caches"),
        encoding="utf-8",
    )


def main() -> None:
    ensure_dirs()
    copy_inputs()
    build_snippets()
    metrics = build_metrics()
    build_macros(metrics)
    build_tables(metrics)


if __name__ == "__main__":
    main()
