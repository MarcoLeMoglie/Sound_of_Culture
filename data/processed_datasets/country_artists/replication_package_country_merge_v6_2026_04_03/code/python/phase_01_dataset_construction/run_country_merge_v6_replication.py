import os
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd


PACKAGE_DIRNAME = "replication_package_country_merge_v6_2026_04_03"
ROOT_DATA_DIR = Path("data/processed_datasets/country_artists")
BACKFILL_SUBDIR = Path("intermediate/restricted_final_v6_backfill")
CACHE_SUBDIR = Path("intermediate/json_caches")

SONG_INPUTS = [
    "Sound_of_Culture_Country_Full.csv",
    "Sound_of_Culture_Country_Full.dta",
    "Sound_of_Culture_Country_Full_Enriched.csv",
    "Sound_of_Culture_Country_Full_Enriched.dta",
    "Sound_of_Culture_Country_Full_Enriched_v2.csv",
    "Sound_of_Culture_Country_Full_Enriched_v2.dta",
    "Sound_of_Culture_Country_Full_Enriched_v3.csv",
    "Sound_of_Culture_Country_Full_Enriched_v3.dta",
    "Sound_of_Culture_Country_Full_Enriched_v5.csv",
    "Sound_of_Culture_Country_Full_Enriched_v5.dta",
]

ARTIST_INPUTS = [
    "country_artists_master.csv",
    "country_artists_master.dta",
    "country_artists_restricted.csv",
    "country_artists_restricted.dta",
    "country_artists_expanded.csv",
    "country_artists_expanded.dta",
    "country_artists_excluded_or_non_us.csv",
    "country_artists_excluded_or_non_us.dta",
    "manual_review_queue.csv",
    "manual_review_queue.dta",
    "country_artists_data_dictionary.csv",
    "country_artists_data_dictionary.dta",
    "country_artists_sources.csv",
    "country_artists_sources.dta",
]

FINAL_OUTPUTS = [
    "Sound_of_Culture_Country_Restricted_Final_v6.csv",
    "Sound_of_Culture_Country_Restricted_Final_v6.dta",
]

CACHE_FILES = [
    "release_years_cache.json",
    "release_years_cache_fuzzy.json",
    "release_years_cache_discogs.json",
    "release_years_cache_internet.json",
    "release_years_cache_wiki.json",
    "release_years_cache_restricted_final_v6.json",
]

BACKFILL_FILES = [
    "country_artists_musicbrainz_bio.csv",
    "country_artists_wikipedia_enrichment.csv",
    "country_artists_wikipedia_lead.csv",
    "targeted_wikidata_details.csv",
    "precomputed_targeted_live_lookup.csv",
]

MERGE_SCRIPT_ORDER = [
    "final_merge_restricted_v2.py",
    "backfill_restricted_final_v6_demographics.py",
    "enrich_release_years_restricted_final_v6.py",
]


def current_script_path() -> Path:
    if "__file__" in globals():
        return Path(__file__).resolve()
    argv0 = Path(sys.argv[0])
    if argv0.exists():
        return argv0.resolve()
    return (Path.cwd() / argv0).resolve()


def parse_runtime_args(argv: list[str]) -> tuple[Path, str, bool]:
    project_root = None
    package_dirname = PACKAGE_DIRNAME
    full_rebuild = False
    for arg in argv[1:]:
        if arg == "--full-rebuild":
            full_rebuild = True
        elif project_root is None:
            project_root = Path(arg).resolve()
        elif package_dirname == PACKAGE_DIRNAME:
            package_dirname = arg
    return project_root or Path.cwd().resolve(), package_dirname, full_rebuild


def package_root_from_script(script_path: Path, project_root: Path, package_dirname: str) -> Path:
    expected = project_root / ROOT_DATA_DIR / package_dirname
    return expected


def copy_file(src: Path, dst: Path) -> None:
    if src.resolve() == dst.resolve():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def ensure_structure(package_root: Path) -> None:
    for path in [
        package_root,
        package_root / "code" / "python" / "phase_01_dataset_construction",
        package_root / "code" / "stata",
        package_root / "docs" / "directives_snapshot",
        package_root / "datasets" / "upstream" / "country_songs",
        package_root / "datasets" / "upstream" / "country_artists" / "final",
        package_root / "datasets" / "upstream" / "country_artists" / BACKFILL_SUBDIR,
        package_root / "datasets" / "final",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def copy_python_tree(project_root: Path, package_root: Path) -> None:
    src_dir = project_root / "execution" / "phase_01_dataset_construction"
    dst_dir = package_root / "code" / "python" / "phase_01_dataset_construction"
    dst_dir.mkdir(parents=True, exist_ok=True)
    for name in [
        "final_merge_restricted_v2.py",
        "backfill_restricted_final_v6_demographics.py",
        "enrich_release_years_restricted_final_v6.py",
        "run_country_merge_v6_replication.py",
    ]:
        src = src_dir / name
        if src.exists():
            copy_file(src, dst_dir / name)


def copy_directives(project_root: Path, package_root: Path) -> None:
    dst_dir = package_root / "docs" / "directives_snapshot"
    for rel in [
        "Gemini.md",
        "directives/02_digitalize_tablatures.md",
        "directives/04_build_country_artists_dataset.md",
        "directives/05_replication_package.md",
        "directives/06_replication_package_country_merge_v6.md",
    ]:
        src = project_root / rel
        if src.exists():
            copy_file(src, dst_dir / src.name)

    extra_dir = project_root / "directives" / "costruzione dataset artisti country"
    if extra_dir.exists():
        for path in sorted(extra_dir.iterdir()):
            if path.is_file():
                copy_file(path, dst_dir / path.name)


def copy_assets(project_root: Path, package_root: Path) -> None:
    mapping = [
        (
            project_root / "execution" / "phase_01_dataset_construction" / "run_country_merge_v6_replication.py",
            package_root / "code" / "python" / "phase_01_dataset_construction" / "run_country_merge_v6_replication.py",
        ),
        (
            project_root / "execution" / "phase_01_dataset_construction" / "do" / "run_country_merge_v6_replication.do",
            package_root / "code" / "stata" / "run_full_replication.do",
        ),
        (
            project_root / "execution" / "step5_replication" / "country_merge_v6_replication_README.md",
            package_root / "README.md",
        ),
    ]
    for src, dst in mapping:
        if src.exists():
            copy_file(src, dst)


def build_precomputed_backfill_lookup(project_root: Path) -> None:
    root_dir = project_root / ROOT_DATA_DIR
    final_csv = root_dir / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
    songs_v5_csv = root_dir / "Sound_of_Culture_Country_Full_Enriched_v5.csv"
    if not final_csv.exists() or not songs_v5_csv.exists():
        return

    final_df = pd.read_csv(final_csv, low_memory=False)
    songs_df = pd.read_csv(songs_v5_csv, low_memory=False, nrows=3)
    song_columns = set(songs_df.columns)
    artist_columns = [col for col in final_df.columns if col not in song_columns and col != "artist_name"]
    if not artist_columns:
        return

    scored = final_df[["artist_name"] + artist_columns].copy()
    scored["__score"] = scored[artist_columns].apply(
        lambda row: int(
            sum(
                1
                for value in row
                if pd.notna(value) and str(value).strip() not in {"", "nan", "None"}
            )
        ),
        axis=1,
    )
    scored = (
        scored.sort_values(["artist_name", "__score"], ascending=[True, False])
        .drop_duplicates(subset=["artist_name"], keep="first")
        .drop(columns="__score")
        .rename(columns={"artist_name": "target_artist_name"})
    )

    backfill_dir = root_dir / BACKFILL_SUBDIR
    backfill_dir.mkdir(parents=True, exist_ok=True)
    scored.to_csv(backfill_dir / "precomputed_targeted_live_lookup.csv", index=False, encoding="utf-8")


def copy_upstream_snapshots(project_root: Path, package_root: Path) -> None:
    root_dir = project_root / ROOT_DATA_DIR
    build_precomputed_backfill_lookup(project_root)

    for name in SONG_INPUTS:
        src = root_dir / name
        if src.exists():
            copy_file(src, package_root / "datasets" / "upstream" / "country_songs" / name)

    artist_dst = package_root / "datasets" / "upstream" / "country_artists" / "final"
    for name in ARTIST_INPUTS:
        src = root_dir / name
        if src.exists():
            copy_file(src, artist_dst / name)

    backfill_src_dir = root_dir / BACKFILL_SUBDIR
    backfill_dst_dir = package_root / "datasets" / "upstream" / "country_artists" / BACKFILL_SUBDIR
    if backfill_src_dir.exists():
        for name in BACKFILL_FILES:
            src = backfill_src_dir / name
            if src.exists():
                copy_file(src, backfill_dst_dir / name)

    cache_src_dir = root_dir / CACHE_SUBDIR
    for name in CACHE_FILES:
        src = cache_src_dir / name
        if src.exists():
            copy_file(src, package_root / "datasets" / "upstream" / "country_artists" / name)

    for name in FINAL_OUTPUTS:
        src = root_dir / name
        if src.exists():
            copy_file(src, package_root / "datasets" / "final" / name)


def restore_upstream_snapshots(project_root: Path, package_root: Path) -> None:
    root_dir = project_root / ROOT_DATA_DIR
    root_dir.mkdir(parents=True, exist_ok=True)

    for name in SONG_INPUTS:
        src = package_root / "datasets" / "upstream" / "country_songs" / name
        if src.exists():
            copy_file(src, root_dir / name)

    for name in ARTIST_INPUTS:
        src = package_root / "datasets" / "upstream" / "country_artists" / "final" / name
        if src.exists():
            copy_file(src, root_dir / name)

    backfill_dst = root_dir / BACKFILL_SUBDIR
    backfill_dst.mkdir(parents=True, exist_ok=True)
    for name in BACKFILL_FILES:
        src = package_root / "datasets" / "upstream" / "country_artists" / BACKFILL_SUBDIR / name
        if src.exists():
            copy_file(src, backfill_dst / name)

    cache_dst = root_dir / CACHE_SUBDIR
    cache_dst.mkdir(parents=True, exist_ok=True)
    for name in CACHE_FILES:
        src = package_root / "datasets" / "upstream" / "country_artists" / name
        if src.exists():
            copy_file(src, cache_dst / name)

    print("Restored bundled upstream inputs for merge-to-v6 replication.", flush=True)


def run(cmd: list[str], cwd: Path, *, env: dict[str, str] | None = None) -> None:
    print("\n>>>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=str(cwd), check=True, env=env)


def script_path_for(project_root: Path, package_root: Path, script_name: str) -> str:
    canonical_script = project_root / "execution" / "phase_01_dataset_construction" / script_name
    if canonical_script.exists():
        return str(canonical_script.relative_to(project_root))
    packaged_script = package_root / "code" / "python" / "phase_01_dataset_construction" / script_name
    return str(packaged_script)


def verify_outputs(project_root: Path, package_root: Path) -> None:
    root_csv = project_root / ROOT_DATA_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
    root_dta = project_root / ROOT_DATA_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.dta"
    if not root_csv.exists() or not root_dta.exists():
        raise FileNotFoundError("Final restricted v6 outputs are missing after replication.")

    df = pd.read_csv(root_csv, low_memory=False)
    if len(df) == 0:
        raise ValueError("Final restricted v6 CSV is empty.")
    missing_names = (
        int(df["name_primary"].fillna("").astype(str).str.strip().eq("").sum())
        if "name_primary" in df.columns
        else 0
    )
    missing_release = (
        int(pd.to_numeric(df["release_year"], errors="coerce").isna().sum())
        if "release_year" in df.columns
        else 0
    )
    if missing_names != 0:
        raise ValueError(f"name_primary still missing in {missing_names} rows.")

    bundled_final = package_root / "datasets" / "final" / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
    if bundled_final.exists():
        bundled_df = pd.read_csv(bundled_final, low_memory=False)
        if len(df) != len(bundled_df):
            raise ValueError("Replicated final CSV row count does not match the bundled canonical snapshot.")
        bundled_missing_names = (
            int(bundled_df["name_primary"].fillna("").astype(str).str.strip().eq("").sum())
            if "name_primary" in bundled_df.columns
            else 0
        )
        bundled_missing_release = (
            int(pd.to_numeric(bundled_df["release_year"], errors="coerce").isna().sum())
            if "release_year" in bundled_df.columns
            else 0
        )
        if missing_names != bundled_missing_names:
            raise ValueError("Replicated final CSV does not match bundled name_primary completeness.")
        if missing_release != bundled_missing_release:
            raise ValueError("Replicated final CSV does not match bundled release_year completeness.")


def main() -> None:
    script_path = current_script_path()
    project_root, package_dirname, full_rebuild = parse_runtime_args(sys.argv)
    package_root = package_root_from_script(script_path, project_root, package_dirname)

    print(f"Project root: {project_root}", flush=True)
    print(f"Replication package target: {package_root}", flush=True)
    print(f"Full rebuild mode: {full_rebuild}", flush=True)

    ensure_structure(package_root)
    build_precomputed_backfill_lookup(project_root)
    restore_upstream_snapshots(project_root, package_root)
    for script_name in MERGE_SCRIPT_ORDER:
        env = None
        if script_name == "enrich_release_years_restricted_final_v6.py":
            env = dict(os.environ, SOC_RELEASE_YEAR_CACHE_ONLY="1")
        run(["python3", script_path_for(project_root, package_root, script_name)], project_root, env=env)
    verify_outputs(project_root, package_root)
    copy_upstream_snapshots(project_root, package_root)
    copy_python_tree(project_root, package_root)
    copy_directives(project_root, package_root)
    copy_assets(project_root, package_root)

    print("Country merge-to-v6 replication completed successfully.", flush=True)


if __name__ == "__main__":
    main()
