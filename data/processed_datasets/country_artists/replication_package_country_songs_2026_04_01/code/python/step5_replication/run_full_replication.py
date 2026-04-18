import shutil
import sys
from pathlib import Path


PACKAGE_DIRNAME = "replication_package_country_songs_2026_04_01"
ROOT_DATA_DIR = Path("data/processed_datasets/country_artists")
CACHE_SUBDIR = Path("intermediate/json_caches")

SONG_OUTPUTS = [
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

CACHE_FILES = [
    "release_years_cache.json",
    "release_years_cache_fuzzy.json",
    "release_years_cache_discogs.json",
    "release_years_cache_internet.json",
    "release_years_cache_wiki.json",
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
        package_root / "code" / "python" / "step5_replication",
        package_root / "code" / "stata",
        package_root / "docs" / "directives_snapshot",
        package_root / "output",
        package_root / "data",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def copy_directives(project_root: Path, package_root: Path) -> None:
    dst_dir = package_root / "docs" / "directives_snapshot"
    for rel in ["Gemini.md", "directives/02_digitalize_tablatures.md", "directives/05_replication_package.md"]:
        src = project_root / rel
        if src.exists():
            copy_file(src, dst_dir / src.name)


def copy_assets(project_root: Path, package_root: Path) -> None:
    mapping = [
        (
            project_root / "execution" / "step5_replication" / "run_country_songs_replication.py",
            package_root / "code" / "python" / "step5_replication" / "run_full_replication.py",
        ),
        (
            project_root / "execution" / "step5_replication" / "run_country_songs_replication.do",
            package_root / "code" / "stata" / "run_full_replication.do",
        ),
        (
            project_root / "execution" / "step5_replication" / "run_country_songs_replication.do",
            package_root / "replicate_dataset.do",
        ),
        (
            project_root / "execution" / "step5_replication" / "country_songs_replication_README.md",
            package_root / "README.md",
        ),
    ]
    for src, dst in mapping:
        if src.exists():
            copy_file(src, dst)


def copy_current_outputs(project_root: Path, package_root: Path) -> None:
    root_dir = project_root / ROOT_DATA_DIR
    for name in SONG_OUTPUTS:
        src = root_dir / name
        if src.exists():
            copy_file(src, package_root / "output" / name)
    cache_src_dir = root_dir / CACHE_SUBDIR
    for name in CACHE_FILES:
        src = cache_src_dir / name
        if src.exists():
            copy_file(src, package_root / "data" / name)


def restore_bundled_outputs(project_root: Path, package_root: Path) -> None:
    root_dir = project_root / ROOT_DATA_DIR
    root_dir.mkdir(parents=True, exist_ok=True)
    for name in SONG_OUTPUTS:
        src = package_root / "output" / name
        if src.exists():
            copy_file(src, root_dir / name)
    cache_dst = root_dir / CACHE_SUBDIR
    cache_dst.mkdir(parents=True, exist_ok=True)
    for name in CACHE_FILES:
        src = package_root / "data" / name
        if src.exists():
            copy_file(src, cache_dst / name)
    print("Restored bundled country-song outputs.", flush=True)


def outputs_exist(project_root: Path) -> bool:
    root_dir = project_root / ROOT_DATA_DIR
    required = [
        root_dir / "Sound_of_Culture_Country_Full.csv",
        root_dir / "Sound_of_Culture_Country_Full.dta",
        root_dir / "Sound_of_Culture_Country_Full_Enriched_v5.csv",
        root_dir / "Sound_of_Culture_Country_Full_Enriched_v5.dta",
    ]
    return all(path.exists() for path in required)


def main() -> None:
    script_path = current_script_path()
    project_root, package_dirname, full_rebuild = parse_runtime_args(sys.argv)
    package_root = package_root_from_script(script_path, project_root, package_dirname)

    print(f"Project root: {project_root}", flush=True)
    print(f"Replication package target: {package_root}", flush=True)
    print(f"Full rebuild requested: {full_rebuild}", flush=True)

    ensure_structure(package_root)
    if not outputs_exist(project_root) or full_rebuild:
        restore_bundled_outputs(project_root, package_root)
    else:
        print("Country-song outputs already present; keeping canonical root copy and refreshing the package snapshot.", flush=True)

    copy_current_outputs(project_root, package_root)
    copy_assets(project_root, package_root)
    copy_directives(project_root, package_root)

    print("Country-song replication package completed successfully.", flush=True)


if __name__ == "__main__":
    main()
