import shutil
import sys
from pathlib import Path


PACKAGE_DIRNAME = "replication_package_country_artist_universe_2026_04_08"
ROOT_DATA_DIR = Path("data/processed_datasets/country_artists")

FINAL_OUTPUTS = [
    "artist_universe_country_only.csv",
    "artist_universe_country_only.dta",
    "artist_universe_adjacent_only.csv",
    "artist_universe_adjacent_only.dta",
    "artist_universe_country_plus_adjacent.csv",
    "artist_universe_country_plus_adjacent.dta",
    "artist_universe_qc_report.md",
    "artist_universe_methodological_note.md",
    "artist_universe_coverage_estimate_2026_04_07.csv",
    "artist_universe_coverage_estimate_2026_04_07.md",
]


def current_script_path() -> Path:
    if "__file__" in globals():
        return Path(__file__).resolve()
    argv0 = Path(sys.argv[0])
    if argv0.exists():
        return argv0.resolve()
    return (Path.cwd() / argv0).resolve()


def parse_runtime_args(argv: list[str]) -> tuple[Path, str]:
    project_root = None
    package_dirname = PACKAGE_DIRNAME
    for arg in argv[1:]:
        if project_root is None:
            project_root = Path(arg).resolve()
        elif package_dirname == PACKAGE_DIRNAME:
            package_dirname = arg
    return project_root or Path.cwd().resolve(), package_dirname


def package_root_from_script(script_path: Path, project_root: Path, package_dirname: str) -> Path:
    expected = project_root / ROOT_DATA_DIR / package_dirname
    if expected.exists():
        return expected
    return script_path.parents[2] / ROOT_DATA_DIR / package_dirname


def copy_file(src: Path, dst: Path) -> None:
    if src.resolve() == dst.resolve():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def ensure_structure(package_root: Path) -> None:
    for path in [
        package_root,
        package_root / "code" / "python" / "step4_country_artists",
        package_root / "code" / "python" / "step5_replication",
        package_root / "code" / "stata",
        package_root / "docs" / "directives_snapshot",
        package_root / "datasets" / "artist_universe" / "final",
    ]:
        path.mkdir(parents=True, exist_ok=True)


def copy_outputs(project_root: Path, package_root: Path) -> None:
    src_dir = project_root / ROOT_DATA_DIR
    dst_dir = package_root / "datasets" / "artist_universe" / "final"
    for name in FINAL_OUTPUTS:
        src = src_dir / name
        if src.exists():
            copy_file(src, dst_dir / name)


def restore_bundled_outputs(project_root: Path, package_root: Path) -> None:
    src_dir = package_root / "datasets" / "artist_universe" / "final"
    dst_dir = project_root / ROOT_DATA_DIR
    dst_dir.mkdir(parents=True, exist_ok=True)
    for path in src_dir.iterdir():
        if path.is_file():
            copy_file(path, dst_dir / path.name)
    print("Restored artist-universe outputs from bundled snapshot.", flush=True)


def copy_code(project_root: Path, package_root: Path) -> None:
    mapping = [
        (
            project_root / "execution" / "step5_replication" / "run_artist_universe_replication.py",
            package_root / "code" / "python" / "step5_replication" / "run_full_replication.py",
        ),
        (
            project_root / "execution" / "step5_replication" / "run_artist_universe_replication.do",
            package_root / "code" / "stata" / "run_full_replication.do",
        ),
        (
            project_root / "execution" / "step5_replication" / "artist_universe_replication_README.md",
            package_root / "README.md",
        ),
        (
            project_root / "execution" / "step4_country_artists" / "enrich_artist_universe_missing_metadata.py",
            package_root / "code" / "python" / "step4_country_artists" / "enrich_artist_universe_missing_metadata.py",
        ),
    ]
    for src, dst in mapping:
        if src.exists():
            copy_file(src, dst)


def copy_directives(project_root: Path, package_root: Path) -> None:
    dst_dir = package_root / "docs" / "directives_snapshot"
    for rel in [
        "Gemini.md",
        "directives/04_build_country_artists_dataset.md",
        "directives/05_replication_package.md",
        "directives/07_replication_package_artist_universe.md",
    ]:
        src = project_root / rel
        if src.exists():
            copy_file(src, dst_dir / src.name)


def outputs_exist(project_root: Path) -> bool:
    return all((project_root / ROOT_DATA_DIR / name).exists() for name in FINAL_OUTPUTS[:6])


def main() -> None:
    script_path = current_script_path()
    project_root, package_dirname = parse_runtime_args(sys.argv)
    package_root = package_root_from_script(script_path, project_root, package_dirname)

    print(f"Project root: {project_root}", flush=True)
    print(f"Replication package target: {package_root}", flush=True)

    ensure_structure(package_root)

    if not outputs_exist(project_root):
        restore_bundled_outputs(project_root, package_root)

    copy_outputs(project_root, package_root)
    copy_code(project_root, package_root)
    copy_directives(project_root, package_root)
    print("Artist-universe replication package completed successfully.", flush=True)


if __name__ == "__main__":
    main()
