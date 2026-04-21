import os
import shutil
import subprocess
import sys
from pathlib import Path


PACKAGE_DIRNAME = "replication_package_country_artists_2026_04_02"


def run(cmd, cwd: Path) -> None:
    print("\n>>>", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=str(cwd), check=True)


def copy_file(src: Path, dst: Path) -> None:
    if src.resolve() == dst.resolve():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def package_root_from_script(script_path: Path, project_root: Path, package_dirname: str) -> Path:
    expected = project_root / "data" / "processed_datasets" / "country_artists" / package_dirname
    if expected.exists():
        return expected
    return script_path.parents[3]


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


def copy_python_tree(project_root: Path, package_root: Path) -> None:
    src_dir = project_root / "execution" / "phase_01_dataset_construction"
    dst_dir = package_root / "code" / "python" / "phase_01_dataset_construction"
    if not src_dir.exists():
        src_dir = dst_dir
    dst_dir.mkdir(parents=True, exist_ok=True)
    for name in ["build_country_artists_dataset.py", "run_full_replication.py"]:
        path = src_dir / name
        if path.exists():
            copy_file(path, dst_dir / path.name)


def copy_directives(project_root: Path, package_root: Path) -> None:
    dst_dir = package_root / "docs" / "directives_snapshot"
    dst_dir.mkdir(parents=True, exist_ok=True)
    files = [
        "Gemini.md",
        "directives/04_build_country_artists_dataset.md",
        "directives/05_replication_package.md",
        "directives/MIGRATE_GITHUB.md",
    ]
    for rel in files:
        src = project_root / rel
        if not src.exists():
            src = dst_dir / Path(rel).name
        if src.exists():
            copy_file(src, dst_dir / src.name)

    extra_dir = project_root / "directives" / "costruzione dataset artisti country"
    if not extra_dir.exists():
        extra_dir = dst_dir
    if extra_dir.exists():
        for path in sorted(extra_dir.iterdir()):
            if path.is_file():
                copy_file(path, dst_dir / path.name)


def copy_replication_assets(project_root: Path, package_root: Path) -> None:
    assets = [
        (
            project_root / "execution" / "phase_01_dataset_construction" / "do" / "run_country_artists_replication.do",
            package_root / "code" / "stata" / "run_full_replication.do",
            package_root / "code" / "stata" / "run_full_replication.do",
        ),
        (
            project_root / "execution" / "step5_replication" / "country_artists_replication_README.md",
            package_root / "README.md",
            package_root / "README.md",
        ),
    ]
    for primary_src, dst, fallback_src in assets:
        src = primary_src if primary_src.exists() else fallback_src
        if src.exists():
            copy_file(src, dst)


def copy_country_outputs(project_root: Path, package_root: Path) -> None:
    final_dir = package_root / "datasets" / "country_artists" / "final"
    intermediate_dir = package_root / "datasets" / "country_artists" / "intermediate"
    final_dir.mkdir(parents=True, exist_ok=True)
    intermediate_dir.mkdir(parents=True, exist_ok=True)

    final_files = [
        "country_artists_master.csv",
        "country_artists_master.dta",
        "country_artists_restricted.csv",
        "country_artists_restricted.dta",
        "country_artists_expanded.csv",
        "country_artists_expanded.dta",
        "country_artists_excluded_or_non_us.csv",
        "country_artists_excluded_or_non_us.dta",
        "country_artists_sources.csv",
        "country_artists_sources.dta",
        "country_artists_data_dictionary.csv",
        "country_artists_data_dictionary.dta",
        "manual_review_queue.csv",
        "manual_review_queue.dta",
        "country_artists_qc_report.md",
    ]

    for name in final_files:
        src = project_root / "data" / "processed_datasets" / "country_artists" / name
        if src.exists():
            copy_file(src, final_dir / name)

    src_intermediate = project_root / "data" / "processed_datasets" / "country_artists" / "intermediate"
    if src_intermediate.exists():
        for path in sorted(src_intermediate.iterdir()):
            if path.is_file():
                copy_file(path, intermediate_dir / path.name)


def country_outputs_exist(project_root: Path) -> bool:
    required = [
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_master.csv",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_master.dta",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_restricted.csv",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_restricted.dta",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_expanded.csv",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_expanded.dta",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_excluded_or_non_us.csv",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_excluded_or_non_us.dta",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_sources.csv",
        project_root / "data" / "processed_datasets" / "country_artists" / "country_artists_sources.dta",
        project_root / "data" / "processed_datasets" / "country_artists" / "manual_review_queue.csv",
        project_root / "data" / "processed_datasets" / "country_artists" / "manual_review_queue.dta",
    ]
    return all(path.exists() for path in required)


def restore_bundled_country_snapshot(project_root: Path, package_root: Path) -> bool:
    bundled_final = package_root / "datasets" / "country_artists" / "final"
    bundled_intermediate = package_root / "datasets" / "country_artists" / "intermediate"
    if not bundled_final.exists():
        return False

    target_final_dir = project_root / "data" / "processed_datasets" / "country_artists"
    target_intermediate_dir = target_final_dir / "intermediate"
    target_final_dir.mkdir(parents=True, exist_ok=True)
    target_intermediate_dir.mkdir(parents=True, exist_ok=True)

    for path in bundled_final.iterdir():
        if path.is_file():
            copy_file(path, target_final_dir / path.name)
    if bundled_intermediate.exists():
        for path in bundled_intermediate.iterdir():
            if path.is_file():
                copy_file(path, target_intermediate_dir / path.name)
    print("Restored country-artist outputs from bundled replication snapshot.", flush=True)
    return True


def ensure_structure(package_root: Path) -> None:
    dirs = [
        package_root,
        package_root / "code" / "python" / "phase_01_dataset_construction",
        package_root / "code" / "stata",
        package_root / "docs" / "directives_snapshot",
        package_root / "datasets" / "country_artists" / "final",
        package_root / "datasets" / "country_artists" / "intermediate",
    ]
    for path in dirs:
        path.mkdir(parents=True, exist_ok=True)


def run_country_pipeline(project_root: Path, package_root: Path, *, full_rebuild: bool) -> None:
    if country_outputs_exist(project_root) and not full_rebuild:
        print("Country-artist final outputs already present; skipping rebuild and snapshotting existing deliverables.", flush=True)
        return
    if not full_rebuild and restore_bundled_country_snapshot(project_root, package_root):
        return
    packaged_builder = package_root / "code" / "python" / "phase_01_dataset_construction" / "build_country_artists_dataset.py"
    builder = project_root / "execution" / "phase_01_dataset_construction" / "build_country_artists_dataset.py"
    if not builder.exists() and packaged_builder.exists():
        builder = packaged_builder
    env = os.environ.copy()
    env["SOC_PROJECT_ROOT"] = str(project_root)
    env["SOC_COUNTRY_SCHEMA_PATH"] = str(package_root / "docs" / "directives_snapshot" / "country_artists_schema.csv")
    print("\n>>> python3", str(builder.relative_to(project_root) if builder.is_relative_to(project_root) else builder), flush=True)
    subprocess.run(["python3", str(builder)], cwd=str(project_root), check=True, env=env)
    fallback_marker = project_root / "data" / "processed_datasets" / "country_artists" / "intermediate" / "_bundled_fallback_used.txt"
    if full_rebuild and fallback_marker.exists():
        print("Bundled intermediate fallback was used during full rebuild; restoring canonical bundled final outputs.", flush=True)
        restore_bundled_country_snapshot(project_root, package_root)


def main() -> None:
    script_path = current_script_path()
    project_root, package_dirname, full_rebuild = parse_runtime_args(sys.argv)
    package_root = package_root_from_script(script_path, project_root, package_dirname)

    print(f"Project root: {project_root}", flush=True)
    print(f"Replication package target: {package_root}", flush=True)
    print(f"Full rebuild mode: {full_rebuild}", flush=True)

    ensure_structure(package_root)
    run_country_pipeline(project_root, package_root, full_rebuild=full_rebuild)
    copy_country_outputs(project_root, package_root)
    copy_python_tree(project_root, package_root)
    copy_directives(project_root, package_root)
    copy_replication_assets(project_root, package_root)

    print("Replication pipeline completed successfully.", flush=True)


if __name__ == "__main__":
    main()
