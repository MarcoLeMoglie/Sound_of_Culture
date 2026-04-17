def run_country_pipeline(project_root: Path, package_root: Path, *, full_rebuild: bool) -> None:
    if country_outputs_exist(project_root) and not full_rebuild:
        print("Country-artist final outputs already present; skipping rebuild and snapshotting existing deliverables.", flush=True)
        return
    if not full_rebuild and restore_bundled_country_snapshot(project_root, package_root):
        return
    packaged_builder = package_root / "code" / "python" / "step4_country_artists" / "build_country_artists_dataset.py"
    builder = project_root / "execution" / "step4_country_artists" / "build_country_artists_dataset.py"
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
