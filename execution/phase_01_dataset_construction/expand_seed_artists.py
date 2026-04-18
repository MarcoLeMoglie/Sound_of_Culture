#!/usr/bin/env python3
"""Phase-based bridge for seed-artist expansion."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import export_legacy_module, run_legacy_script
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step1_download/expand_seed_artists.py")
else:
    export_legacy_module(
        "execution/step1_download/expand_seed_artists.py",
        globals(),
        module_name="soc_phase01_expand_seed_artists",
    )
