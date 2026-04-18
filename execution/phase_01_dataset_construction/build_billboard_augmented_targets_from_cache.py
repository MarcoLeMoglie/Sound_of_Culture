#!/usr/bin/env python3
"""Phase-based bridge for cached Billboard augmented target building."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import (
        export_legacy_module,
        run_legacy_script,
    )
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step4_country_artists/build_billboard_augmented_targets_from_cache.py")
else:
    export_legacy_module(
        "execution/step4_country_artists/build_billboard_augmented_targets_from_cache.py",
        globals(),
        module_name="soc_phase01_build_billboard_augmented_targets_from_cache",
    )
