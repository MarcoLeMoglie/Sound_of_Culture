#!/usr/bin/env python3
"""Phase-based bridge for 1985 tracklist repair."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import export_legacy_module, run_legacy_script
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step1_download/repair_1985.py")
else:
    export_legacy_module(
        "execution/step1_download/repair_1985.py",
        globals(),
        module_name="soc_phase01_repair_1985",
    )
