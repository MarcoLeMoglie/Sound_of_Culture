#!/usr/bin/env python3
"""Phase-based bridge for best-chords main scrape."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import export_legacy_module, run_legacy_script
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step1_download/main_scrape_best_chords.py")
else:
    export_legacy_module(
        "execution/step1_download/main_scrape_best_chords.py",
        globals(),
        module_name="soc_phase01_main_scrape_best_chords",
    )
