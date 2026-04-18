#!/usr/bin/env python3
"""Phase-based bridge for fuzzy release-year enrichment."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import export_legacy_module, run_legacy_script
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step2_digitalize/enrich_release_years_fuzzy.py")
else:
    export_legacy_module(
        "execution/step2_digitalize/enrich_release_years_fuzzy.py",
        globals(),
        module_name="soc_phase01_enrich_release_years_fuzzy",
    )
