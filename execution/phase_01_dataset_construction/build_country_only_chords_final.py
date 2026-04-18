#!/usr/bin/env python3
"""Phase-based bridge for final country-only chord dataset assembly."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import (
        export_legacy_module,
        run_legacy_script,
    )
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step2_digitalize/build_country_only_chords_final.py")
else:
    export_legacy_module(
        "execution/step2_digitalize/build_country_only_chords_final.py",
        globals(),
        module_name="soc_phase01_build_country_only_chords_final",
    )
