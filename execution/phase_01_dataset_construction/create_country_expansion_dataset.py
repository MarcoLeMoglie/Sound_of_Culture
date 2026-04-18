#!/usr/bin/env python3
"""Phase-based bridge for the country expansion dataset builder."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import (
        export_legacy_module,
        run_legacy_script,
    )
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step2_digitalize/create_country_expansion_dataset.py")
else:
    export_legacy_module(
        "execution/step2_digitalize/create_country_expansion_dataset.py",
        globals(),
        module_name="soc_phase01_create_country_expansion_dataset",
    )
