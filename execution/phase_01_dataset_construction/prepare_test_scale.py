#!/usr/bin/env python3
"""Phase-based bridge for test-scale input preparation."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import export_legacy_module, run_legacy_script
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step1_download/prepare_test_scale.py")
else:
    export_legacy_module(
        "execution/step1_download/prepare_test_scale.py",
        globals(),
        module_name="soc_phase01_prepare_test_scale",
    )
