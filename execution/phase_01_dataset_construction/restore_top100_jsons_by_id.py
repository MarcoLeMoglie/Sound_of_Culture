#!/usr/bin/env python3
"""Phase-based bridge for Top 100 JSON restoration by Ultimate Guitar tab id."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import (
        export_legacy_module,
        run_legacy_script,
    )
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module, run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step5_replication/restore_top100_jsons_by_id.py")
else:
    export_legacy_module(
        "execution/step5_replication/restore_top100_jsons_by_id.py",
        globals(),
        module_name="soc_phase01_restore_top100_jsons_by_id",
    )
