#!/usr/bin/env python3
"""Phase-based entrypoint for the final merge to restricted v6."""

from execution.phase_01_dataset_construction._legacy_runner import run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step2_digitalize/final_merge_restricted_v2.py")
