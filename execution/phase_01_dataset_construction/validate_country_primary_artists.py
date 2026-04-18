#!/usr/bin/env python3
"""Phase-based entrypoint for country-primary validation."""

from execution.phase_01_dataset_construction._legacy_runner import run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step4_country_artists/validate_country_primary_artists.py")
