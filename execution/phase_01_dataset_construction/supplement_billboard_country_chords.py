#!/usr/bin/env python3
"""Phase-based wrapper for the Billboard supplement workflow."""

from _legacy_runner import run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step1_download/supplement_billboard_country_chords.py")
