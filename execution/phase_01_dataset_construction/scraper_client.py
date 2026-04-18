#!/usr/bin/env python3
"""Phase-based bridge for the Ultimate Guitar API client."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import export_legacy_module
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module


export_legacy_module(
    "execution/step1_download/scraper_client.py",
    globals(),
    module_name="soc_phase01_scraper_client",
)
