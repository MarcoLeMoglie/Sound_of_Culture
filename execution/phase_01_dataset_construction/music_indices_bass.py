#!/usr/bin/env python3
"""Phase-based bridge for bass-specific music index helpers."""

try:
    from execution.phase_01_dataset_construction._legacy_runner import export_legacy_module
except ModuleNotFoundError:
    from _legacy_runner import export_legacy_module


export_legacy_module(
    "execution/step2_digitalize/music_indices_bass.py",
    globals(),
    module_name="soc_phase01_music_indices_bass",
)
