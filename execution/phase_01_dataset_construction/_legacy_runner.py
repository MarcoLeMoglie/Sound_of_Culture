#!/usr/bin/env python3
"""Run a legacy script from the canonical phase-based entrypoint.

This wrapper keeps the repository's public surface aligned with the new
phase-based architecture while the actual implementation still lives in legacy
`execution/step*` directories.
"""

from __future__ import annotations

import os
import runpy
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def run_legacy_script(relative_legacy_path: str) -> None:
    legacy_script = REPO_ROOT / relative_legacy_path
    if not legacy_script.exists():
        raise FileNotFoundError(
            f"Legacy script not found for phase wrapper: {legacy_script}"
        )

    # Most project scripts expect to run from the repository root.
    os.chdir(REPO_ROOT)
    runpy.run_path(str(legacy_script), run_name="__main__")
