#!/usr/bin/env python3
"""Run a legacy script from the canonical phase-based entrypoint.

This wrapper keeps the repository's public surface aligned with the new
phase-based architecture while the actual implementation still lives in legacy
`execution/step*` directories.
"""

from __future__ import annotations

import importlib.util
import os
import runpy
import sys
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


def load_legacy_module(relative_legacy_path: str, *, module_name: str) -> object:
    legacy_script = REPO_ROOT / relative_legacy_path
    if not legacy_script.exists():
        raise FileNotFoundError(
            f"Legacy module not found for phase bridge: {legacy_script}"
        )

    spec = importlib.util.spec_from_file_location(module_name, legacy_script)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not build import spec for {legacy_script}")

    module = importlib.util.module_from_spec(spec)
    added_paths: list[str] = []
    for path in [str(REPO_ROOT), str(legacy_script.parent)]:
        if path not in sys.path:
            sys.path.insert(0, path)
            added_paths.append(path)

    try:
        spec.loader.exec_module(module)
    finally:
        for path in reversed(added_paths):
            if path in sys.path:
                sys.path.remove(path)

    return module


def export_legacy_module(
    relative_legacy_path: str,
    target_globals: dict[str, object],
    *,
    module_name: str,
) -> object:
    module = load_legacy_module(relative_legacy_path, module_name=module_name)
    public_names = getattr(module, "__all__", None)
    if public_names is None:
        public_names = [name for name in module.__dict__ if not name.startswith("_")]

    for name in public_names:
        if name in module.__dict__:
            target_globals[name] = module.__dict__[name]

    target_globals["LEGACY_MODULE"] = module
    target_globals["__all__"] = [name for name in public_names if name in module.__dict__]
    if getattr(module, "__doc__", None):
        target_globals["__doc__"] = module.__doc__
    return module
