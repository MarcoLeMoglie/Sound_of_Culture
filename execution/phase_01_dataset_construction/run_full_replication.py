#!/usr/bin/env python3
"""Phase-based wrapper for the full replication runner."""

from _legacy_runner import run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step5_replication/run_full_replication.py")
