#!/usr/bin/env python3
"""Phase-based wrapper for artist-universe metadata enrichment."""

from _legacy_runner import run_legacy_script


if __name__ == "__main__":
    run_legacy_script("execution/step4_country_artists/enrich_artist_universe_missing_metadata.py")
