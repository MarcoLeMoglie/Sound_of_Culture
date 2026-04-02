import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from execution.step1_download.scraper_client import UltimateGuitarClient


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9 _-]+", "", str(value)).strip().replace(" ", "_")
    return cleaned or "unknown"


def save_tab(output_dir: Path, artist: str, song: str, tab_id: int, payload: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{safe_name(artist)}_{safe_name(song)}_{tab_id}.json"
    with open(output_dir / filename, "w") as handle:
        json.dump(payload, handle, indent=4)


def restore(dataset_csv: Path, output_dir: Path, pause_seconds: float = 0.2) -> None:
    df = pd.read_csv(dataset_csv)
    if "id" not in df.columns:
        raise ValueError(f"Column 'id' not found in {dataset_csv}")

    client = UltimateGuitarClient()
    restored = 0
    skipped = 0

    for _, row in df.iterrows():
        tab_id = int(row["id"])
        artist = row.get("artist_name", "unknown")
        song = row.get("song_name", "unknown")
        filename = output_dir / f"{safe_name(artist)}_{safe_name(song)}_{tab_id}.json"
        if filename.exists():
            skipped += 1
            continue

        attempts = 0
        while attempts < 3:
            attempts += 1
            try:
                payload = client.get_tab_info(tab_id)
                save_tab(output_dir, artist, song, tab_id, payload)
                restored += 1
                break
            except Exception as exc:
                if attempts >= 3:
                    print(f"Failed {tab_id} ({artist} - {song}): {exc}", flush=True)
                else:
                    time.sleep(1.5 * attempts)
        time.sleep(pause_seconds)

    print(
        f"Restoration complete for {dataset_csv.name}: restored={restored}, skipped_existing={skipped}, total={len(df)}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_csv")
    parser.add_argument("output_dir")
    parser.add_argument("--pause-seconds", type=float, default=0.2)
    args = parser.parse_args()

    restore(Path(args.dataset_csv), Path(args.output_dir), pause_seconds=args.pause_seconds)


if __name__ == "__main__":
    main()
