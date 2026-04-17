import argparse
import csv
import json
import os
import random
import re
import sys
import threading
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(Path(__file__).resolve().parent))

from scraper_client import UltimateGuitarClient


ARTIST_CSV = PROJECT_ROOT / "data" / "processed_datasets" / "country_artists" / "artist_universe_country_only.csv"
INTERMEDIATE_DIR = PROJECT_ROOT / "data" / "intermediate" / "json"
CHORDS_DISCOVERY_PATH = INTERMEDIATE_DIR / "ug_country_only_chords_discovery.json"
TABS_DISCOVERY_PATH = INTERMEDIATE_DIR / "ug_country_only_tabs_discovery.json"
PROGRESS_PATH = INTERMEDIATE_DIR / "ug_country_only_discovery_progress.json"
PLAN_PATH = INTERMEDIATE_DIR / "ug_country_only_download_plan.json"
RAW_CHORDS_DIR = PROJECT_ROOT / "data" / "raw_tabs_country"
RAW_TABS_DIR = PROJECT_ROOT / "data" / "raw_country_tabs"

TYPE_TO_DIR = {
    "Chords": RAW_CHORDS_DIR,
    "Tabs": RAW_TABS_DIR,
}

TYPE_TO_DISCOVERY = {
    "Chords": CHORDS_DISCOVERY_PATH,
    "Tabs": TABS_DISCOVERY_PATH,
}

STOPWORDS = {
    "the",
    "and",
    "feat",
    "featuring",
    "with",
    "band",
}

print_lock = threading.Lock()
counter_lock = threading.Lock()


def log(message):
    with print_lock:
        print(message, flush=True)


def normalize_text(value):
    if value is None:
        return ""
    value = unicodedata.normalize("NFKD", str(value))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower().replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def safe_filename_component(value):
    value = unicodedata.normalize("NFKD", str(value))
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = "".join(ch for ch in value if ch.isalnum() or ch in (" ", "_", "-")).strip()
    value = re.sub(r"\s+", "_", value)
    return value or "unknown"


def load_artists(csv_path):
    artists = []
    with open(csv_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            artist_id = (row.get("artist_id") or "").strip()
            name_primary = (row.get("name_primary") or "").strip()
            stage_name = (row.get("stage_name") or "").strip()
            if not artist_id or not name_primary:
                continue
            search_names = []
            for candidate in [name_primary, stage_name]:
                candidate = candidate.strip()
                if candidate and candidate not in search_names:
                    search_names.append(candidate)
            artists.append(
                {
                    "artist_id": artist_id,
                    "name_primary": name_primary,
                    "stage_name": stage_name,
                    "search_names": search_names,
                }
            )
    return artists


def artist_matches(target_names, result_artist_name):
    result_norm = normalize_text(result_artist_name)
    if not result_norm:
        return False

    result_tokens = {token for token in result_norm.split() if token not in STOPWORDS}
    for candidate in target_names:
        candidate_norm = normalize_text(candidate)
        if not candidate_norm:
            continue
        if candidate_norm == result_norm:
            return True
        if len(candidate_norm) >= 8 and candidate_norm in result_norm:
            return True

        candidate_tokens = {token for token in candidate_norm.split() if token not in STOPWORDS}
        if candidate_tokens and candidate_tokens.issubset(result_tokens):
            return True
    return False


def tab_sort_key(tab):
    rating = tab.get("rating")
    votes = tab.get("votes")
    try:
        rating = float(rating or 0.0)
    except (TypeError, ValueError):
        rating = 0.0
    try:
        votes = int(votes or 0)
    except (TypeError, ValueError):
        votes = 0
    return (rating, votes, int(tab.get("id") or 0))


def flatten_top_versions(by_song):
    flat = []
    for versions in by_song.values():
        ordered = sorted(versions, key=tab_sort_key, reverse=True)[:3]
        flat.extend(ordered)
    flat.sort(
        key=lambda row: (
            row.get("artist_id", ""),
            normalize_text(row.get("song_name", "")),
            row.get("type", ""),
            -tab_sort_key(row)[0],
            -tab_sort_key(row)[1],
            -tab_sort_key(row)[2],
        )
    )
    return flat


def load_progress():
    if not PROGRESS_PATH.exists():
        return {"processed_artist_ids": [], "chords": [], "tabs": []}
    with open(PROGRESS_PATH, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return {
        "processed_artist_ids": data.get("processed_artist_ids", []),
        "chords": data.get("chords", []),
        "tabs": data.get("tabs", []),
    }


def rebuild_maps(progress):
    chords_map = defaultdict(list)
    tabs_map = defaultdict(list)

    for row in progress.get("chords", []):
        key = (row.get("artist_id", ""), normalize_text(row.get("song_name", "")))
        chords_map[key].append(row)

    for row in progress.get("tabs", []):
        key = (row.get("artist_id", ""), normalize_text(row.get("song_name", "")))
        tabs_map[key].append(row)

    for store in (chords_map, tabs_map):
        for key, versions in list(store.items()):
            deduped = {}
            for version in versions:
                deduped[version["id"]] = version
            store[key] = sorted(deduped.values(), key=tab_sort_key, reverse=True)[:3]

    return chords_map, tabs_map


def save_progress(processed_artist_ids, chords_map, tabs_map, artists_total):
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)
    chords_flat = flatten_top_versions(chords_map)
    tabs_flat = flatten_top_versions(tabs_map)

    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "artists_total": artists_total,
        "processed_artist_ids": sorted(processed_artist_ids),
        "chords": chords_flat,
        "tabs": tabs_flat,
    }
    with open(PROGRESS_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)

    with open(CHORDS_DISCOVERY_PATH, "w", encoding="utf-8") as handle:
        json.dump(chords_flat, handle, indent=2)

    with open(TABS_DISCOVERY_PATH, "w", encoding="utf-8") as handle:
        json.dump(tabs_flat, handle, indent=2)


def discover_kind_for_artist(client, artist_row, tab_type, max_pages):
    discovered_by_song = defaultdict(dict)

    for query in artist_row["search_names"]:
        page = 1
        empty_pages = 0
        seen_page_ids = set()

        while page <= max_pages:
            try:
                response = client.search(query, page=page, tab_type=tab_type)
            except Exception as exc:
                if "404" in str(exc):
                    break
                log(f"[discover:{tab_type}] {artist_row['name_primary']} page {page} failed: {exc}")
                break

            if not isinstance(response, dict):
                log(f"[discover:{tab_type}] {artist_row['name_primary']} page {page} returned an empty payload")
                break

            tabs = response.get("tabs", []) or []
            if not tabs:
                empty_pages += 1
                if empty_pages >= 1:
                    break

            new_matches_this_page = 0
            duplicate_page = True
            for tab in tabs:
                tab_id = tab.get("id")
                if tab_id is None:
                    continue
                if tab_id not in seen_page_ids:
                    duplicate_page = False
                    seen_page_ids.add(tab_id)

                tab_type_value = str(tab.get("type", "")).strip().lower()
                if tab_type_value != tab_type.lower():
                    continue
                result_artist_name = tab.get("artist_name", "")
                if not artist_matches(artist_row["search_names"], result_artist_name):
                    continue

                normalized_song = normalize_text(tab.get("song_name", ""))
                if not normalized_song:
                    continue

                record = {
                    "id": int(tab_id),
                    "artist_id": artist_row["artist_id"],
                    "artist_name": result_artist_name,
                    "target_artist_name": artist_row["name_primary"],
                    "song_name": tab.get("song_name", ""),
                    "rating": tab.get("rating", 0.0),
                    "votes": tab.get("votes", 0),
                    "type": tab.get("type", tab_type),
                    "query": query,
                }
                song_key = (artist_row["artist_id"], normalized_song)
                discovered_by_song[song_key][record["id"]] = record
                new_matches_this_page += 1

            if duplicate_page or len(tabs) < 50:
                break

            page += 1
            time.sleep(random.uniform(0.35, 0.9))

        time.sleep(random.uniform(0.25, 0.6))

    reduced = {}
    for song_key, versions in discovered_by_song.items():
        reduced[song_key] = sorted(versions.values(), key=tab_sort_key, reverse=True)[:3]
    return reduced


def discover_artist_payload(artist_row, max_pages):
    client = UltimateGuitarClient()
    chords_result = discover_kind_for_artist(client, artist_row, "Chords", max_pages=max_pages)
    tabs_result = discover_kind_for_artist(client, artist_row, "Tabs", max_pages=max_pages)
    return artist_row, chords_result, tabs_result


def run_discovery(max_pages, checkpoint_every, workers):
    artists = load_artists(ARTIST_CSV)
    progress = load_progress()
    processed_artist_ids = set(progress["processed_artist_ids"])
    chords_map, tabs_map = rebuild_maps(progress)

    total = len(artists)
    log(f"[discover] Loaded {total} artists from {ARTIST_CSV}")
    log(f"[discover] Resume state: {len(processed_artist_ids)} artists already processed")

    pending_artists = [artist for artist in artists if artist["artist_id"] not in processed_artist_ids]
    artist_positions = {artist["artist_id"]: idx for idx, artist in enumerate(artists, start=1)}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_artist = {
            executor.submit(discover_artist_payload, artist_row, max_pages): artist_row
            for artist_row in pending_artists
        }
        for future in as_completed(future_to_artist):
            artist_row = future_to_artist[future]
            artist_id = artist_row["artist_id"]
            position = artist_positions[artist_id]
            log(f"[discover] [{position}/{total}] completed {artist_row['name_primary']}")

            try:
                _, chords_result, tabs_result = future.result()
            except Exception as exc:
                log(f"[discover] artist failed and will be retried on the next run: {artist_row['name_primary']} ({exc})")
                continue

            for key, versions in chords_result.items():
                chords_map[key] = versions
            for key, versions in tabs_result.items():
                tabs_map[key] = versions

            processed_artist_ids.add(artist_id)

            processed_count = len(processed_artist_ids)
            if processed_count % checkpoint_every == 0 or processed_count == total:
                save_progress(processed_artist_ids, chords_map, tabs_map, total)
                log(
                    f"[discover] checkpoint: artists={processed_count}/{total}, "
                    f"chords_json={len(flatten_top_versions(chords_map))}, "
                    f"tabs_json={len(flatten_top_versions(tabs_map))}"
                )

    save_progress(processed_artist_ids, chords_map, tabs_map, total)
    log(
        f"[discover] complete: artists={len(processed_artist_ids)}/{total}, "
        f"chords_json={len(flatten_top_versions(chords_map))}, "
        f"tabs_json={len(flatten_top_versions(tabs_map))}"
    )


def extract_existing_ids(output_dir):
    ids = set()
    if not output_dir.exists():
        return ids
    for filename in output_dir.iterdir():
        if filename.suffix.lower() != ".json":
            continue
        match = re.search(r"_([0-9]+)\.json$", filename.name)
        if match:
            ids.add(int(match.group(1)))
    return ids


def load_discovery(discovery_path):
    if not discovery_path.exists():
        raise FileNotFoundError(f"Discovery file not found: {discovery_path}")
    with open(discovery_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def count_unique_songs(rows):
    return len({(row.get("artist_id", ""), normalize_text(row.get("song_name", ""))) for row in rows})


def build_plan():
    chords = load_discovery(CHORDS_DISCOVERY_PATH)
    tabs = load_discovery(TABS_DISCOVERY_PATH)

    RAW_CHORDS_DIR.mkdir(parents=True, exist_ok=True)
    RAW_TABS_DIR.mkdir(parents=True, exist_ok=True)

    existing_chords_ids = extract_existing_ids(RAW_CHORDS_DIR)
    existing_tabs_ids = extract_existing_ids(RAW_TABS_DIR)

    chords_ids = {int(row["id"]) for row in chords}
    tabs_ids = {int(row["id"]) for row in tabs}

    plan = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "artists_targeted": len(load_artists(ARTIST_CSV)),
        "chords": {
            "discovery_file": str(CHORDS_DISCOVERY_PATH),
            "output_dir": str(RAW_CHORDS_DIR),
            "unique_songs": count_unique_songs(chords),
            "json_total": len(chords_ids),
            "json_existing": len(chords_ids & existing_chords_ids),
            "json_missing": len(chords_ids - existing_chords_ids),
        },
        "tabs": {
            "discovery_file": str(TABS_DISCOVERY_PATH),
            "output_dir": str(RAW_TABS_DIR),
            "unique_songs": count_unique_songs(tabs),
            "json_total": len(tabs_ids),
            "json_existing": len(tabs_ids & existing_tabs_ids),
            "json_missing": len(tabs_ids - existing_tabs_ids),
        },
    }
    plan["combined"] = {
        "json_total": plan["chords"]["json_total"] + plan["tabs"]["json_total"],
        "json_existing": plan["chords"]["json_existing"] + plan["tabs"]["json_existing"],
        "json_missing": plan["chords"]["json_missing"] + plan["tabs"]["json_missing"],
    }

    with open(PLAN_PATH, "w", encoding="utf-8") as handle:
        json.dump(plan, handle, indent=2)

    print(json.dumps(plan, indent=2), flush=True)


def download_rows(rows, output_dir, workers, min_delay, max_delay, shard_mod, shard_rem):
    output_dir.mkdir(parents=True, exist_ok=True)
    existing_ids = extract_existing_ids(output_dir)
    sharded_rows = rows
    if shard_mod is not None and shard_rem is not None:
        sharded_rows = [row for row in rows if int(row["id"]) % shard_mod == shard_rem]
    pending = [row for row in sharded_rows if int(row["id"]) not in existing_ids]
    total = len(rows)
    shard_total = len(sharded_rows)
    pending_total = len(pending)

    log(f"[download] output_dir={output_dir}")
    log(
        f"[download] discovered={total}, shard_scope={shard_total}, "
        f"existing_in_scope={shard_total - pending_total}, pending={pending_total}"
    )

    if not pending:
        return

    client = UltimateGuitarClient()
    shared_existing_ids = set(existing_ids)
    completed = 0

    def worker(row):
        nonlocal completed
        tab_id = int(row["id"])
        with counter_lock:
            if tab_id in shared_existing_ids:
                completed += 1
                return "skip"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                payload = client.get_tab_info(tab_id)
                filename = (
                    f"{safe_filename_component(row.get('artist_name') or row.get('target_artist_name'))}_"
                    f"{safe_filename_component(row.get('song_name'))}_{tab_id}.json"
                )
                filepath = output_dir / filename
                with open(filepath, "w", encoding="utf-8") as handle:
                    json.dump(payload, handle, indent=2)

                with counter_lock:
                    shared_existing_ids.add(tab_id)
                    completed += 1
                    done = completed
                if done % 25 == 0 or done == pending_total:
                    log(f"[download] progress={done}/{pending_total}")
                time.sleep(random.uniform(min_delay, max_delay))
                return "success"
            except Exception as exc:
                message = str(exc)
                if "429" in message:
                    backoff = min((attempt + 1) * 30, 90)
                    log(f"[download] rate limit on {tab_id}, waiting {backoff}s")
                    time.sleep(backoff)
                    continue
                if "451" in message:
                    with counter_lock:
                        completed += 1
                    log(f"[download] legal skip 451 for {tab_id}")
                    return "legal_unavailable"
                if attempt == max_retries - 1:
                    with counter_lock:
                        completed += 1
                    log(f"[download] failed {tab_id}: {exc}")
                    return "failed"
                time.sleep(5)
        return "failed"

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker, row) for row in pending]
        for future in as_completed(futures):
            future.result()


def run_download(kind, workers, min_delay, max_delay, shard_mod, shard_rem):
    targets = []
    if kind in ("chords", "both"):
        targets.append(("Chords", CHORDS_DISCOVERY_PATH, RAW_CHORDS_DIR))
    if kind in ("tabs", "both"):
        targets.append(("Tabs", TABS_DISCOVERY_PATH, RAW_TABS_DIR))

    for label, discovery_path, output_dir in targets:
        rows = load_discovery(discovery_path)
        log(f"[download:{label}] starting")
        download_rows(
            rows,
            output_dir,
            workers,
            min_delay=min_delay,
            max_delay=max_delay,
            shard_mod=shard_mod,
            shard_rem=shard_rem,
        )
        log(f"[download:{label}] complete")


def parse_args():
    parser = argparse.ArgumentParser(description="Ultimate Guitar pipeline for 2333 country-only artists.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    discover = subparsers.add_parser("discover", help="Discover top-3 chords and tabs per song for all country-only artists.")
    discover.add_argument("--max-pages", type=int, default=20)
    discover.add_argument("--checkpoint-every", type=int, default=10)
    discover.add_argument("--workers", type=int, default=3)

    subparsers.add_parser("plan", help="Summarize how many JSON files exist and are still missing.")

    download = subparsers.add_parser("download", help="Download discovered JSON payloads.")
    download.add_argument("--kind", choices=["chords", "tabs", "both"], default="both")
    download.add_argument("--workers", type=int, default=3)
    download.add_argument("--min-delay", type=float, default=0.6)
    download.add_argument("--max-delay", type=float, default=1.6)
    download.add_argument("--shard-mod", type=int, default=None)
    download.add_argument("--shard-rem", type=int, default=None)

    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == "discover":
        run_discovery(max_pages=args.max_pages, checkpoint_every=args.checkpoint_every, workers=args.workers)
    elif args.command == "plan":
        build_plan()
    elif args.command == "download":
        run_download(
            kind=args.kind,
            workers=args.workers,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
            shard_mod=args.shard_mod,
            shard_rem=args.shard_rem,
        )


if __name__ == "__main__":
    main()
