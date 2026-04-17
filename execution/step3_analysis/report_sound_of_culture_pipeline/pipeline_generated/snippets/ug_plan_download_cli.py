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
