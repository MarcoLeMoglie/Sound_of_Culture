import csv
import importlib.util
import json
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = ROOT / "data" / "processed_datasets" / "country_artists"
INTERMEDIATE_DIR = OUTPUT_DIR / "intermediate"
SONGS_CSV = OUTPUT_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
SEED_JSON = ROOT / "data" / "intermediate" / "json" / "seed_country_artists.json"
BUILDER_PATH = ROOT / "execution" / "phase_01_dataset_construction" / "build_country_artists_dataset.py"

ARTIST_SNAPSHOT_ROOT = OUTPUT_DIR / "replication_package_country_artists_2026_04_02" / "datasets" / "country_artists" / "final"
RESTRICTED_CSV_CANDIDATES = [
    OUTPUT_DIR / "country_artists_restricted.csv",
    ARTIST_SNAPSHOT_ROOT / "country_artists_restricted.csv",
]
MASTER_CSV_CANDIDATES = [
    OUTPUT_DIR / "country_artists_master.csv",
    ARTIST_SNAPSHOT_ROOT / "country_artists_master.csv",
]

CORE_OUTPUT_BASENAME = "artist_universe_country_core"
ADJACENT_OUTPUT_BASENAME = "artist_universe_country_plus_adjacent"
ADJACENT_ONLY_OUTPUT_BASENAME = "artist_universe_adjacent_only"
REVIEW_OUTPUT_BASENAME = "artist_universe_manual_review"

TAG_SEED_CACHE = INTERMEDIATE_DIR / "artist_universe_adjacent_tag_seeds.csv"
ADJACENT_SEED_CACHE = INTERMEDIATE_DIR / "artist_universe_adjacent_seed_candidates.csv"
ADJACENT_DETAIL_CACHE = INTERMEDIATE_DIR / "artist_universe_adjacent_wikidata_details.csv"
ADJACENT_UNRESOLVED_CACHE = INTERMEDIATE_DIR / "artist_universe_adjacent_unresolved_titles.csv"

ADJACENT_TAGS = [
    "folk",
    "americana",
    "bluegrass",
    "rockabilly",
    "western swing",
    "outlaw country",
    "roots rock",
    "country gospel",
]

KEY_COMPLETENESS_COLUMNS = [
    "name_primary",
    "birth_year",
    "birth_city",
    "birth_state",
    "birth_country",
]

EXTRA_COLUMNS = [
    "name_key",
    "in_songfile",
    "in_country_restricted",
    "in_country_master",
    "in_adjacent_seed_pool",
    "source_membership_count",
    "artist_name_songfile_example",
    "adjacent_genre_bucket",
    "adjacent_source_group",
    "core_union_flag",
]


def log(message: str) -> None:
    print(f"[artist-universe] {message}", flush=True)


def is_network_resolution_failure(exc: Exception) -> bool:
    text = str(exc).lower()
    markers = [
        "failed to resolve",
        "name resolution",
        "nodename nor servname provided",
        "temporary failure in name resolution",
    ]
    return any(marker in text for marker in markers)


def load_builder():
    spec = importlib.util.spec_from_file_location("country_builder_module", BUILDER_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load country builder from {BUILDER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_builder_rate_limit_tolerant(builder) -> None:
    original_enrich_from_wikipedia = builder.enrich_from_wikipedia
    original_enrich_from_wikipedia_lead = builder.enrich_from_wikipedia_lead
    original_mb_bio = builder.targeted_musicbrainz_bio_enrichment

    def safe_enrich_from_wikipedia(s, df, checkpoint_path):
        try:
            return original_enrich_from_wikipedia(s, df, checkpoint_path)
        except Exception as exc:
            log(f"Wikipedia enrichment skipped after failure: {exc}")
            return df

    def safe_enrich_from_wikipedia_lead(s, df, checkpoint_path):
        try:
            return original_enrich_from_wikipedia_lead(s, df, checkpoint_path)
        except Exception as exc:
            log(f"Wikipedia lead enrichment skipped after failure: {exc}")
            return df

    def safe_targeted_musicbrainz_bio_enrichment(s, df, checkpoint_path):
        try:
            return original_mb_bio(s, df, checkpoint_path)
        except Exception as exc:
            log(f"Targeted MusicBrainz bio enrichment skipped after failure: {exc}")
            return df

    builder.enrich_from_wikipedia = safe_enrich_from_wikipedia
    builder.enrich_from_wikipedia_lead = safe_enrich_from_wikipedia_lead
    builder.targeted_musicbrainz_bio_enrichment = safe_targeted_musicbrainz_bio_enrichment


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)


def find_existing(paths: Sequence[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError(f"Could not find any of the candidate files: {paths}")


def normalize_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = text.strip().lower()
    text = re.sub(r"\([^)]*\)", "", text)
    text = text.replace("&", " and ")
    text = text.replace("+", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def nonempty_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def count_nonmissing(row: pd.Series, columns: Sequence[str]) -> int:
    total = 0
    for col in columns:
        value = row.get(col)
        if pd.isna(value):
            continue
        if isinstance(value, str):
            if value.strip() and value.strip().lower() != "nan":
                total += 1
        else:
            total += 1
    return total


def first_nonempty(series: pd.Series) -> str:
    for value in series:
        text = nonempty_text(value)
        if text:
            return text
    return ""


def coerce_numeric_if_needed(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def safe_int(value: object) -> int:
    try:
        if pd.isna(value):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def artist_schema_columns(builder) -> List[str]:
    schema_df = pd.read_csv(builder.SCHEMA_PATH)
    return schema_df["column_name"].tolist()


def load_current_country_frames(builder) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str]]:
    schema_cols = artist_schema_columns(builder)
    songs_df = pd.read_csv(SONGS_CSV, low_memory=False)
    restricted_df = pd.read_csv(find_existing(RESTRICTED_CSV_CANDIDATES), low_memory=False)
    master_df = pd.read_csv(find_existing(MASTER_CSV_CANDIDATES), low_memory=False)

    songs_artist_df = songs_df.reindex(columns=schema_cols).copy()
    if "name_primary" in songs_artist_df.columns and "artist_name" in songs_df.columns:
        missing_mask = songs_artist_df["name_primary"].fillna("").astype(str).str.strip().eq("")
        songs_artist_df.loc[missing_mask, "name_primary"] = songs_df.loc[missing_mask, "artist_name"]
        songs_artist_df["artist_name_songfile_example"] = songs_df["artist_name"].fillna("").astype(str)
    else:
        songs_artist_df["artist_name_songfile_example"] = ""

    songs_artist_df["in_songfile"] = 1
    songs_artist_df["in_country_restricted"] = 0
    songs_artist_df["in_country_master"] = 0
    songs_artist_df["in_adjacent_seed_pool"] = 0
    songs_artist_df["adjacent_source_group"] = ""
    songs_artist_df["adjacent_genre_bucket"] = ""
    songs_artist_df["core_union_flag"] = 1

    restricted_df = restricted_df.reindex(columns=schema_cols).copy()
    restricted_df["artist_name_songfile_example"] = ""
    restricted_df["in_songfile"] = 0
    restricted_df["in_country_restricted"] = 1
    restricted_df["in_country_master"] = 0
    restricted_df["in_adjacent_seed_pool"] = 0
    restricted_df["adjacent_source_group"] = ""
    restricted_df["adjacent_genre_bucket"] = ""
    restricted_df["core_union_flag"] = 1

    master_df = master_df.reindex(columns=schema_cols).copy()
    master_df["artist_name_songfile_example"] = ""
    master_df["in_songfile"] = 0
    master_df["in_country_restricted"] = 0
    master_df["in_country_master"] = 1
    master_df["in_adjacent_seed_pool"] = 0
    master_df["adjacent_source_group"] = ""
    master_df["adjacent_genre_bucket"] = ""
    master_df["core_union_flag"] = 1

    return songs_artist_df, restricted_df, master_df, schema_cols


def dedupe_with_flags(df: pd.DataFrame, builder, schema_cols: Sequence[str]) -> pd.DataFrame:
    base = df.copy()
    base["name_key"] = base["name_primary"].map(normalize_name)
    base = base[base["name_key"].ne("")].copy()
    base["completeness_score"] = base.apply(lambda row: count_nonmissing(row, KEY_COMPLETENESS_COLUMNS), axis=1)
    base["source_count"] = pd.to_numeric(base.get("source_count", 0), errors="coerce").fillna(0)
    base["country_relevance_score"] = pd.to_numeric(base.get("country_relevance_score", 0), errors="coerce").fillna(0)
    base = base.sort_values(
        ["completeness_score", "country_relevance_score", "source_count", "flag_restricted_sample", "flag_expanded_sample"],
        ascending=[False, False, False, False, False],
    )

    flags = (
        base.groupby("name_key", as_index=False)
        .agg(
            in_songfile=("in_songfile", "max"),
            in_country_restricted=("in_country_restricted", "max"),
            in_country_master=("in_country_master", "max"),
            in_adjacent_seed_pool=("in_adjacent_seed_pool", "max"),
            core_union_flag=("core_union_flag", "max"),
            artist_name_songfile_example=("artist_name_songfile_example", first_nonempty),
            adjacent_source_group=("adjacent_source_group", first_nonempty),
            adjacent_genre_bucket=("adjacent_genre_bucket", first_nonempty),
        )
    )
    flags["source_membership_count"] = (
        flags["in_songfile"] + flags["in_country_restricted"] + flags["in_country_master"] + flags["in_adjacent_seed_pool"]
    )

    deduped = builder.deduplicate(base.reindex(columns=list(schema_cols) + ["name_key"]))
    deduped["name_key"] = deduped["name_primary"].map(normalize_name)
    deduped = deduped.merge(flags, on="name_key", how="left")
    deduped["source_membership_count"] = deduped["source_membership_count"].fillna(0).map(safe_int)
    return deduped


def build_lookup(df: pd.DataFrame) -> Dict[str, dict]:
    lookup: Dict[str, List[dict]] = {}
    for _, row in df.iterrows():
        base = row.to_dict()
        keys: List[str] = []
        for col in ["name_primary", "stage_name", "birth_name"]:
            key = normalize_name(row.get(col, ""))
            if key:
                keys.append(key)
        aliases = nonempty_text(row.get("aliases", ""))
        for alias in aliases.split("|"):
            key = normalize_name(alias)
            if key:
                keys.append(key)
        for key in dict.fromkeys(keys):
            lookup.setdefault(key, []).append(base)

    collapsed: Dict[str, dict] = {}
    for key, records in lookup.items():
        ranked = sorted(
            records,
            key=lambda row: (
                -count_nonmissing(pd.Series(row), KEY_COMPLETENESS_COLUMNS),
                -safe_int(row.get("flag_restricted_sample", 0)),
                -safe_int(row.get("flag_expanded_sample", 0)),
                -safe_int(row.get("source_count", 0)),
            ),
        )
        collapsed[key] = ranked[0]
    return collapsed


def fill_missing_from_lookup(df: pd.DataFrame, lookup: Dict[str, dict], columns: Sequence[str]) -> Tuple[pd.DataFrame, int]:
    filled_rows = 0
    for idx, row in df.iterrows():
        key = normalize_name(row.get("name_primary", ""))
        if not key or key not in lookup:
            continue
        changed = False
        source = lookup[key]
        for col in columns:
            current = row.get(col)
            if pd.isna(current) or nonempty_text(current) == "":
                replacement = source.get(col)
                if pd.notna(replacement) and nonempty_text(replacement) != "":
                    df.at[idx, col] = replacement
                    changed = True
        if changed:
            filled_rows += 1
    return df, filled_rows


def match_seed_rows_to_detail_cache(seed_df: pd.DataFrame, detail_df: pd.DataFrame) -> pd.DataFrame:
    if seed_df.empty or detail_df.empty:
        return pd.DataFrame(columns=["seed_title", "source_seed", "seed_name_key", "wikidata_qid"])

    detail_lookup: Dict[str, str] = {}
    for _, row in detail_df.iterrows():
        qid = valid_qid(row.get("wikidata_qid", ""))
        if not qid:
            continue
        keys = [normalize_name(row.get("name_primary", ""))]
        aliases = nonempty_text(row.get("aliases", ""))
        for alias in aliases.split("|"):
            keys.append(normalize_name(alias))
        for key in dict.fromkeys([key for key in keys if key]):
            detail_lookup.setdefault(key, qid)

    matched = seed_df.copy()
    matched["wikidata_qid"] = matched["seed_name_key"].map(detail_lookup).fillna("")
    matched["wikidata_qid"] = matched["wikidata_qid"].map(valid_qid)
    matched = matched[matched["wikidata_qid"].astype(str).str.strip().ne("")].copy()
    return matched[["seed_title", "source_seed", "seed_name_key", "wikidata_qid"]].drop_duplicates(
        subset=["seed_title", "source_seed", "wikidata_qid"]
    )


def valid_qid(value: object) -> str:
    text = nonempty_text(value)
    return text if text.startswith("Q") else ""


def targeted_live_lookup(builder, artist_names: Sequence[str], source_seed: str, checkpoint_prefix: str) -> pd.DataFrame:
    cleaned_names = [name.strip() for name in artist_names if name and str(name).strip()]
    if not cleaned_names:
        return pd.DataFrame()

    s = builder.session()
    seed_cache_path = INTERMEDIATE_DIR / f"{checkpoint_prefix}_seed_candidates.csv"
    if seed_cache_path.exists():
        cached_seed_df = pd.read_csv(seed_cache_path)
        cached_seed_df["seed_title"] = cached_seed_df["seed_title"].map(nonempty_text)
        qid_map = dict(zip(cached_seed_df["seed_title"], cached_seed_df["wikidata_qid"].fillna("").astype(str)))
    else:
        qid_map = {}

    unresolved_batch = [name for name in cleaned_names if not qid_map.get(name)]
    batch_lookup_available = True
    for batch_start in range(0, len(unresolved_batch), 20):
        batch = unresolved_batch[batch_start : batch_start + 20]
        try:
            batch_map = builder.wikipedia_titles_to_qids(s, batch)
        except Exception as exc:
            log(f"Batch Wikipedia title-to-QID lookup failed for {len(batch)} names: {exc}")
            batch_map = {name: "" for name in batch}
            if is_network_resolution_failure(exc):
                batch_lookup_available = False
                qid_map.update({key: value or "" for key, value in batch_map.items()})
                partial_seed_df = pd.DataFrame(
                    [{"seed_title": name, "source_seed": source_seed, "wikidata_qid": qid_map.get(name, "")} for name in cleaned_names]
                )
                partial_seed_df.to_csv(seed_cache_path, index=False)
                log(
                    f"Targeted batch title lookup is offline for {checkpoint_prefix}; "
                    "skipping single-title fallback and preserving unresolved artists for review"
                )
                break
        qid_map.update({key: value or "" for key, value in batch_map.items()})
        partial_seed_df = pd.DataFrame(
            [{"seed_title": name, "source_seed": source_seed, "wikidata_qid": qid_map.get(name, "")} for name in cleaned_names]
        )
        partial_seed_df.to_csv(seed_cache_path, index=False)
        time.sleep(0.2)

    seed_rows = []
    unresolved = []
    for name in cleaned_names:
        qid = qid_map.get(name)
        if not qid:
            unresolved.append(name)
        seed_rows.append({"seed_title": name, "source_seed": source_seed, "wikidata_qid": qid or ""})

    if batch_lookup_available:
        consecutive_search_failures = 0
        for name in unresolved:
            try:
                qid = builder.search_wikidata_qid(s, name)
                consecutive_search_failures = 0
            except Exception as exc:
                log(f"Wikidata title search failed for {name}: {exc}")
                qid = None
                consecutive_search_failures += 1
                if is_network_resolution_failure(exc):
                    log(
                        f"Targeted single-title lookup is offline for {checkpoint_prefix}; "
                        "stopping fallback search and preserving unresolved artists for review"
                    )
                    break
            if qid:
                for row in seed_rows:
                    if row["seed_title"] == name:
                        row["wikidata_qid"] = qid
                        break
                qid_map[name] = qid
            pd.DataFrame(seed_rows).to_csv(seed_cache_path, index=False)
            if consecutive_search_failures >= 10:
                log(f"Targeted single-title lookup hit repeated rate limits for {checkpoint_prefix}; stopping early and preserving unresolved artists for review")
                break
            time.sleep(0.1)

    seed_df = pd.DataFrame(seed_rows)
    seed_df["wikidata_qid"] = seed_df["wikidata_qid"].map(valid_qid)
    resolved_df = seed_df[seed_df["wikidata_qid"].astype(str).str.strip().ne("")].copy()
    if resolved_df.empty:
        return pd.DataFrame()

    checkpoint_path = INTERMEDIATE_DIR / f"{checkpoint_prefix}_wikidata_details.csv"
    resolved_qids = sorted(dict.fromkeys(resolved_df["wikidata_qid"].map(valid_qid).tolist()))
    resolved_qids = [qid for qid in resolved_qids if qid]
    if not resolved_qids:
        return pd.DataFrame()
    try:
        detail_df = builder.wikidata_details(s, resolved_qids, checkpoint_path=checkpoint_path)
    except Exception as exc:
        log(f"Targeted detail fetch stopped early for {checkpoint_prefix}; proceeding with cached partial detail set: {exc}")
        if checkpoint_path.exists():
            detail_df = pd.read_csv(checkpoint_path)
        else:
            return pd.DataFrame()
    dataset_df = builder.prepare_dataset(resolved_df, detail_df, s)
    dataset_df = builder.deduplicate(dataset_df)
    dataset_df["name_key"] = dataset_df["name_primary"].map(normalize_name)
    return dataset_df


def repair_key_metadata(
    df: pd.DataFrame,
    builder,
    checkpoint_prefix: str,
    selector_mask: pd.Series | None = None,
    max_candidates: int | None = None,
) -> pd.DataFrame:
    missing_mask = (
        df["birth_year"].isna()
        | df["birth_state"].fillna("").astype(str).str.strip().eq("")
        | df["birth_country"].fillna("").astype(str).str.strip().eq("")
        | df["birth_city"].fillna("").astype(str).str.strip().eq("")
    )
    if selector_mask is not None:
        missing_mask = missing_mask & selector_mask.fillna(False)
    candidate_names = sorted(df.loc[missing_mask, "name_primary"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().unique())
    if max_candidates is not None and len(candidate_names) > max_candidates:
        candidate_names = candidate_names[:max_candidates]
    if not candidate_names:
        return df

    log(f"Running targeted enrichment for {len(candidate_names)} artists with missing key metadata")
    live_df = targeted_live_lookup(builder, candidate_names, f"{checkpoint_prefix}_targeted", checkpoint_prefix)
    if live_df.empty:
        log("Targeted enrichment returned no additional rows")
        return df
    lookup = build_lookup(live_df)
    df, filled_rows = fill_missing_from_lookup(
        df,
        lookup,
        ["name_primary", "birth_year", "birth_city", "birth_county", "birth_state", "birth_state_abbr", "birth_country", "wikidata_qid", "musicbrainz_mbid", "birth_date", "birth_place_raw", "us_macro_region", "genres_raw", "genres_normalized"],
    )
    log(f"Filled missing key metadata for {filled_rows} artists from targeted enrichment")
    return df


def fetch_artists_by_tag(tag: str, limit: int = 100, offset: int = 0) -> Tuple[List[str], int]:
    url = "https://musicbrainz.org/ws/2/artist/"
    params = {
        "query": f"tag:{tag} AND (country:US OR area:US)",
        "fmt": "json",
        "limit": limit,
        "offset": offset,
    }
    headers = {"User-Agent": "SoundOfCultureResearch/1.0 (local pipeline)"}
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    artists = [item["name"] for item in data.get("artists", []) if item.get("name")]
    total = int(data.get("count", 0) or 0)
    return artists, total


def fetch_adjacent_tag_seed_rows() -> pd.DataFrame:
    rows: List[dict] = []
    for tag in ADJACENT_TAGS:
        log(f"Fetching adjacent-genre seed artists for tag '{tag}' from MusicBrainz")
        try:
            artists, total = fetch_artists_by_tag(tag, limit=100, offset=0)
        except Exception as exc:
            log(f"Tag seed fetch failed for {tag}: {exc}")
            continue
        rows.extend({"seed_title": artist, "source_seed": f"MusicBrainz tag:{tag}"} for artist in artists)
        upper = min(total, 500)
        for offset in range(100, upper, 100):
            time.sleep(1.1)
            try:
                batch, _ = fetch_artists_by_tag(tag, limit=100, offset=offset)
            except Exception as exc:
                log(f"Tag seed fetch failed for {tag} offset {offset}: {exc}")
                break
            rows.extend({"seed_title": artist, "source_seed": f"MusicBrainz tag:{tag}"} for artist in batch)
    df = pd.DataFrame(rows).drop_duplicates()
    if not df.empty:
        df.to_csv(TAG_SEED_CACHE, index=False)
    return df


def classify_adjacent_bucket(row: pd.Series) -> str:
    haystack = "|".join(
        [
            nonempty_text(row.get("genres_raw", "")).lower(),
            nonempty_text(row.get("source_seed", "")).lower(),
            nonempty_text(row.get("occupations", "")).lower(),
        ]
    )
    rules = [
        ("western swing", "western swing"),
        ("rockabilly", "rockabilly"),
        ("bluegrass", "bluegrass"),
        ("country gospel", "country gospel"),
        ("gospel", "country gospel"),
        ("roots rock", "roots rock"),
        ("outlaw country", "outlaw country"),
        ("americana", "americana"),
        ("folk", "folk"),
    ]
    for pattern, bucket in rules:
        if pattern in haystack:
            return bucket
    return "other_adjacent"


def build_core_universe(builder, schema_cols: Sequence[str]) -> pd.DataFrame:
    songs_artist_df, restricted_df, master_df, _ = load_current_country_frames(builder)
    combined = pd.concat([songs_artist_df, restricted_df, master_df], ignore_index=True, sort=False)
    combined = coerce_numeric_if_needed(
        combined,
        ["birth_year", "death_year", "country_relevance_score", "source_count", "flag_restricted_sample", "flag_expanded_sample"],
    )
    core_df = dedupe_with_flags(combined, builder, schema_cols)
    core_df = repair_key_metadata(core_df, builder, checkpoint_prefix="artist_universe_core")
    core_df["adjacent_source_group"] = core_df["adjacent_source_group"].fillna("")
    core_df["adjacent_genre_bucket"] = core_df["adjacent_genre_bucket"].fillna("")
    core_df["core_union_flag"] = 1
    return core_df


def build_adjacent_seed_pool(core_df: pd.DataFrame) -> pd.DataFrame:
    core_names = set(core_df["name_key"].dropna().tolist())
    seed_rows: List[dict] = []

    if SEED_JSON.exists():
        names = json.loads(SEED_JSON.read_text())
        seed_rows.extend({"seed_title": name, "source_seed": "seed_country_artists.json"} for name in names if str(name).strip())

    tag_df = pd.read_csv(TAG_SEED_CACHE) if TAG_SEED_CACHE.exists() else fetch_adjacent_tag_seed_rows()
    if not tag_df.empty:
        seed_rows.extend(tag_df.to_dict("records"))

    seed_df = pd.DataFrame(seed_rows)
    if seed_df.empty:
        return seed_df

    seed_df["seed_title"] = seed_df["seed_title"].map(nonempty_text)
    seed_df["seed_name_key"] = seed_df["seed_title"].map(normalize_name)
    seed_df = seed_df[seed_df["seed_name_key"].ne("")].copy()
    seed_df = seed_df[~seed_df["seed_name_key"].isin(core_names)].copy()
    seed_df = seed_df.drop_duplicates(subset=["seed_title", "source_seed"])
    return seed_df


def resolve_adjacent_seeds(builder, seed_df: pd.DataFrame) -> pd.DataFrame:
    if seed_df.empty:
        return seed_df

    if ADJACENT_SEED_CACHE.exists():
        cached = pd.read_csv(ADJACENT_SEED_CACHE)
        cached["seed_title"] = cached["seed_title"].map(nonempty_text)
        cached["source_seed"] = cached["source_seed"].map(nonempty_text)
        cached["seed_name_key"] = cached["seed_title"].map(normalize_name)
        cached_keys = set(zip(cached["seed_title"], cached["source_seed"]))
        pending = seed_df[[key not in cached_keys for key in zip(seed_df["seed_title"], seed_df["source_seed"])]].copy()
        resolved = cached
    else:
        pending = seed_df.copy()
        resolved = pd.DataFrame(columns=["seed_title", "source_seed", "seed_name_key", "wikidata_qid"])

    if pending.empty:
        return resolved.drop_duplicates(subset=["seed_title", "source_seed"])

    s = builder.session()
    qid_map: Dict[str, str] = {}
    titles = pending["seed_title"].tolist()
    consecutive_batch_failures = 0
    batch_lookup_available = True
    for batch_start in range(0, len(titles), 20):
        batch = titles[batch_start : batch_start + 20]
        try:
            batch_map = builder.wikipedia_titles_to_qids(s, batch)
            consecutive_batch_failures = 0
        except Exception as exc:
            log(f"Adjacent batch Wikipedia title-to-QID lookup failed for {len(batch)} names: {exc}")
            batch_map = {title: "" for title in batch}
            consecutive_batch_failures += 1
            if is_network_resolution_failure(exc):
                batch_lookup_available = False
        qid_map.update({key: valid_qid(value) for key, value in batch_map.items()})
        pending["wikidata_qid"] = pending["seed_title"].map(qid_map).fillna("")
        pending.to_csv(ADJACENT_SEED_CACHE, index=False)
        if not batch_lookup_available:
            log("Adjacent batch title lookup is offline; skipping single-title fallback and preserving unresolved seeds for review")
            break
        if consecutive_batch_failures >= 5:
            log("Adjacent batch title resolution hit repeated rate limits; stopping batch lookups early and preserving unresolved seeds for fallback/review")
            break
        time.sleep(0.2)

    unresolved_mask = pending["wikidata_qid"].astype(str).str.strip().eq("")
    unresolved_titles = pending.loc[unresolved_mask, "seed_title"].tolist()
    log(
        f"Adjacent seeds: {int((pending['wikidata_qid'].astype(str).str.strip() != '').sum())} resolved directly; "
        f"{len(unresolved_titles)} still unresolved before fallback search"
    )

    if batch_lookup_available:
        fallback_titles = unresolved_titles[:300]
        consecutive_search_failures = 0
        for index, title in enumerate(fallback_titles, start=1):
            try:
                qid = builder.search_wikidata_qid(s, title) or ""
                consecutive_search_failures = 0
            except Exception as exc:
                log(f"Wikidata title search failed for adjacent seed {title}: {exc}")
                qid = ""
                consecutive_search_failures += 1
                if is_network_resolution_failure(exc):
                    log("Adjacent single-title fallback is offline; stopping further fallback searches and preserving unresolved seeds for review")
                    break
            pending.loc[pending["seed_title"].eq(title), "wikidata_qid"] = qid
            if index % 50 == 0 or index == len(fallback_titles):
                checkpoint = pd.concat([resolved, pending], ignore_index=True).drop_duplicates(subset=["seed_title", "source_seed"], keep="last")
                checkpoint.to_csv(ADJACENT_SEED_CACHE, index=False)
                log(f"Adjacent fallback search progress: {index}/{len(fallback_titles)} titles")
            if consecutive_search_failures >= 10:
                log("Adjacent fallback title search hit repeated rate limits; stopping single-title searches early and preserving unresolved seeds for review")
                break
            time.sleep(0.1)

    resolved = pd.concat([resolved, pending], ignore_index=True).drop_duplicates(subset=["seed_title", "source_seed"], keep="last")
    resolved.to_csv(ADJACENT_SEED_CACHE, index=False)
    unresolved_after = resolved[resolved["wikidata_qid"].astype(str).str.strip().eq("")][["seed_title", "source_seed"]].drop_duplicates()
    unresolved_after.to_csv(ADJACENT_UNRESOLVED_CACHE, index=False)
    return resolved


def build_adjacent_universe(builder, core_df: pd.DataFrame, schema_cols: Sequence[str]) -> pd.DataFrame:
    seed_df = build_adjacent_seed_pool(core_df)
    if seed_df.empty:
        return pd.DataFrame(columns=list(schema_cols) + EXTRA_COLUMNS)

    resolved_seed_df = resolve_adjacent_seeds(builder, seed_df)
    resolved_only = resolved_seed_df[resolved_seed_df["wikidata_qid"].astype(str).str.strip().ne("")].copy()

    if ADJACENT_DETAIL_CACHE.exists():
        detail_df = pd.read_csv(ADJACENT_DETAIL_CACHE)
        done_qids = set(detail_df["wikidata_qid"].dropna().astype(str))
    else:
        detail_df = pd.DataFrame()
        done_qids = set()

    salvaged_resolved = match_seed_rows_to_detail_cache(seed_df, detail_df)
    if not salvaged_resolved.empty:
        resolved_only = pd.concat([resolved_only, salvaged_resolved], ignore_index=True).drop_duplicates(
            subset=["seed_title", "source_seed", "wikidata_qid"], keep="first"
        )

    if resolved_only.empty:
        detail_df = pd.DataFrame()
        done_qids = set()
        # proceed to unresolved placeholder construction below instead of returning early

    pending_qids = [qid for qid in resolved_only["wikidata_qid"].map(valid_qid).drop_duplicates().tolist() if qid and qid not in done_qids]
    if pending_qids:
        s = builder.session()
        try:
            new_detail_df = builder.wikidata_details(s, pending_qids, checkpoint_path=ADJACENT_DETAIL_CACHE)
            detail_df = pd.concat([detail_df, new_detail_df], ignore_index=True).drop_duplicates(subset=["wikidata_qid"], keep="last")
            detail_df.to_csv(ADJACENT_DETAIL_CACHE, index=False)
        except Exception as exc:
            log(f"Adjacent Wikidata detail fetch stopped early; proceeding with cached partial detail set: {exc}")
            if ADJACENT_DETAIL_CACHE.exists():
                detail_df = pd.read_csv(ADJACENT_DETAIL_CACHE)
            elif detail_df.empty:
                detail_df = pd.DataFrame()

    available_qids = set(detail_df["wikidata_qid"].map(valid_qid)) if not detail_df.empty else set()
    available_qids.discard("")
    resolved_available = resolved_only[resolved_only["wikidata_qid"].map(valid_qid).isin(available_qids)].copy()
    resolved_missing_detail = resolved_only[~resolved_only["wikidata_qid"].map(valid_qid).isin(available_qids)].copy()

    if not resolved_available.empty:
        s = builder.session()
        adjacent_df = builder.prepare_dataset(resolved_available, detail_df, s)
        adjacent_df = builder.deduplicate(adjacent_df)
        adjacent_df = adjacent_df.reindex(columns=schema_cols)
    else:
        adjacent_df = pd.DataFrame(columns=schema_cols)

    unresolved_seed_rows = resolved_seed_df[resolved_seed_df["wikidata_qid"].astype(str).str.strip().eq("")].copy()
    if not resolved_missing_detail.empty:
        unresolved_seed_rows = pd.concat(
            [
                unresolved_seed_rows,
                resolved_missing_detail[["seed_title", "source_seed", "seed_name_key", "wikidata_qid"]],
            ],
            ignore_index=True,
        ).drop_duplicates(subset=["seed_title", "source_seed"], keep="last")

    unresolved_rows = []
    for _, row in unresolved_seed_rows.iterrows():
        unresolved_rows.append(
            {
                "artist_id": "",
                "name_primary": row["seed_title"],
                "birth_name": "",
                "stage_name": "",
                "aliases": "",
                "wikidata_qid": nonempty_text(row.get("wikidata_qid", "")),
                "musicbrainz_mbid": "",
                "isni": "",
                "viaf_id": "",
                "wikipedia_url": "",
                "birth_date": "",
                "birth_year": None,
                "birth_place_raw": "",
                "birth_city": "",
                "birth_county": "",
                "birth_state": "",
                "birth_state_abbr": "",
                "birth_country": "",
                "death_date": "",
                "death_year": None,
                "death_place_raw": "",
                "citizenship": "",
                "occupations": "",
                "genres_raw": "",
                "genres_normalized": "",
                "country_relevance_score": 0.0,
                "instruments": "",
                "member_of": "",
                "record_labels": "",
                "awards": "",
                "official_website": "",
                "birth_decade": None,
                "us_macro_region": "Unknown",
                "is_deceased": 0,
                "age_or_age_at_death": None,
                "is_us_born": 0,
                "is_solo_person": 1,
                "is_country_core": 0,
                "is_country_broad": 0,
                "flag_restricted_sample": 0,
                "flag_expanded_sample": 0,
                "sample_membership": "",
                "inclusion_reason": "",
                "exclusion_reason": "adjacent_seed_unresolved",
                "source_primary": "adjacent_seed_pool",
                "source_secondary": "",
                "source_seed": row["source_seed"],
                "evidence_urls": "",
                "source_count": 0,
                "birth_date_confidence": "low",
                "birth_place_confidence": "low",
                "genre_confidence": "low",
                "manual_review_needed": 1,
                "notes": "Adjacent-genre seed not fully resolved during this build",
            }
        )

    if unresolved_rows:
        adjacent_df = pd.concat([adjacent_df, pd.DataFrame(unresolved_rows)], ignore_index=True, sort=False)

    adjacent_df["name_key"] = adjacent_df["name_primary"].map(normalize_name)
    adjacent_df = adjacent_df[~adjacent_df["name_key"].isin(set(core_df["name_key"].dropna()))].copy()
    adjacent_df["in_songfile"] = 0
    adjacent_df["in_country_restricted"] = 0
    adjacent_df["in_country_master"] = 0
    adjacent_df["in_adjacent_seed_pool"] = 1
    adjacent_df["artist_name_songfile_example"] = ""
    adjacent_df["adjacent_source_group"] = adjacent_df["source_seed"].fillna("").astype(str)
    adjacent_df["adjacent_genre_bucket"] = adjacent_df.apply(classify_adjacent_bucket, axis=1)
    adjacent_df["core_union_flag"] = 0
    adjacent_df["source_membership_count"] = 1
    adjacent_repair_mask = (
        adjacent_df["wikidata_qid"].fillna("").astype(str).str.startswith("Q")
        | adjacent_df["musicbrainz_mbid"].fillna("").astype(str).str.strip().ne("")
        | adjacent_df["genres_raw"].fillna("").astype(str).str.strip().ne("")
        | adjacent_df["birth_place_raw"].fillna("").astype(str).str.strip().ne("")
    )
    adjacent_df = repair_key_metadata(
        adjacent_df,
        builder,
        checkpoint_prefix="artist_universe_adjacent",
        selector_mask=adjacent_repair_mask,
        max_candidates=500,
    )
    return adjacent_df


def save_csv_and_dta(df: pd.DataFrame, basename: str) -> None:
    csv_path = OUTPUT_DIR / f"{basename}.csv"
    dta_path = OUTPUT_DIR / f"{basename}.dta"
    df.to_csv(csv_path, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
    stata_df = df.copy()
    for col in stata_df.columns:
        if not pd.api.types.is_numeric_dtype(stata_df[col]):
            stata_df[col] = stata_df[col].fillna("").astype(str)
    stata_df.to_stata(dta_path, write_index=False, version=118)
    log(f"Saved {csv_path.name} and {dta_path.name}")


def missing_count(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    series = df[col]
    if pd.api.types.is_numeric_dtype(series):
        return int(series.isna().sum())
    return int(series.fillna("").astype(str).str.strip().eq("").sum())


def write_qc_report(core_df: pd.DataFrame, adjacent_df: pd.DataFrame, full_df: pd.DataFrame) -> None:
    lines = [
        "# artist_universe_qc_report",
        "",
        "## Counts",
        f"- core country-union artists: {len(core_df)}",
        f"- adjacent-only artists added: {len(adjacent_df)}",
        f"- full artist universe: {len(full_df)}",
        "",
        "## Missingness In Full Universe",
    ]
    for col in ["name_primary", "birth_year", "birth_city", "birth_state", "birth_country", "wikidata_qid", "musicbrainz_mbid"]:
        lines.append(f"- {col}: {missing_count(full_df, col)} missing")
    lines.extend(
        [
            "",
            "## Membership",
            f"- artists linked to current songs file: {int(full_df['in_songfile'].fillna(0).sum())}",
            f"- artists in country_artists_restricted: {int(full_df['in_country_restricted'].fillna(0).sum())}",
            f"- artists in country_artists_master: {int(full_df['in_country_master'].fillna(0).sum())}",
            f"- artists coming from adjacent-genre seed pool: {int(full_df['in_adjacent_seed_pool'].fillna(0).sum())}",
        ]
    )
    (OUTPUT_DIR / "artist_universe_qc_report.md").write_text("\n".join(lines), encoding="utf-8")


def write_method_note(core_df: pd.DataFrame, full_df: pd.DataFrame) -> None:
    song_file = pd.read_csv(SONGS_CSV, low_memory=False)
    songs_count = len(song_file)
    song_artists = song_file["artist_name"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
    note = f"""# Methodological Note: Three Denominators

## Current Local Counts

- songs in current country song file: {songs_count}
- unique artists in current song file: {song_artists}
- artists in country core union (`songs + restricted + master`): {len(core_df)}
- artists in full adjacent-genre universe: {len(full_df)}

## Denominator 1: UG-Observable Country Universe

This is the narrowest denominator. It means songs and artists that are observable on Ultimate Guitar under the search and filtering strategy actually used by the project.

In our pipeline this denominator is constrained by:

- the Ultimate Guitar platform itself
- the initial genre-led discovery (`Country` and `Americana`)
- artist-led recovery on the country artist registry
- the decision to focus primarily on chord/tab-visible songs

Interpretation: the current song dataset is best treated as a sample from this denominator, not from the full historical universe of country recordings.

## Denominator 2: Charted Country Universe

This is broader. It means country songs that entered nationally visible chart ecosystems such as Billboard country charts or comparable canonical repertoires.

Interpretation: this denominator is larger than the UG-observable universe because many charted songs never generate stable tablature presence, and many chart entries predate modern online tab ecosystems.

## Denominator 3: All-Recorded Country Universe

This is the broadest denominator. It includes singles, album cuts, regional releases, local recordings, catalog material, gospel/bluegrass/roots-borderline material, and historically obscure tracks.

Interpretation: the current project certainly covers only a small share of this denominator. A proper approximation would require discography-first reconstruction outside Ultimate Guitar.

## Practical Implication

The right interpretation of the current data is therefore:

1. best coverage for artists and songs that are UG-observable
2. partial coverage for the charted country repertoire
3. low coverage of the full recorded country universe

## How This Artist Universe Helps

The new artist universe separates the artist side from the song side. It expands from the current country core to adjacent genres such as folk, americana, bluegrass, rockabilly, western swing, outlaw country, roots rock, and country gospel. This should be used as the staging frame before any new Ultimate Guitar song discovery is attempted.
"""
    (OUTPUT_DIR / "artist_universe_methodological_note.md").write_text(note, encoding="utf-8")


def main() -> None:
    ensure_dirs()
    builder = load_builder()
    make_builder_rate_limit_tolerant(builder)
    builder.ensure_dirs()
    schema_cols = artist_schema_columns(builder)

    log("Building core artist universe from songs + restricted + master")
    core_df = build_core_universe(builder, schema_cols)

    log("Building adjacent-genre artist universe")
    adjacent_df = build_adjacent_universe(builder, core_df, schema_cols)

    log("Combining core and adjacent artist universes")
    combined_df = pd.concat([core_df, adjacent_df], ignore_index=True, sort=False)
    combined_df = dedupe_with_flags(combined_df, builder, schema_cols)
    combined_repair_mask = (
        combined_df["core_union_flag"].fillna(0).map(safe_int).eq(1)
        | combined_df["source_membership_count"].fillna(0).map(safe_int).ge(2)
        | combined_df["wikidata_qid"].fillna("").astype(str).str.startswith("Q")
        | combined_df["musicbrainz_mbid"].fillna("").astype(str).str.strip().ne("")
    )
    combined_df = repair_key_metadata(
        combined_df,
        builder,
        checkpoint_prefix="artist_universe_full",
        selector_mask=combined_repair_mask,
        max_candidates=600,
    )

    review_df = combined_df[
        combined_df["manual_review_needed"].fillna(0).map(safe_int).eq(1)
        | combined_df["birth_year"].isna()
        | combined_df["birth_state"].fillna("").astype(str).str.strip().eq("")
        | combined_df["birth_country"].fillna("").astype(str).str.strip().eq("")
    ].copy()

    save_csv_and_dta(core_df.reindex(columns=list(schema_cols) + EXTRA_COLUMNS), CORE_OUTPUT_BASENAME)
    save_csv_and_dta(adjacent_df.reindex(columns=list(schema_cols) + EXTRA_COLUMNS), ADJACENT_ONLY_OUTPUT_BASENAME)
    save_csv_and_dta(combined_df.reindex(columns=list(schema_cols) + EXTRA_COLUMNS), ADJACENT_OUTPUT_BASENAME)
    save_csv_and_dta(review_df.reindex(columns=list(schema_cols) + EXTRA_COLUMNS), REVIEW_OUTPUT_BASENAME)

    schema_df = pd.read_csv(builder.SCHEMA_PATH).copy()
    extra_rows = pd.DataFrame(
        [
            {"column_name": "in_songfile", "data_type": "byte", "required": "no", "allowed_values": "0/1", "description": "Artist appears in current songs file", "example": "1"},
            {"column_name": "in_country_restricted", "data_type": "byte", "required": "no", "allowed_values": "0/1", "description": "Artist appears in country_artists_restricted", "example": "1"},
            {"column_name": "in_country_master", "data_type": "byte", "required": "no", "allowed_values": "0/1", "description": "Artist appears in country_artists_master", "example": "1"},
            {"column_name": "in_adjacent_seed_pool", "data_type": "byte", "required": "no", "allowed_values": "0/1", "description": "Artist was added from adjacent-genre seed expansion", "example": "1"},
            {"column_name": "source_membership_count", "data_type": "integer", "required": "no", "allowed_values": "", "description": "Count of source memberships across core and adjacent pools", "example": "2"},
            {"column_name": "artist_name_songfile_example", "data_type": "string", "required": "no", "allowed_values": "", "description": "Observed artist spelling from the current song file", "example": "Doyle Lawson & Quicksilver"},
            {"column_name": "adjacent_genre_bucket", "data_type": "string", "required": "no", "allowed_values": "", "description": "Primary adjacent-genre bucket inferred after enrichment", "example": "bluegrass"},
            {"column_name": "adjacent_source_group", "data_type": "string", "required": "no", "allowed_values": "", "description": "Seed source used for adjacent-genre expansion", "example": "MusicBrainz tag:bluegrass"},
            {"column_name": "core_union_flag", "data_type": "byte", "required": "no", "allowed_values": "0/1", "description": "Artist belongs to the core country union (songs, restricted, or master)", "example": "1"},
        ]
    )
    full_dictionary = pd.concat([schema_df, extra_rows], ignore_index=True)
    full_dictionary.to_csv(OUTPUT_DIR / "artist_universe_data_dictionary.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    full_dictionary.to_stata(OUTPUT_DIR / "artist_universe_data_dictionary.dta", write_index=False, version=118)

    write_qc_report(core_df, adjacent_df, combined_df)
    write_method_note(core_df, combined_df)
    log("Extended artist universe build completed")


if __name__ == "__main__":
    main()
