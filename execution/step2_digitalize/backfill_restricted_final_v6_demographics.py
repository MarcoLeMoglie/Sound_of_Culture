import re
import importlib.util
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

BASE_DIR = Path("data/processed_datasets/country_artists")
TARGET_CSV = BASE_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.csv"
TARGET_DTA = BASE_DIR / "Sound_of_Culture_Country_Restricted_Final_v6.dta"
BACKFILL_DIR = BASE_DIR / "intermediate" / "restricted_final_v6_backfill"
PRECOMPUTED_LOOKUP_FILE = BACKFILL_DIR / "precomputed_targeted_live_lookup.csv"
COUNTRY_BUILDER_PATH = Path(
    "execution/phase_01_dataset_construction/build_country_artists_dataset.py"
)

REFERENCE_FILES: List[Tuple[str, str, int]] = [
    ("country_artists_restricted.csv", "restricted", 1),
    ("country_artists_master.csv", "master", 2),
    ("manual_review_queue.csv", "manual_review", 3),
    ("country_artists_excluded_or_non_us.csv", "excluded", 4),
]

MANUAL_CANONICAL_EQUIVALENTS = {
    "the del mccoury band": "del mccoury",
    "christian lopez band": "christian lopez",
    "star anna and the laughing dog": "star anna",
    "the roger springer band": "roger springer",
    "roy acuff and his smoky mountain boys": "roy acuff",
    "carson robison and his buckaroos": "carson robison",
    "carson robison and his pioneers": "carson robison",
    "james intveld and the rockin shadows": "james intveld",
    "the bernie leadon michael georgiades band": "bernie leadon",
}

SEARCH_TITLE_OVERRIDES = {
    "doyle lawson and quicksilver": ["Doyle Lawson", "Doyle Lawson & Quicksilver"],
    "the infamous stringdusters": ["The Infamous Stringdusters", "Infamous Stringdusters"],
    "the del mccoury band": ["The Del McCoury Band", "Del McCoury"],
    "the amazing rhythm aces": ["The Amazing Rhythm Aces", "Amazing Rhythm Aces"],
    "valdy nyonk": ["Valdy"],
    "the osborne brothers": ["The Osborne Brothers", "Osborne Brothers"],
    "christian lopez band": ["Christian Lopez", "Christian Lopez Band"],
    "lady a": ["Lady A", "Lady Antebellum"],
    "hardy sandhu": ["Harrdy Sandhu", "Hardy Sandhu"],
    "the chicks": ["The Chicks", "Dixie Chicks"],
    "christian lopez": ["Christian Lopez"],
    "donny and marie osmond": ["Donny & Marie", "Donny and Marie Osmond"],
    "beyonce": ["Beyonce", "Beyoncé"],
    "conjunto rio grande": ["Conjunto Rio Grande", "Conjunto Río Grande"],
    "the bernie leadon michael georgiades band": ["Bernie Leadon", "The Bernie Leadon-Michael Georgiades Band"],
    "james intveld and the rockin shadows": ["James Intveld", "James Intveld and The Rockin' Shadows"],
    "alee": ["Alee"],
    "roy acuff and his smoky mountain boys": ["Roy Acuff", "Roy Acuff and his Smoky Mountain Boys"],
    "star anna and the laughing dog": ["Star Anna", "Star Anna and The Laughing Dog"],
    "carson robison and his buckaroos": ["Carson Robison", "Carson Robison and his Buckaroos"],
    "leen and leahy": ["Leen and Leahy", "Leen & Leahy"],
    "aleen aldas": ["Aleen Aldas"],
    "cameron leahy": ["Cameron Leahy"],
    "eagles": ["Eagles (band)", "Eagles"],
    "johnny duncan and the blue grass boys": ["Johnny Duncan", "Johnny Duncan and the Blue Grass Boys"],
    "valdy marshall": ["Valdy"],
    "alee kinder": ["Alee", "Alee Kinder"],
    "tommy leahy": ["Tommy Leahy"],
    "the roger springer band": ["Roger Springer", "The Roger Springer Band"],
    "carson robison and his pioneers": ["Carson Robison", "Carson Robison and his Pioneers"],
}

MATCH_COLUMNS = ["name_primary", "stage_name", "birth_name", "aliases"]
ARTIST_COLUMNS = [
    "artist_id",
    "name_primary",
    "birth_name",
    "stage_name",
    "aliases",
    "wikidata_qid",
    "musicbrainz_mbid",
    "isni",
    "viaf_id",
    "wikipedia_url",
    "birth_date",
    "birth_year",
    "birth_place_raw",
    "birth_city",
    "birth_county",
    "birth_state",
    "birth_state_abbr",
    "birth_country",
    "death_date",
    "death_year",
    "death_place_raw",
    "citizenship",
    "occupations",
    "genres_raw",
    "genres_normalized",
    "country_relevance_score",
    "instruments",
    "member_of",
    "record_labels",
    "awards",
    "official_website",
    "birth_decade",
    "us_macro_region",
    "is_deceased",
    "age_or_age_at_death",
    "is_us_born",
    "is_solo_person",
    "is_country_core",
    "is_country_broad",
    "flag_restricted_sample",
    "flag_expanded_sample",
    "sample_membership",
    "inclusion_reason",
    "exclusion_reason",
    "source_primary",
    "source_secondary",
    "source_seed",
    "evidence_urls",
    "source_count",
    "birth_date_confidence",
    "birth_place_confidence",
    "genre_confidence",
    "manual_review_needed",
    "notes",
]

NUMERIC_ARTIST_COLUMNS = {
    "birth_year",
    "death_year",
    "country_relevance_score",
    "birth_decade",
    "is_deceased",
    "age_or_age_at_death",
    "is_us_born",
    "is_solo_person",
    "is_country_core",
    "is_country_broad",
    "flag_restricted_sample",
    "flag_expanded_sample",
    "source_count",
    "manual_review_needed",
}


def log(message: str) -> None:
    print(f"[restricted-v6-backfill] {message}", flush=True)


def load_country_builder():
    spec = importlib.util.spec_from_file_location("country_builder_module", COUNTRY_BUILDER_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load country builder from {COUNTRY_BUILDER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalize_name(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    text = text.strip().lower()
    text = re.sub(r"\([^)]*\)", "", text)
    text = text.replace("&", " and ")
    text = text.replace("+", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_space(value: object) -> str:
    text = "" if pd.isna(value) else str(value)
    return re.sub(r"\s+", " ", text).strip()


def maybe_fix_mojibake(text: str) -> str:
    if not text:
        return text
    if "Ã" not in text and "Â" not in text:
        return text
    for src, dst in [("Ã©", "é"), ("Ã¨", "è"), ("Ã¡", "á"), ("Ã³", "ó"), ("Â", "")]:
        text = text.replace(src, dst)
    try:
        repaired = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        repaired = text
    return repaired or text


def generate_search_titles(name: str) -> List[str]:
    raw = str(name).strip()
    if not raw:
        return []

    candidates: List[str] = []

    def add_candidate(value: str) -> None:
        value = normalize_space(value)
        if value and value not in candidates:
            candidates.append(value)

    repaired = normalize_space(maybe_fix_mojibake(raw))
    add_candidate(raw)
    if repaired != raw:
        add_candidate(repaired)

    base = normalize_name(repaired or raw)
    override_key = base
    if base.startswith("beyonc"):
        override_key = "beyonce"
    for candidate in SEARCH_TITLE_OVERRIDES.get(override_key, []):
        add_candidate(candidate)

    stripped_parenthetical = normalize_space(re.sub(r"\([^)]*\)", "", repaired))
    add_candidate(stripped_parenthetical)

    if stripped_parenthetical.lower().startswith("the "):
        add_candidate(stripped_parenthetical[4:])

    simplified = stripped_parenthetical
    for pattern in [
        r"\s+and\s+his\s+.+$",
        r"\s+and\s+the\s+.+$",
        r"\s*&\s+.+$",
        r"\s+band$",
    ]:
        simplified = re.sub(pattern, "", simplified, flags=re.IGNORECASE).strip()
        add_candidate(simplified)

    return candidates


def target_missing_mask(df: pd.DataFrame) -> pd.Series:
    return (
        df["artist_name"].notna()
        & df["artist_name"].astype(str).str.strip().ne("")
        & (df["name_primary"].isna() | df["name_primary"].astype(str).str.strip().eq(""))
    )


def prepare_target_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in ARTIST_COLUMNS:
        if col not in df.columns:
            continue
        if col not in NUMERIC_ARTIST_COLUMNS:
            df[col] = df[col].astype("object")
    return df


def existing_value_count(df: pd.DataFrame, columns: List[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for col in columns:
        if col not in df.columns:
            continue
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            counts[col] = int(series.notna().sum())
        else:
            counts[col] = int(series.fillna("").astype(str).str.strip().ne("").sum())
    return counts


def split_aliases(value: object) -> List[str]:
    if pd.isna(value):
        return []
    return [piece.strip() for piece in str(value).split("|") if piece.strip()]


def string_or_empty(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def normalize_notes(value: object) -> str:
    text = string_or_empty(value)
    if not text:
        return ""
    pieces = [piece.strip() for piece in text.split("|") if piece.strip() and piece.strip().lower() != "nan"]
    return "|".join(dict.fromkeys(pieces))


def coerce_value_for_column(df: pd.DataFrame, col: str, value: object):
    if pd.isna(value) or value == "":
        if pd.api.types.is_numeric_dtype(df[col]):
            return pd.NA
        return ""
    return value


def safe_float(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: object) -> int:
    return int(safe_float(value))


def best_record(records: List[dict]) -> dict:
    ranked = sorted(
        records,
        key=lambda row: (
            row["_source_rank"],
            row["_match_rank"],
            -safe_float(row.get("country_relevance_score", 0)),
            -safe_int(row.get("source_count", 0)),
        ),
    )
    return ranked[0]


def lookup_rows_from_reference(df: pd.DataFrame, source_name: str, source_rank: int) -> Dict[str, dict]:
    lookup: Dict[str, List[dict]] = {}
    for _, row in df.iterrows():
        base = row.to_dict()
        base["_reference_source"] = source_name
        base["_source_rank"] = source_rank

        keys: List[Tuple[str, int]] = []
        for col, match_rank in [("name_primary", 1), ("stage_name", 2), ("birth_name", 3)]:
            key = normalize_name(row.get(col, ""))
            if key:
                keys.append((key, match_rank))
        for alias in split_aliases(row.get("aliases", "")):
            key = normalize_name(alias)
            if key:
                keys.append((key, 4))

        seen = set()
        for key, match_rank in keys:
            if (key, match_rank) in seen:
                continue
            seen.add((key, match_rank))
            record = dict(base)
            record["_match_rank"] = match_rank
            lookup.setdefault(key, []).append(record)

    return {key: best_record(records) for key, records in lookup.items()}


def build_combined_lookup() -> Dict[str, dict]:
    combined: Dict[str, List[dict]] = {}
    for filename, source_name, source_rank in REFERENCE_FILES:
        path = BASE_DIR / filename
        df = pd.read_csv(path)
        file_lookup = lookup_rows_from_reference(df, source_name, source_rank)
        for key, record in file_lookup.items():
            combined.setdefault(key, []).append(record)
    collapsed = {key: best_record(records) for key, records in combined.items()}
    for alias_key, canonical_key in MANUAL_CANONICAL_EQUIVALENTS.items():
        if alias_key not in collapsed and canonical_key in collapsed:
            collapsed[alias_key] = dict(collapsed[canonical_key])
    return collapsed


def build_precomputed_lookup() -> Dict[str, dict]:
    if not PRECOMPUTED_LOOKUP_FILE.exists():
        return {}
    df = pd.read_csv(PRECOMPUTED_LOOKUP_FILE)
    lookup: Dict[str, List[dict]] = {}
    for _, row in df.iterrows():
        key = normalize_name(row.get("target_artist_name", ""))
        if not key:
            continue
        record = row.to_dict()
        record["_reference_source"] = "precomputed_targeted_live"
        record["_source_rank"] = 1
        record["_match_rank"] = 1
        lookup.setdefault(key, []).append(record)
    return {key: best_record(records) for key, records in lookup.items()}


def apply_lookup(df: pd.DataFrame, lookup: Dict[str, dict], *, match_label: str) -> Tuple[pd.DataFrame, int]:
    target_cols = [col for col in ARTIST_COLUMNS if col in df.columns]
    fill_mask = target_missing_mask(df)
    matched_rows = 0

    for idx in df.index[fill_mask]:
        key = normalize_name(df.at[idx, "artist_name"])
        if not key or key not in lookup:
            continue
        record = lookup[key]
        for col in target_cols:
            value = coerce_value_for_column(df, col, record.get(col, ""))
            df.at[idx, col] = value
        notes = normalize_notes(df.at[idx, "notes"]) if "notes" in df.columns else ""
        pieces = [piece for piece in notes.split("|") if piece]
        pieces.append(f"demographic_backfill:{match_label}:{record['_reference_source']}")
        if "notes" in df.columns:
            df.at[idx, "notes"] = "|".join(dict.fromkeys(pieces))
        matched_rows += 1
    return df, matched_rows


def choose_identity_fallback_name(artist_name: object) -> str:
    titles = generate_search_titles("" if pd.isna(artist_name) else str(artist_name))
    for title in titles:
        if "Ã" in title or "Â" in title:
            continue
        if "(" in title and ")" in title:
            continue
        return title
    return normalize_space(maybe_fix_mojibake(artist_name))


def apply_identity_fallback(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    matched_rows = 0
    for idx in df.index[target_missing_mask(df)]:
        fallback_name = choose_identity_fallback_name(df.at[idx, "artist_name"])
        if not fallback_name:
            continue
        df.at[idx, "name_primary"] = fallback_name
        if "stage_name" in df.columns and (pd.isna(df.at[idx, "stage_name"]) or str(df.at[idx, "stage_name"]).strip() == ""):
            df.at[idx, "stage_name"] = fallback_name
        if "source_primary" in df.columns and (pd.isna(df.at[idx, "source_primary"]) or str(df.at[idx, "source_primary"]).strip() == ""):
            df.at[idx, "source_primary"] = "artist_name_identity_fallback"
        notes = normalize_notes(df.at[idx, "notes"]) if "notes" in df.columns else ""
        pieces = [piece for piece in notes.split("|") if piece]
        pieces.append("demographic_backfill:identity_only_fallback")
        if "notes" in df.columns:
            df.at[idx, "notes"] = "|".join(dict.fromkeys(pieces))
        matched_rows += 1
    return df, matched_rows


def targeted_live_lookup(unmatched_names: List[str]) -> pd.DataFrame:
    if not unmatched_names:
        return pd.DataFrame(columns=ARTIST_COLUMNS)

    country_builder = load_country_builder()
    BACKFILL_DIR.mkdir(parents=True, exist_ok=True)
    country_builder.INTERMEDIATE_DIR = BACKFILL_DIR
    country_builder.OUTPUT_DIR = BACKFILL_DIR
    country_builder.FALLBACK_MARKER_PATH = BACKFILL_DIR / "_bundled_fallback_used.txt"
    country_builder.ensure_dirs()

    s = country_builder.session()
    cleaned_names = sorted({name.strip() for name in unmatched_names if name and str(name).strip()})
    candidate_map = {name: generate_search_titles(name) for name in cleaned_names}
    query_titles = []
    for titles in candidate_map.values():
        query_titles.extend(titles)
    query_titles = list(dict.fromkeys(query_titles))

    qid_map = country_builder.wikipedia_titles_to_qids(s, query_titles)
    seed_rows = []
    for original_name in cleaned_names:
        qid = None
        winning_title = original_name
        for title in candidate_map.get(original_name, [original_name]):
            qid = qid_map.get(title)
            if not qid:
                try:
                    qid = country_builder.search_wikidata_qid(s, title)
                except Exception as exc:
                    log(f"Wikidata search failed for {title}: {exc}")
                    qid = None
            if qid:
                winning_title = title
                break
        seed_rows.append(
            {
                "seed_title": winning_title,
                "source_seed": "restricted_final_v6_targeted_artist_name",
                "wikidata_qid": qid or "",
                "target_artist_name": original_name,
            }
        )

    seed_df = pd.DataFrame(seed_rows)
    resolved = seed_df[seed_df["wikidata_qid"].astype(str).str.strip().ne("")].copy()
    if resolved.empty:
        log("No live QIDs resolved for unmatched artist names")
        return pd.DataFrame(columns=ARTIST_COLUMNS)

    qids = resolved["wikidata_qid"].dropna().astype(str).unique().tolist()
    log(f"Resolved {len(qids)} live Wikidata QIDs for targeted backfill")
    detail_df = country_builder.wikidata_details(s, qids, checkpoint_path=BACKFILL_DIR / "targeted_wikidata_details.csv")
    dataset_df = country_builder.prepare_dataset(resolved, detail_df, s)
    dataset_df = country_builder.deduplicate(dataset_df)
    if "target_artist_name" in resolved.columns:
        alias_map = (
            resolved.groupby("wikidata_qid")["target_artist_name"]
            .apply(lambda values: "|".join(dict.fromkeys(str(value).strip() for value in values if str(value).strip())))
            .to_dict()
        )
        dataset_df["target_artist_aliases"] = dataset_df["wikidata_qid"].map(alias_map).fillna("")
        dataset_df["aliases"] = dataset_df.apply(
            lambda row: "|".join(
                dict.fromkeys(
                    piece.strip()
                    for piece in [str(row.get("aliases", "")), str(row.get("target_artist_aliases", ""))]
                    for piece in piece.split("|")
                    if piece.strip()
                )
            ),
            axis=1,
        )
        dataset_df = dataset_df.drop(columns=["target_artist_aliases"])
    return dataset_df


def save_outputs(df: pd.DataFrame) -> None:
    if "notes" in df.columns:
        df["notes"] = df["notes"].map(normalize_notes)
    df.to_csv(TARGET_CSV, index=False, encoding="utf-8")
    stata_df = df.copy()
    for col in stata_df.columns:
        if (
            pd.api.types.is_object_dtype(stata_df[col])
            or pd.api.types.is_string_dtype(stata_df[col])
            or stata_df[col].isna().all()
        ):
            stata_df[col] = stata_df[col].astype("string").fillna("")
    stata_df.to_stata(TARGET_DTA, write_index=False, version=118)
    log(f"Saved {TARGET_CSV.name} and {TARGET_DTA.name}")


def main() -> None:
    df = pd.read_csv(TARGET_CSV)
    df = prepare_target_dtypes(df)
    artist_cols = [col for col in ARTIST_COLUMNS if col in df.columns]
    before_mask = target_missing_mask(df)
    before_missing_rows = int(before_mask.sum())
    before_missing_names = df.loc[before_mask, "artist_name"].drop_duplicates().tolist()
    before_counts = existing_value_count(df.loc[before_mask], artist_cols)

    log(f"Rows with artist_name present and name_primary missing before backfill: {before_missing_rows}")
    log(f"Unique missing artist names before backfill: {len(before_missing_names)}")

    local_lookup = build_combined_lookup()
    df, local_matches = apply_lookup(df, local_lookup, match_label="local")
    log(f"Rows filled from existing country-artist datasets: {local_matches}")

    precomputed_lookup = build_precomputed_lookup()
    if precomputed_lookup:
        df, precomputed_matches = apply_lookup(df, precomputed_lookup, match_label="precomputed_live")
        log(f"Rows filled from bundled precomputed lookup: {precomputed_matches}")
    else:
        log("No bundled precomputed lookup file was available")

    remaining_mask = target_missing_mask(df)
    remaining_names = sorted(df.loc[remaining_mask, "artist_name"].drop_duplicates().tolist())
    log(f"Unique artist names still unmatched after bundled lookup: {len(remaining_names)}")

    try:
        live_df = targeted_live_lookup(remaining_names)
    except Exception as exc:
        log(f"Targeted live lookup failed; proceeding with offline fallback only: {exc}")
        live_df = pd.DataFrame(columns=ARTIST_COLUMNS)
    if not live_df.empty:
        live_lookup = lookup_rows_from_reference(live_df, "targeted_live", 1)
        df, live_matches = apply_lookup(df, live_lookup, match_label="live")
        log(f"Rows filled from targeted live lookup: {live_matches}")
    else:
        log("No targeted live lookup rows were available")

    df, identity_fallback_matches = apply_identity_fallback(df)
    if identity_fallback_matches:
        log(f"Rows filled from identity-only fallback: {identity_fallback_matches}")

    after_mask = target_missing_mask(df)
    after_missing_rows = int(after_mask.sum())
    after_counts = existing_value_count(df.loc[before_mask], artist_cols)

    for col in ["name_primary", "birth_year", "birth_state", "birth_country", "birth_city", "birth_county", "us_macro_region"]:
        if col in before_counts and col in after_counts:
            log(f"{col}: {before_counts[col]} -> {after_counts[col]}")

    log(f"Rows still missing name_primary after backfill: {after_missing_rows}")
    save_outputs(df)


if __name__ == "__main__":
    main()
