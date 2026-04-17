def prepare_dataset(seed_df: pd.DataFrame, detail_df: pd.DataFrame, s: requests.Session) -> pd.DataFrame:
    seed_summary = (
        seed_df.groupby("wikidata_qid", dropna=False)
        .agg(
            source_seed=("source_seed", lambda items: pipe_join(items)),
            seed_title=("seed_title", "first"),
        )
        .reset_index()
    )
    merged = detail_df.merge(seed_summary, on="wikidata_qid", how="left")
    merged["source_seed"] = merged["source_seed"].fillna("Wikidata genre query")
    merged = fill_missing_wikipedia_urls_from_sitelinks(s, merged)
    merged = enrich_from_wikipedia(s, merged, INTERMEDIATE_DIR / "country_artists_wikipedia_enrichment.csv")
    merged["name_primary"] = merged.apply(choose_name_primary, axis=1)
    merged["birth_date"] = merged["birth_date"].map(parse_iso_date)
    merged["death_date"] = merged["death_date"].map(parse_iso_date)
    merged["birth_date"] = merged["birth_date"].fillna("").astype(str).replace({"NaT": "", "nan": ""})
    merged["death_date"] = merged["death_date"].fillna("").astype(str).replace({"NaT": "", "nan": ""})
    merged["birth_year"] = merged["birth_date"].str.slice(0, 4).map(maybe_int)

    geo_df = merged.apply(resolve_birth_geography, axis=1, result_type="expand")
    merged = pd.concat([merged, geo_df], axis=1)
    merged = enrich_from_wikipedia_lead(s, merged, INTERMEDIATE_DIR / "country_artists_wikipedia_lead.csv")
    merged["birth_date"] = merged["birth_date"].map(parse_iso_date)
    merged["birth_date"] = merged["birth_date"].fillna("").astype(str).replace({"NaT": "", "nan": ""})
    merged["birth_year_hint"] = merged.apply(
        lambda row: maybe_int(clean_text(row["birth_date"])[:4])
        or row.get("birth_year")
        or extract_year_hint(row.get("wp_birth_year", ""))
        or extract_birth_year_from_categories(row.get("wp_categories", "")),
        axis=1,
    )
    geo_df = merged.apply(resolve_birth_geography, axis=1, result_type="expand")
    merged = merged.drop(columns=["birth_city", "birth_county", "birth_state", "birth_state_abbr", "birth_country"], errors="ignore")
    merged = pd.concat([merged, geo_df], axis=1)
    geo_fallback_df = merged.apply(apply_category_location_fallback, axis=1, result_type="expand")
    for col in ["birth_city", "birth_county", "birth_state", "birth_state_abbr", "birth_country"]:
        merged[col] = geo_fallback_df[col]

    merged["birth_year"] = merged.apply(
        lambda row: maybe_int(clean_text(row["birth_date"])[:4]) if clean_text(row["birth_date"]) else row["birth_year_hint"],
        axis=1,
    )
    merged["death_year"] = merged["death_date"].str.slice(0, 4).map(maybe_int)
    merged["birth_decade"] = merged["birth_year"].map(lambda x: int(math.floor(x / 10) * 10) if pd.notna(x) else None)
    merged["us_macro_region"] = merged["birth_state"].map(lambda x: STATE_TO_REGION.get(x, "Unknown"))
    merged["is_deceased"] = merged["death_date"].map(lambda x: 1 if x else 0)

    today = date.today()

    def calc_age(row: pd.Series) -> Optional[int]:
        if not row["birth_date"]:
            return None
        try:
            born = datetime.fromisoformat(row["birth_date"]).date()
            ended = datetime.fromisoformat(row["death_date"]).date() if row["death_date"] else today
            return ended.year - born.year - ((ended.month, ended.day) < (born.month, born.day))
        except Exception:
            return None

    merged["notes"] = ""
    merged = targeted_musicbrainz_bio_enrichment(s, merged, INTERMEDIATE_DIR / "country_artists_musicbrainz_bio.csv")
    merged["birth_year"] = merged.apply(
        lambda row: maybe_int(clean_text(row["birth_date"])[:4]) if clean_text(row["birth_date"]) else row["birth_year"],
        axis=1,
    )
    merged["age_or_age_at_death"] = merged.apply(calc_age, axis=1)
    merged["genres_normalized"] = merged["genres_raw"].map(normalize_country_genres)
    merged["instruments"] = merged["instruments"].map(lambda x: normalize_values(str(x), INSTRUMENT_NORMALIZATION))
    merged["country_relevance_score"] = merged.apply(
        lambda row: score_country_relevance(
            row["genres_raw"],
            row["occupations"],
            row["source_seed"],
            row["awards"],
            row.get("wp_categories", ""),
        ),
        axis=1,
    )
    merged["birth_date_confidence"] = merged.apply(
        lambda row: "high" if row["birth_date"] else ("medium" if pd.notna(row["birth_year"]) else "low"),
        axis=1,
    )
    merged["birth_place_confidence"] = merged.apply(
        lambda row: "high" if row["birth_state"] and row["birth_place_raw"] else ("medium" if row["birth_state"] else "low"),
        axis=1,
    )
    merged["genre_confidence"] = merged["country_relevance_score"].map(
        lambda x: "high" if x >= 0.8 else ("medium" if x >= 0.5 else "low")
    )
    merged["manual_review_needed"] = merged.apply(
        lambda row: 1
        if (
            not row["wikidata_qid"]
            or pd.isna(row["birth_year"])
            or (row["birth_country"] == "United States" and not row["birth_state"])
            or (row["genre_confidence"] == "low" and not has_strong_country_seed_evidence(row["source_seed"], row.get("wp_categories", "")))
        )
        else 0,
        axis=1,
    )
    merged["source_primary"] = "Wikidata"
    merged["source_secondary"] = ""
    merged["evidence_urls"] = merged.apply(
        lambda row: pipe_join(
            [
                f"https://www.wikidata.org/wiki/{row['wikidata_qid']}" if row["wikidata_qid"] else "",
                row["wikipedia_url"],
                f"https://musicbrainz.org/artist/{row['musicbrainz_mbid']}" if clean_text(row["musicbrainz_mbid"]) else "",
            ]
        ),
        axis=1,
    )
    merged["source_count"] = merged["evidence_urls"].map(lambda x: len([piece for piece in str(x).split("|") if piece]))
    merged["notes"] = merged.apply(
        lambda row: pipe_join(
            [
                clean_text(row.get("notes", "")),
                "birth_state unresolved from structured place hierarchy"
                if row["birth_country"] == "United States" and not row["birth_state"]
                else "",
                "birth geography inferred from Wikipedia categories"
                if row["birth_country"] == "United States" and row["birth_state"] and not row["birth_place_raw"]
                else "",
                "low country confidence" if row["genre_confidence"] == "low" else "",
            ]
        ),
        axis=1,
    )
    sample_df = merged.apply(infer_samples, axis=1, result_type="expand")
    merged = pd.concat([merged, sample_df], axis=1)
    merged["artist_id"] = [f"artist_{index:06d}" for index in range(1, len(merged) + 1)]
    merged = merged.drop(columns=["birth_year_hint"], errors="ignore")
    if "seed_title" in merged.columns:
        merged = merged.drop(columns=["seed_title"])
    return merged
