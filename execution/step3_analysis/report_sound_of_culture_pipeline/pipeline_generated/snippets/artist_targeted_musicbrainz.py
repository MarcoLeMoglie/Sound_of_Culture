def targeted_musicbrainz_bio_enrichment(s: requests.Session, df: pd.DataFrame, checkpoint_path: Path) -> pd.DataFrame:
    needs = df[
        df["musicbrainz_mbid"].map(lambda x: not is_blank(x))
        & (
            df["birth_year"].isna()
            | df["birth_city"].map(is_blank)
            | df["birth_state"].map(is_blank)
            | df["birth_country"].map(is_blank)
        )
    ].copy()
    if needs.empty:
        return df

    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(
            columns=[
                "musicbrainz_mbid",
                "mb_birth_date",
                "mb_birth_year",
                "mb_birth_city",
                "mb_birth_county",
                "mb_birth_state",
                "mb_birth_country",
            ]
        )
    for column in [
        "musicbrainz_mbid",
        "mb_birth_date",
        "mb_birth_year",
        "mb_birth_city",
        "mb_birth_county",
        "mb_birth_state",
        "mb_birth_country",
    ]:
        if column not in checkpoint_df.columns:
            checkpoint_df[column] = ""

    done = set(checkpoint_df["musicbrainz_mbid"].dropna().astype(str).tolist())
    pending = needs[~needs["musicbrainz_mbid"].isin(done)].copy()
    log(f"Targeted MusicBrainz bio enrichment pending for {len(pending)} records")

    area_cache: Dict[str, dict] = {}
    new_rows = []
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        mbid = clean_text(row["musicbrainz_mbid"])
        try:
            payload = request_json(s, f"{MUSICBRAINZ_API}/artist/{mbid}", params={"fmt": "json"}, pause=1.05)
            begin_area = payload.get("begin-area", {}) or {}
            begin_area_name = clean_text(begin_area.get("name", ""))
            begin_area_type = clean_text(begin_area.get("type", ""))
            begin_area_id = clean_text(begin_area.get("id", ""))
            area = payload.get("area", {}) or {}
            area_name = clean_text(area.get("name", ""))
            area_state = area_name if area_name in US_STATE_ABBR else ""
            area_country = "United States" if clean_text(payload.get("country", "")) == "US" else ""
            area_ctx = musicbrainz_area_context(s, begin_area_id, area_cache) if begin_area_id else {
                "mb_birth_city": "",
                "mb_birth_county": "",
                "mb_birth_state": "",
                "mb_birth_country": "",
            }
            mb_birth_date = parse_iso_date(clean_text(payload.get("life-span", {}).get("begin", "")))
            mb_birth_year = extract_year_hint(clean_text(payload.get("life-span", {}).get("begin", ""))) or ""
            mb_birth_city = ""
            mb_birth_county = area_ctx["mb_birth_county"]
            mb_birth_state = area_ctx["mb_birth_state"] or area_state
            mb_birth_country = area_ctx["mb_birth_country"] or area_country

            if begin_area_type == "City" and begin_area_name:
                mb_birth_city = begin_area_name
            elif begin_area_name and "County" in begin_area_name:
                mb_birth_county = mb_birth_county or begin_area_name

            new_rows.append(
                {
                    "musicbrainz_mbid": mbid,
                    "mb_birth_date": mb_birth_date,
                    "mb_birth_year": mb_birth_year,
                    "mb_birth_city": mb_birth_city,
                    "mb_birth_county": mb_birth_county,
                    "mb_birth_state": mb_birth_state,
                    "mb_birth_country": mb_birth_country,
                }
            )
        except Exception as exc:
            new_rows.append(
                {
                    "musicbrainz_mbid": mbid,
                    "mb_birth_date": "",
                    "mb_birth_year": "",
                    "mb_birth_city": "",
                    "mb_birth_county": "",
                    "mb_birth_state": "",
                    "mb_birth_country": "",
                }
            )
            log(f"Targeted MusicBrainz enrichment failed for {mbid}: {exc}")

        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["musicbrainz_mbid"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            log(f"Saved MusicBrainz bio checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    enriched = df.merge(checkpoint_df, on="musicbrainz_mbid", how="left")
    enriched["birth_date"] = enriched.apply(
        lambda row: clean_text(row["birth_date"]) or clean_text(row["mb_birth_date"]),
        axis=1,
    )
    enriched["birth_year"] = enriched.apply(
        lambda row: row["birth_year"] if pd.notna(row["birth_year"]) else extract_year_hint(row.get("mb_birth_year", "")),
        axis=1,
    )
    enriched["birth_city"] = enriched.apply(
        lambda row: clean_text(row["birth_city"]) or clean_text(row["mb_birth_city"]),
        axis=1,
    )
    enriched["birth_county"] = enriched.apply(
        lambda row: clean_text(row["birth_county"]) or clean_text(row["mb_birth_county"]),
        axis=1,
    )
    enriched["birth_state"] = enriched.apply(
        lambda row: clean_text(row["birth_state"]) or clean_text(row["mb_birth_state"]),
        axis=1,
    )
    enriched["birth_country"] = enriched.apply(
        lambda row: clean_text(row["birth_country"]) or clean_text(row["mb_birth_country"]),
        axis=1,
    )
    enriched["birth_state_abbr"] = enriched["birth_state"].map(lambda x: US_STATE_ABBR.get(clean_text(x), ""))
    enriched["notes"] = enriched.apply(
        lambda row: pipe_join(
            [
                clean_text(row.get("notes", "")),
                "birth data enriched from MusicBrainz"
                if any(
                    clean_text(row.get(col, ""))
                    for col in ["mb_birth_date", "mb_birth_city", "mb_birth_county", "mb_birth_state", "mb_birth_country"]
                )
                else "",
            ]
        ),
        axis=1,
    )
    return enriched.drop(
        columns=[
            "mb_birth_date",
            "mb_birth_year",
            "mb_birth_city",
            "mb_birth_county",
            "mb_birth_state",
            "mb_birth_country",
        ]
    )
