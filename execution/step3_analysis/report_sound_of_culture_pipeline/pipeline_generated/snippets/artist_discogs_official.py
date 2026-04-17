def enrich_with_discogs_and_itunes(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["birth_year"].isna()
        | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
        | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(
            columns=[
                "name_primary",
                "discogs_birth_year",
                "discogs_birth_city",
                "discogs_birth_state",
                "discogs_birth_country",
                "discogs_url",
                "itunes_artist_url",
            ]
        )

    done = set(checkpoint_df["name_primary"].fillna("").astype(str))
    pending = needs[~needs["name_primary"].isin(done)].copy()
    if pending.empty:
        merged = enriched.merge(checkpoint_df, on="name_primary", how="left")
        return apply_discogs_itunes_merge(merged)

    s = requests.Session()
    new_rows: List[dict] = []
    consecutive_discogs_failures = 0
    consecutive_itunes_failures = 0
    discogs_enabled = True
    itunes_enabled = True
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        name = nonempty(row["name_primary"])
        discogs_birth_year = None
        discogs_birth_city = ""
        discogs_birth_state = ""
        discogs_birth_country = ""
        discogs_url = ""
        itunes_artist_url = ""

        if discogs_enabled:
            try:
                search = s.get(
                    f"{DISCOGS_API}/database/search",
                    params={"q": name, "type": "artist", "per_page": 5, "page": 1},
                    headers=discogs_headers(),
                    timeout=20,
                )
                search.raise_for_status()
                results = search.json().get("results", [])
                best = None
                target_key = normalize_name(name)
                for item in results:
                    title_key = normalize_name(item.get("title", ""))
                    if title_key == target_key:
                        best = item
                        break
                if not best and results:
                    best = results[0]
                if best and best.get("resource_url"):
                    artist_resp = s.get(best["resource_url"], headers=discogs_headers(), timeout=20)
                    artist_resp.raise_for_status()
                    artist_payload = artist_resp.json()
                    parsed = parse_discogs_profile(artist_payload.get("profile", ""))
                    discogs_birth_year = parsed["birth_year"]
                    discogs_birth_city = parsed["birth_city"]
                    discogs_birth_state = parsed["birth_state"]
                    discogs_birth_country = parsed["birth_country"]
                    discogs_url = nonempty(best.get("uri", ""))
                consecutive_discogs_failures = 0
            except Exception as exc:
                builder.log(f"Discogs enrichment failed for {name}: {exc}")
                consecutive_discogs_failures += 1
                if consecutive_discogs_failures >= 10:
                    discogs_enabled = False
                    builder.log("Discogs enrichment disabled for the rest of this run after repeated failures/rate limits")

        if itunes_enabled:
            try:
                itunes = s.get(
                    ITUNES_SEARCH_API,
                    params={"term": name, "entity": "musicArtist", "attribute": "artistTerm", "limit": 5},
                    timeout=20,
                )
                itunes.raise_for_status()
                results = itunes.json().get("results", [])
                best = None
                target_key = normalize_name(name)
                for item in results:
                    title_key = normalize_name(item.get("artistName", ""))
                    if title_key == target_key:
                        best = item
                        break
                if not best and results:
                    best = results[0]
                if best:
                    itunes_artist_url = nonempty(best.get("artistLinkUrl", ""))
                consecutive_itunes_failures = 0
            except Exception as exc:
                builder.log(f"iTunes/Apple Music lookup failed for {name}: {exc}")
                consecutive_itunes_failures += 1
                if consecutive_itunes_failures >= 10:
                    itunes_enabled = False
                    builder.log("iTunes/Apple Music enrichment disabled for the rest of this run after repeated failures/rate limits")

        new_rows.append(
            {
                "name_primary": name,
                "discogs_birth_year": discogs_birth_year,
                "discogs_birth_city": discogs_birth_city,
                "discogs_birth_state": discogs_birth_state,
                "discogs_birth_country": discogs_birth_country,
                "discogs_url": discogs_url,
                "itunes_artist_url": itunes_artist_url,
            }
        )

        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["name_primary"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved Discogs/iTunes checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    merged = enriched.merge(checkpoint_df, on="name_primary", how="left")
    return apply_discogs_itunes_merge(merged)

def enrich_from_official_websites(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["official_website"].fillna("").astype(str).str.strip().ne("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(
            columns=[
                "official_website",
                "official_birth_year",
                "official_birth_city",
                "official_birth_county",
                "official_birth_state",
                "official_birth_country",
                "official_schema_url",
            ]
        )

    done = set(checkpoint_df["official_website"].fillna("").astype(str))
    pending = needs[~needs["official_website"].isin(done)].copy()
    new_rows: List[dict] = []
    for index, (_, row) in enumerate(pending.iterrows(), start=1):
        url = nonempty(row["official_website"])
        parsed = {
            "official_birth_year": None,
            "official_birth_city": "",
            "official_birth_county": "",
            "official_birth_state": "",
            "official_birth_country": "",
            "official_schema_url": "",
        }
        try:
            parsed = parse_official_website_schema(url, is_group_entity(row))
        except Exception as exc:
            builder.log(f"Official website schema enrichment failed for {url}: {exc}")
        new_rows.append({"official_website": url, **parsed})
        if new_rows and (index % 25 == 0 or index == len(pending)):
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["official_website"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved official-website checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

    merged = enriched.merge(checkpoint_df, on="official_website", how="left")
    merged["birth_year"] = merged.apply(
        lambda row: row["birth_year"] if pd.notna(row["birth_year"]) else builder.extract_year_hint(row.get("official_birth_year", "")),
        axis=1,
    )
    for source_col, target_col in [
        ("official_birth_city", "birth_city"),
        ("official_birth_county", "birth_county"),
        ("official_birth_state", "birth_state"),
        ("official_birth_country", "birth_country"),
    ]:
        merged[target_col] = merged.apply(
            lambda row: nonempty(row.get(target_col, "")) or nonempty(row.get(source_col, "")),
            axis=1,
        )
    merged["evidence_urls"] = merged.apply(
        lambda row: "|".join(piece for piece in [nonempty(row.get("evidence_urls", "")), nonempty(row.get("official_schema_url", ""))] if piece),
        axis=1,
    )
    merged["notes"] = merged.apply(
        lambda row: "|".join(
            piece
            for piece in [nonempty(row.get("notes", "")), "birth/origin data enriched from official website schema" if nonempty(row.get("official_schema_url", "")) else ""]
            if piece
        ),
        axis=1,
    )
    merged["birth_state_abbr"] = merged["birth_state"].map(lambda value: builder.US_STATE_ABBR.get(nonempty(value), ""))
    drop_cols = [
        "official_birth_year",
        "official_birth_city",
        "official_birth_county",
        "official_birth_state",
        "official_birth_country",
        "official_schema_url",
    ]
    return merged.drop(columns=[col for col in drop_cols if col in merged.columns], errors="ignore")
