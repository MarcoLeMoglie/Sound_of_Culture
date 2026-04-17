def resolve_missing_qids_from_names(df: pd.DataFrame, checkpoint_name: str) -> pd.DataFrame:
    enriched = df.copy()
    needs = enriched[
        enriched["wikidata_qid"].fillna("").astype(str).str.strip().eq("")
        & (
            enriched["birth_year"].isna()
            | enriched["birth_state"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_country"].fillna("").astype(str).str.strip().eq("")
            | enriched["birth_city"].fillna("").astype(str).str.strip().eq("")
        )
    ].copy()
    if needs.empty:
        return enriched

    checkpoint_path = INTERMEDIATE_DIR / checkpoint_name
    if checkpoint_path.exists():
        checkpoint_df = pd.read_csv(checkpoint_path)
    else:
        checkpoint_df = pd.DataFrame(columns=["name_primary", "resolved_wikidata_qid"])

    done = set(checkpoint_df["name_primary"].fillna("").astype(str))
    pending_names = [name for name in needs["name_primary"].fillna("").astype(str).tolist() if name and name not in done]
    if pending_names:
        s = builder.session()
        new_rows: List[dict] = []
        for batch_start in range(0, len(pending_names), 20):
            batch = pending_names[batch_start : batch_start + 20]
            try:
                batch_map = builder.wikipedia_titles_to_qids(s, batch)
            except Exception as exc:
                builder.log(f"Batch Wikipedia title-to-QID lookup failed for {len(batch)} names: {exc}")
                batch_map = {name: "" for name in batch}
            for name in batch:
                new_rows.append({"name_primary": name, "resolved_wikidata_qid": batch_map.get(name, "") or ""})
            checkpoint_df = pd.concat([checkpoint_df, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint_df = checkpoint_df.drop_duplicates(subset=["name_primary"], keep="last")
            checkpoint_df.to_csv(checkpoint_path, index=False)
            builder.log(f"Saved name-to-QID checkpoint at {len(checkpoint_df)} rows")
            new_rows = []

        unresolved_mask = checkpoint_df["resolved_wikidata_qid"].fillna("").astype(str).str.strip().eq("")
        unresolved_names = checkpoint_df.loc[unresolved_mask, "name_primary"].dropna().astype(str).tolist()
        consecutive_search_failures = 0
        for index, name in enumerate(unresolved_names, start=1):
            try:
                qid = builder.search_wikidata_qid(s, name) or ""
                consecutive_search_failures = 0
            except Exception as exc:
                builder.log(f"Wikidata title search failed for {name}: {exc}")
                qid = ""
                consecutive_search_failures += 1
            checkpoint_df.loc[checkpoint_df["name_primary"].eq(name), "resolved_wikidata_qid"] = qid
            if index % 25 == 0 or index == len(unresolved_names):
                checkpoint_df.to_csv(checkpoint_path, index=False)
                builder.log(f"Updated name-to-QID checkpoint at {len(checkpoint_df)} rows")
            if consecutive_search_failures >= 10:
                builder.log("Stopping single-title QID search early after repeated failures/rate limits")
                break

    merged = enriched.merge(checkpoint_df, on="name_primary", how="left")
    merged["wikidata_qid"] = merged.apply(
        lambda row: nonempty(row.get("wikidata_qid", "")) or nonempty(row.get("resolved_wikidata_qid", "")),
        axis=1,
    )
    return merged.drop(columns=["resolved_wikidata_qid"])
