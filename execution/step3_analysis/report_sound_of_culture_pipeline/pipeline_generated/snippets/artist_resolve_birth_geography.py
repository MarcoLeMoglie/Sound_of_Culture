def resolve_birth_geography(row: pd.Series) -> dict:
    birth_admin_labels = "" if pd.isna(row.get("birth_admin_labels", "")) else str(row.get("birth_admin_labels", ""))
    labels = [piece.strip() for piece in birth_admin_labels.split("|") if piece.strip()]
    raw = str(row.get("birth_place_raw", "")).strip()
    birth_state = ""
    birth_country = ""
    birth_city = ""
    birth_county = ""

    if raw:
        birth_city = raw.split(",")[0].strip()
        if COUNTY_PATTERN.search(raw):
            birth_county = raw.split(",")[0].strip()

    for label in [raw] + labels:
        for state in US_STATE_ABBR:
            if re.search(rf"\b{re.escape(state)}\b", label):
                birth_state = state
                break
        if birth_state:
            break

    raw_country = str(row.get("birth_country_raw", "")).strip()
    combined_geo = "|".join([raw, birth_admin_labels, raw_country])
    if "United States" in combined_geo or "USA" in combined_geo or birth_state:
        birth_country = "United States"
    elif raw_country:
        birth_country = raw_country

    for label in labels:
        if COUNTY_PATTERN.search(label):
            birth_county = label
            break

    return {
        "birth_city": birth_city if birth_city and birth_city != birth_county else birth_city,
        "birth_county": birth_county,
        "birth_state": birth_state,
        "birth_state_abbr": US_STATE_ABBR.get(birth_state, ""),
        "birth_country": birth_country,
    }


def apply_category_location_fallback(row: pd.Series) -> dict:
    inferred = infer_us_location_from_categories(row.get("wp_categories", ""), row.get("source_seed", ""))
    birth_city = clean_text(row.get("birth_city", "")) or clean_text(inferred["birth_city_from_categories"])
    birth_county = clean_text(row.get("birth_county", "")) or clean_text(inferred["birth_county_from_categories"])
    birth_state = clean_text(row.get("birth_state", "")) or clean_text(inferred["birth_state_from_categories"])
    birth_country = clean_text(row.get("birth_country", "")) or clean_text(inferred["birth_country_from_categories"])
    if not birth_country and birth_state:
        birth_country = "United States"
    return {
        "birth_city": birth_city,
        "birth_county": birth_county,
        "birth_state": birth_state,
        "birth_state_abbr": US_STATE_ABBR.get(birth_state, ""),
        "birth_country": birth_country,
    }
