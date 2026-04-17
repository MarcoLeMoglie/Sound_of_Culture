from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
WAR_PATH = REPO_ROOT / "data/casualties_uswars/combined/war_deaths_individual_level.csv"
SOC_PATH = (
    REPO_ROOT
    / "data/processed_datasets/country_artists/Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv"
)
OUTPUT_DIR = REPO_ROOT / "data/casualties_uswars/combined/merged_sound_of_culture_country_pure_1946plus"


def normalize_text(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    text = re.sub(r"\s+", " ", str(value).strip().upper())
    return text if text else pd.NA


def load_war_data() -> pd.DataFrame:
    war = pd.read_csv(WAR_PATH)
    war = war.reset_index(names="war_row_id")

    war["war_death_year"] = pd.to_datetime(war["death_date"], errors="coerce").dt.year.astype("Int64")
    war["age"] = pd.to_numeric(war["age"], errors="coerce").round().astype("Int64")
    war["war_birth_year_est"] = war["war_death_year"] - war["age"]
    war["merge_birth_city_key"] = war["birth_city"].map(normalize_text)
    war["merge_birth_state_key"] = war["birth_state"].map(normalize_text)

    rename_map = {col: f"war_{col}" for col in war.columns if not col.startswith("war_") and not col.startswith("merge_")}
    war = war.rename(columns=rename_map)
    return war


def load_soc_data() -> pd.DataFrame:
    soc = pd.read_csv(SOC_PATH)
    soc = soc.reset_index(names="soc_row_id")

    soc["release_year"] = pd.to_numeric(soc["release_year"], errors="coerce").round().astype("Int64")
    soc["birth_year"] = pd.to_numeric(soc["birth_year"], errors="coerce").round().astype("Int64")

    is_country_pure = soc["genres_normalized"].astype("string").str.strip().str.lower().eq("country")
    soc = soc[(soc["release_year"] >= 1946) & is_country_pure].copy()

    soc["merge_birth_city_key"] = soc["birth_city"].map(normalize_text)
    soc["merge_birth_state_key"] = soc["birth_state"].map(normalize_text)

    rename_map = {col: f"soc_{col}" for col in soc.columns if not col.startswith("soc_") and not col.startswith("merge_")}
    soc = soc.rename(columns=rename_map)
    return soc


def write_outputs(df: pd.DataFrame, base_name: str) -> tuple[Path, Path]:
    csv_path = OUTPUT_DIR / f"{base_name}.csv"
    dta_path = OUTPUT_DIR / f"{base_name}.dta"

    df.to_csv(csv_path, index=False)

    stata_df = df.copy()
    string_columns = stata_df.select_dtypes(include=["object", "string"]).columns.tolist()
    for column in string_columns:
        stata_df[column] = stata_df[column].where(stata_df[column].isna(), stata_df[column].astype(str))
        stata_df[column] = stata_df[column].astype("string")

    stata_df.to_stata(dta_path, write_index=False, version=118, convert_strl=string_columns)
    return csv_path, dta_path


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    war = load_war_data()
    soc = load_soc_data()

    merge_specs = [
        {
            "name": "war_deaths_x_sound_of_culture_country_pure_1946plus_city_birthyear",
            "left_on": ["war_birth_year_est", "merge_birth_city_key"],
            "right_on": ["soc_birth_year", "merge_birth_city_key"],
            "description": "birth year and birth city",
        },
        {
            "name": "war_deaths_x_sound_of_culture_country_pure_1946plus_city_deathyear_releaseyear",
            "left_on": ["war_death_year", "merge_birth_city_key"],
            "right_on": ["soc_release_year", "merge_birth_city_key"],
            "description": "death year/release year and birth city",
        },
        {
            "name": "war_deaths_x_sound_of_culture_country_pure_1946plus_city_birthyear_deathyear",
            "left_on": ["war_birth_year_est", "war_death_year", "merge_birth_city_key"],
            "right_on": ["soc_birth_year", "soc_release_year", "merge_birth_city_key"],
            "description": "birth year, death year/release year, and birth city",
        },
        {
            "name": "war_deaths_x_sound_of_culture_country_pure_1946plus_state_birthyear",
            "left_on": ["war_birth_year_est", "merge_birth_state_key"],
            "right_on": ["soc_birth_year", "merge_birth_state_key"],
            "description": "birth year and birth state",
        },
        {
            "name": "war_deaths_x_sound_of_culture_country_pure_1946plus_state_deathyear_releaseyear",
            "left_on": ["war_death_year", "merge_birth_state_key"],
            "right_on": ["soc_release_year", "merge_birth_state_key"],
            "description": "death year/release year and birth state",
        },
        {
            "name": "war_deaths_x_sound_of_culture_country_pure_1946plus_state_birthyear_deathyear",
            "left_on": ["war_birth_year_est", "war_death_year", "merge_birth_state_key"],
            "right_on": ["soc_birth_year", "soc_release_year", "merge_birth_state_key"],
            "description": "birth year, death year/release year, and birth state",
        },
    ]

    summary_rows: list[dict[str, object]] = []

    for spec in merge_specs:
        merged = war.merge(
            soc,
            how="inner",
            left_on=spec["left_on"],
            right_on=spec["right_on"],
            sort=False,
        )
        merged["merge_spec"] = spec["description"]

        csv_path, dta_path = write_outputs(merged, spec["name"])

        summary_rows.append(
            {
                "dataset_name": spec["name"],
                "merge_spec": spec["description"],
                "rows": len(merged),
                "columns": len(merged.columns),
                "csv_path": str(csv_path.relative_to(REPO_ROOT)),
                "dta_path": str(dta_path.relative_to(REPO_ROOT)),
            }
        )
        print(f"Wrote {csv_path}")
        print(f"Wrote {dta_path}")
        print(f"Rows: {len(merged):,}")

    summary = pd.DataFrame(summary_rows)
    summary_csv = OUTPUT_DIR / "merge_summary.csv"
    summary_dta = OUTPUT_DIR / "merge_summary.dta"
    summary.to_csv(summary_csv, index=False)
    summary.to_stata(summary_dta, write_index=False, version=118, convert_strl=["dataset_name", "merge_spec", "csv_path", "dta_path"])
    print(f"Wrote {summary_csv}")
    print(f"Wrote {summary_dta}")


if __name__ == "__main__":
    main()
