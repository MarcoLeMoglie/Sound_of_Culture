# Final Dataset Variable Profile - 2026-04-21

Dataset: `data/phase_01_dataset_construction/processed/country_artists/Sound_of_Culture_Country_CountryOnly_Chords_Final_2026_04_08.csv`
Rows: 44,058. Columns: 109.

Missing counts treat true nulls and blank strings as missing. `missing_or_unknown` also counts explicit `Unknown` style values.

## High-Priority Gaps
| variable | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- |
| difficulty | 16721 | 27337 | 62.05 | 27337 | 62.05 | Can partly recover from UG tab text/JSON; otherwise keep missing because imputation would be weak. |
| genre | 43934 | 124 | 0.28 | 124 | 0.28 | Use MusicBrainz/Discogs/Apple/Deezer track or release metadata; separate official genre from UG/fallback genre. |
| main_key | 16246 | 27812 | 63.13 | 27812 | 63.13 | Can partly recover from UG tab text/JSON; otherwise keep missing because imputation would be weak. |
| bpm | 21612 | 22446 | 50.95 | 22446 | 50.95 | Priority: use additional allowed tempo sources; Spotify only if existing app has Audio Features access; otherwise estimate from previews/audio and keep source flags. |
| song_structure | 31366 | 12692 | 28.81 | 12692 | 28.81 | Can partly recover from UG tab text/JSON; otherwise keep missing because imputation would be weak. |
| birth_year | 43572 | 486 | 1.1 | 486 | 1.1 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| birth_city | 42778 | 1280 | 2.91 | 1280 | 2.91 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_county | 23378 | 20680 | 46.94 | 20680 | 46.94 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_state | 43925 | 133 | 0.3 | 133 | 0.3 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_country | 43936 | 122 | 0.28 | 122 | 0.28 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| us_macro_region | 44058 | 0 | 0.0 | 122 | 0.28 | Strict missing is zero, but remaining Unknown rows should be resolved via birth_country/birth_state review; confirmed non-US should remain Non-US. |
| sample_membership | 39081 | 4977 | 11.3 | 4977 | 11.3 | Backfill from the same upstream source family or document as intentionally missing. |
| bpm_sections | 4967 | 39091 | 88.73 | 39091 | 88.73 | Only recoverable where UG JSON contains strumming blocks; otherwise cannot be imputed reliably without audio/score analysis. |
| strumming_patterns | 4973 | 39085 | 88.71 | 39085 | 88.71 | Only recoverable where UG JSON contains strumming blocks; otherwise cannot be imputed reliably without audio/score analysis. |

## Full Variable Table
### Song identity / UG tab identity
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| song_name | Song/tab identity from UG or row construction. | 44058 | 0 | 0.0 | 1 | 0.0 | 36323 | No action needed. |
| artist_name | Song/tab identity from UG or row construction. | 44058 | 0 | 0.0 | 0 | 0.0 | 1794 | No action needed. |
| id | Song/tab identity from UG or row construction. | 44058 | 0 | 0.0 | 0 | 0.0 | 44058 | No action needed. |
| type | Song/tab identity from UG or row construction. | 44058 | 0 | 0.0 | 0 | 0.0 | 1 | No action needed. |
| version | Song/tab identity from UG or row construction. | 43308 | 750 | 1.7 | 750 | 1.7 | 11 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| url_web | Song/tab identity from UG or row construction. | 43308 | 750 | 1.7 | 750 | 1.7 | 43149 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| upload_date | Song/tab identity from UG or row construction. | 43308 | 750 | 1.7 | 750 | 1.7 | 6213 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| upload_year | Song/tab identity from UG or row construction. | 43308 | 750 | 1.7 | 750 | 1.7 | 26 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| num_chord_json_used | Song/tab identity from UG or row construction. | 44058 | 0 | 0.0 | 0 | 0.0 | 3 | No action needed. |

### UG tab quality and setup
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| votes | UG tab metadata on votes, rating, difficulty, tuning or capo. | 44058 | 0 | 0.0 | 0 | 0.0 | 961 | No action needed. |
| rating | UG tab metadata on votes, rating, difficulty, tuning or capo. | 44058 | 0 | 0.0 | 0 | 0.0 | 3760 | No action needed. |
| difficulty | UG tab metadata on votes, rating, difficulty, tuning or capo. | 16721 | 27337 | 62.05 | 27337 | 62.05 | 3 | Can partly recover from UG tab text/JSON; otherwise keep missing because imputation would be weak. |
| tuning | UG tab metadata on votes, rating, difficulty, tuning or capo. | 43308 | 750 | 1.7 | 750 | 1.7 | 17 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| capo | UG tab metadata on votes, rating, difficulty, tuning or capo. | 43308 | 750 | 1.7 | 750 | 1.7 | 12 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |

### UG musical fields and rhythm
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| genre | Musical field parsed from UG JSON or enriched from external sources/previews. | 43934 | 124 | 0.28 | 124 | 0.28 | 67 | Use MusicBrainz/Discogs/Apple/Deezer track or release metadata; separate official genre from UG/fallback genre. |
| main_key | Musical field parsed from UG JSON or enriched from external sources/previews. | 16246 | 27812 | 63.13 | 27812 | 63.13 | 31 | Can partly recover from UG tab text/JSON; otherwise keep missing because imputation would be weak. |
| bpm | Musical field parsed from UG JSON or enriched from external sources/previews. | 21612 | 22446 | 50.95 | 22446 | 50.95 | 196 | Priority: use additional allowed tempo sources; Spotify only if existing app has Audio Features access; otherwise estimate from previews/audio and keep source flags. |
| song_structure | Musical field parsed from UG JSON or enriched from external sources/previews. | 31366 | 12692 | 28.81 | 12692 | 28.81 | 14860 | Can partly recover from UG tab text/JSON; otherwise keep missing because imputation would be weak. |
| has_intro | Musical field parsed from UG JSON or enriched from external sources/previews. | 43308 | 750 | 1.7 | 750 | 1.7 | 2 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| has_verse | Musical field parsed from UG JSON or enriched from external sources/previews. | 43308 | 750 | 1.7 | 750 | 1.7 | 2 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| has_chorus | Musical field parsed from UG JSON or enriched from external sources/previews. | 43308 | 750 | 1.7 | 750 | 1.7 | 2 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| has_bridge | Musical field parsed from UG JSON or enriched from external sources/previews. | 43308 | 750 | 1.7 | 750 | 1.7 | 2 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| has_outro | Musical field parsed from UG JSON or enriched from external sources/previews. | 43308 | 750 | 1.7 | 750 | 1.7 | 2 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| bpm_sections | Musical field parsed from UG JSON or enriched from external sources/previews. | 4967 | 39091 | 88.73 | 39091 | 88.73 | 1445 | Only recoverable where UG JSON contains strumming blocks; otherwise cannot be imputed reliably without audio/score analysis. |
| strumming_patterns | Musical field parsed from UG JSON or enriched from external sources/previews. | 4973 | 39085 | 88.71 | 39085 | 88.71 | 3515 | Only recoverable where UG JSON contains strumming blocks; otherwise cannot be imputed reliably without audio/score analysis. |

### Harmonic/chord indices
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| chord_1 | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 280 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| chord_1_count | Derived chord/harmony index computed from parsed chord sequence. | 43308 | 750 | 1.7 | 750 | 1.7 | 206 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| chord_2 | Derived chord/harmony index computed from parsed chord sequence. | 43273 | 785 | 1.78 | 785 | 1.78 | 396 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| chord_2_count | Derived chord/harmony index computed from parsed chord sequence. | 43308 | 750 | 1.7 | 750 | 1.7 | 177 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| chord_3 | Derived chord/harmony index computed from parsed chord sequence. | 42696 | 1362 | 3.09 | 1362 | 3.09 | 446 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| chord_3_count | Derived chord/harmony index computed from parsed chord sequence. | 43308 | 750 | 1.7 | 750 | 1.7 | 147 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| complexity | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 9140 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| repetition | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 3054 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| melodicness | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 2349 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| energy | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 6931 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| finger_movement | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 32031 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| disruption | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 13595 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| root_stability | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 4642 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| intra_root_variation | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 32 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| harmonic_palette | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 126 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| loop_strength | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 4832 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| structure_variation | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 83 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| playability | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 9026 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |
| harmonic_softness | Derived chord/harmony index computed from parsed chord sequence. | 43294 | 764 | 1.73 | 764 | 1.73 | 4191 | Rebuild/fix rows whose UG JSON was unavailable or malformed; do not impute analytically. |

### Artist identifiers
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| artist_id | Artist identity and external identifier from artist metadata pipeline. | 44058 | 0 | 0.0 | 0 | 0.0 | 1754 | No action needed. |
| name_primary | Artist identity and external identifier from artist metadata pipeline. | 43949 | 109 | 0.25 | 109 | 0.25 | 1562 | Backfill from the same upstream source family or document as intentionally missing. |
| birth_name | Artist identity and external identifier from artist metadata pipeline. | 27496 | 16562 | 37.59 | 16562 | 37.59 | 700 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| stage_name | Artist identity and external identifier from artist metadata pipeline. | 17720 | 26338 | 59.78 | 26338 | 59.78 | 536 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| aliases | Artist identity and external identifier from artist metadata pipeline. | 33111 | 10947 | 24.85 | 10947 | 24.85 | 905 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| wikidata_qid | Artist identity and external identifier from artist metadata pipeline. | 43826 | 232 | 0.53 | 232 | 0.53 | 1543 | Backfill from the same upstream source family or document as intentionally missing. |
| musicbrainz_mbid | Artist identity and external identifier from artist metadata pipeline. | 41122 | 2936 | 6.66 | 2936 | 6.66 | 1351 | Backfill from the same upstream source family or document as intentionally missing. |
| isni | Artist identity and external identifier from artist metadata pipeline. | 33571 | 10487 | 23.8 | 10487 | 23.8 | 857 | Backfill from the same upstream source family or document as intentionally missing. |
| viaf_id | Artist identity and external identifier from artist metadata pipeline. | 35211 | 8847 | 20.08 | 8847 | 20.08 | 988 | Backfill from the same upstream source family or document as intentionally missing. |
| wikipedia_url | Artist identity and external identifier from artist metadata pipeline. | 43742 | 316 | 0.72 | 316 | 0.72 | 1539 | Backfill from the same upstream source family or document as intentionally missing. |

### Artist geography and demography
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| birth_date | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 15036 | 29022 | 65.87 | 29022 | 65.87 | 677 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| birth_year | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 43572 | 486 | 1.1 | 486 | 1.1 | 129 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| birth_place_raw | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 11598 | 32460 | 73.68 | 32460 | 73.68 | 448 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_city | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 42778 | 1280 | 2.91 | 1280 | 2.91 | 951 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_county | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 23378 | 20680 | 46.94 | 20680 | 46.94 | 362 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_state | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 43925 | 133 | 0.3 | 133 | 0.3 | 69 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_state_abbr | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 41723 | 2335 | 5.3 | 2335 | 5.3 | 48 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| birth_country | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 43936 | 122 | 0.28 | 122 | 0.28 | 11 | Use targeted Wikipedia/Wikidata/MusicBrainz/official bio/manual review; non-US should remain Non-US, not Unknown. |
| death_date | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 11398 | 32660 | 74.13 | 32660 | 74.13 | 346 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| death_year | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 11398 | 32660 | 74.13 | 32660 | 74.13 | 72 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| death_place_raw | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 11075 | 32983 | 74.86 | 32983 | 74.86 | 182 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| birth_decade | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 43572 | 486 | 1.1 | 486 | 1.1 | 15 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| us_macro_region | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 44058 | 0 | 0.0 | 122 | 0.28 | 7 | Strict missing is zero, but remaining Unknown rows should be resolved via birth_country/birth_state review; confirmed non-US should remain Non-US. |
| is_deceased | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 44058 | 0 | 0.0 | 0 | 0.0 | 2 | No action needed. |
| age_or_age_at_death | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 10830 | 33228 | 75.42 | 33228 | 75.42 | 91 | Backfill from Wikidata/Wikipedia/MusicBrainz; age needs both birth and death/current reference date. |
| is_us_born | Artist geography/demography from Wikidata, Wikipedia/MusicBrainz, and manual backfills. | 44058 | 0 | 0.0 | 0 | 0.0 | 2 | No action needed. |

### Artist descriptors and sample flags
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| citizenship | Artist descriptor, sample inclusion, or country-universe classification. | 32803 | 11255 | 25.55 | 11255 | 25.55 | 12 | Backfill from the same upstream source family or document as intentionally missing. |
| occupations | Artist descriptor, sample inclusion, or country-universe classification. | 40690 | 3368 | 7.64 | 3368 | 7.64 | 623 | Backfill from the same upstream source family or document as intentionally missing. |
| genres_raw | Artist descriptor, sample inclusion, or country-universe classification. | 42953 | 1105 | 2.51 | 1105 | 2.51 | 580 | Backfill from the same upstream source family or document as intentionally missing. |
| genres_normalized | Artist descriptor, sample inclusion, or country-universe classification. | 40661 | 3397 | 7.71 | 3397 | 7.71 | 23 | Backfill from the same upstream source family or document as intentionally missing. |
| country_relevance_score | Artist descriptor, sample inclusion, or country-universe classification. | 43839 | 219 | 0.5 | 219 | 0.5 | 21 | Backfill from the same upstream source family or document as intentionally missing. |
| instruments | Artist descriptor, sample inclusion, or country-universe classification. | 30990 | 13068 | 29.66 | 13068 | 29.66 | 60 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| member_of | Artist descriptor, sample inclusion, or country-universe classification. | 10816 | 33242 | 75.45 | 33242 | 75.45 | 110 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| record_labels | Artist descriptor, sample inclusion, or country-universe classification. | 33215 | 10843 | 24.61 | 10843 | 24.61 | 481 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| awards | Artist descriptor, sample inclusion, or country-universe classification. | 22143 | 21915 | 49.74 | 21915 | 49.74 | 184 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| official_website | Artist descriptor, sample inclusion, or country-universe classification. | 33045 | 11013 | 25.0 | 11013 | 25.0 | 746 | Optional enrichment from Wikidata/MusicBrainz/official sites; not critical for core culture measure. |
| is_solo_person | Artist descriptor, sample inclusion, or country-universe classification. | 44058 | 0 | 0.0 | 0 | 0.0 | 2 | No action needed. |
| is_country_core | Artist descriptor, sample inclusion, or country-universe classification. | 43828 | 230 | 0.52 | 230 | 0.52 | 2 | Backfill from the same upstream source family or document as intentionally missing. |
| is_country_broad | Artist descriptor, sample inclusion, or country-universe classification. | 43828 | 230 | 0.52 | 230 | 0.52 | 2 | Backfill from the same upstream source family or document as intentionally missing. |
| flag_restricted_sample | Artist descriptor, sample inclusion, or country-universe classification. | 43828 | 230 | 0.52 | 230 | 0.52 | 2 | Backfill from the same upstream source family or document as intentionally missing. |
| flag_expanded_sample | Artist descriptor, sample inclusion, or country-universe classification. | 43828 | 230 | 0.52 | 230 | 0.52 | 2 | Backfill from the same upstream source family or document as intentionally missing. |
| sample_membership | Artist descriptor, sample inclusion, or country-universe classification. | 39081 | 4977 | 11.3 | 4977 | 11.3 | 2 | Backfill from the same upstream source family or document as intentionally missing. |
| inclusion_reason | Artist descriptor, sample inclusion, or country-universe classification. | 40971 | 3087 | 7.01 | 3087 | 7.01 | 3 | Backfill from the same upstream source family or document as intentionally missing. |
| exclusion_reason | Artist descriptor, sample inclusion, or country-universe classification. | 4747 | 39311 | 89.23 | 39311 | 89.23 | 4 | Missing is mostly structural: only populated for excluded/review/backfill cases. |

### Provenance and confidence
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| source_primary | Source/confidence/audit field documenting artist metadata provenance. | 43839 | 219 | 0.5 | 219 | 0.5 | 3 | Backfill from the same upstream source family or document as intentionally missing. |
| source_secondary | Source/confidence/audit field documenting artist metadata provenance. | 0 | 44058 | 100.0 | 44058 | 100.0 | 0 | Currently unused/empty carrier; either populate deliberately or drop from final public schema. |
| source_seed | Source/confidence/audit field documenting artist metadata provenance. | 43828 | 230 | 0.52 | 230 | 0.52 | 27 | Backfill from the same upstream source family or document as intentionally missing. |
| evidence_urls | Source/confidence/audit field documenting artist metadata provenance. | 43759 | 299 | 0.68 | 299 | 0.68 | 1596 | Backfill from the same upstream source family or document as intentionally missing. |
| source_count | Source/confidence/audit field documenting artist metadata provenance. | 43839 | 219 | 0.5 | 219 | 0.5 | 4 | Backfill from the same upstream source family or document as intentionally missing. |
| birth_date_confidence | Source/confidence/audit field documenting artist metadata provenance. | 43828 | 230 | 0.52 | 230 | 0.52 | 3 | Backfill from the same upstream source family or document as intentionally missing. |
| birth_place_confidence | Source/confidence/audit field documenting artist metadata provenance. | 43828 | 230 | 0.52 | 230 | 0.52 | 3 | Backfill from the same upstream source family or document as intentionally missing. |
| genre_confidence | Source/confidence/audit field documenting artist metadata provenance. | 43828 | 230 | 0.52 | 230 | 0.52 | 3 | Backfill from the same upstream source family or document as intentionally missing. |
| manual_review_needed | Source/confidence/audit field documenting artist metadata provenance. | 43839 | 219 | 0.5 | 219 | 0.5 | 2 | Backfill from the same upstream source family or document as intentionally missing. |
| notes | Source/confidence/audit field documenting artist metadata provenance. | 11786 | 32272 | 73.25 | 32272 | 73.25 | 147 | Missing is mostly structural: only populated for excluded/review/backfill cases. |

### Billboard country chart supplement
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| billboard_country_year_end_flag | Billboard country year-end supplement marker and chart position/year. | 44058 | 0 | 0.0 | 0 | 0.0 | 2 | No action needed. |
| billboard_country_year_end_year | Billboard country year-end supplement marker and chart position/year. | 1428 | 42630 | 96.76 | 42630 | 96.76 | 38 | Missing means not in Billboard country year-end supplement; no fill needed when flag=0. |
| billboard_country_year_end_pos | Billboard country year-end supplement marker and chart position/year. | 1428 | 42630 | 96.76 | 42630 | 96.76 | 48 | Missing means not in Billboard country year-end supplement; no fill needed when flag=0. |

### Genre/BPM provenance fields
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| wikipedia_categories | Final metadata provenance field for genre/BPM or unused legacy carrier. | 0 | 44058 | 100.0 | 44058 | 100.0 | 0 | Currently unused/empty carrier; either populate deliberately or drop from final public schema. |
| musicbrainz_artist_type | Final metadata provenance field for genre/BPM or unused legacy carrier. | 0 | 44058 | 100.0 | 44058 | 100.0 | 0 | Currently unused/empty carrier; either populate deliberately or drop from final public schema. |
| genre_ug_original | Final metadata provenance field for genre/BPM or unused legacy carrier. | 41660 | 2398 | 5.44 | 2398 | 5.44 | 16 | Backfill from the same upstream source family or document as intentionally missing. |
| genre_official_raw | Final metadata provenance field for genre/BPM or unused legacy carrier. | 18860 | 25198 | 57.19 | 25198 | 57.19 | 109 | Use MusicBrainz/Discogs/Apple/Deezer track or release metadata; separate official genre from UG/fallback genre. |
| genre_source | Final metadata provenance field for genre/BPM or unused legacy carrier. | 43934 | 124 | 0.28 | 124 | 0.28 | 5 | Use MusicBrainz/Discogs/Apple/Deezer track or release metadata; separate official genre from UG/fallback genre. |
| genre_is_official | Final metadata provenance field for genre/BPM or unused legacy carrier. | 44058 | 0 | 0.0 | 0 | 0.0 | 2 | No action needed. |
| bpm_source | Final metadata provenance field for genre/BPM or unused legacy carrier. | 21612 | 22446 | 50.95 | 22446 | 50.95 | 3 | Backfill from the same upstream source family or document as intentionally missing. |

### Other
| variable | what_it_is | non_missing | missing | missing_pct | missing_or_unknown | missing_or_unknown_pct | unique_non_missing | fill_strategy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| release_year | Residual project field. | 41161 | 2897 | 6.58 | 2897 | 6.58 | 118 | Backfill from the same upstream source family or document as intentionally missing. |

## Source Distributions
### genre_source
| genre_source | rows |
| --- | --- |
| ug_selected | 24238 |
| apple_music_track | 18382 |
| artist_metadata_fallback | 836 |
| deezer_album_genre | 329 |
| discogs_release | 149 |
| <missing> | 124 |

### genre_is_official
| genre_is_official | rows |
| --- | --- |
| 0 | 25198 |
| 1 | 18860 |

### bpm_source
| bpm_source | rows |
| --- | --- |
| <missing> | 22446 |
| apple_preview_estimated | 15418 |
| ug_selected | 6137 |
| deezer_preview_estimated | 57 |

### birth_place_confidence
| birth_place_confidence | rows |
| --- | --- |
| high | 36068 |
| medium | 4658 |
| low | 3102 |
| <missing> | 230 |

### birth_date_confidence
| birth_date_confidence | rows |
| --- | --- |
| high | 42494 |
| low | 1197 |
| <missing> | 230 |
| medium | 137 |

### genre_confidence
| genre_confidence | rows |
| --- | --- |
| high | 34869 |
| medium | 5949 |
| low | 3010 |
| <missing> | 230 |

### sample_membership
| sample_membership | rows |
| --- | --- |
| restricted | 36906 |
| <missing> | 4977 |
| expanded_only | 2175 |

### source_primary
| source_primary | rows |
| --- | --- |
| Wikidata | 43746 |
| <missing> | 219 |
| Billboard country year-end chart | 82 |
| artist_name_identity_fallback | 11 |

### us_macro_region
| us_macro_region | rows |
| --- | --- |
| South | 23896 |
| Appalachia | 6177 |
| Midwest | 4852 |
| West | 4099 |
| Non-US | 2768 |
| Northeast | 2144 |
| Unknown | 122 |

### birth_country
| birth_country | rows |
| --- | --- |
| United States | 41168 |
| Canada | 1494 |
| United Kingdom | 486 |
| Australia | 377 |
| Japan | 197 |
| New Zealand | 177 |
| <missing> | 122 |
| Sweden | 20 |
| Netherlands | 7 |
| India | 6 |
| Ireland | 3 |
| Cuba | 1 |
