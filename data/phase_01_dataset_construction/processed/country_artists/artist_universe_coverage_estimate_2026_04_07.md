# Artist Universe Coverage Estimate

Reference dataset: `artist_universe_country_only.csv`
Country-core count used here: `2333`

## Historical start

For this note, country music is treated as beginning in the commercial-recording era of the early `1920s`, following Britannica's historical summary updated on `January 24, 2026`.

Source:
- Britannica: country music originated in the rural South and West in the early 20th century and began to be commercially recorded in the early 1920s.

## Three denominators

### 1. Canon-restricted denominator

Definition:
- Official narrow canon measured by membership in the Country Music Hall of Fame.

Observed denominator:
- `158` members as of the Medallion Ceremony of `October 19, 2025`.

Coverage:
- `2333 / 158 = 1476.6%`

Interpretation:
- The country-core dataset is much larger than the official canon.
- So it should not be interpreted as a "greatest names only" sample.

### 2. Encyclopedic operational denominator

Definition:
- The broad documented artist universe assembled inside this project, measured by `artist_universe_country_plus_adjacent.csv`.

Observed denominator:
- `4824` artists.

Coverage:
- `2333 / 4824 = 48.4%`

Interpretation:
- The country-core dataset captures about half of the broad documented universe we were able to assemble from structured and semi-structured sources.
- This is the most useful denominator for internal project comparisons.

### 3. Historical-total denominator

Definition:
- A reasoned estimate of all U.S. country artists since the 1920s, including local, regional, forgotten, short-lived, and weakly documented acts that do not reliably appear in structured databases.

Estimated denominator range:
- Low estimate: `12,000`
- High estimate: `25,000`

Coverage range:
- `2333 / 12,000 = 19.4%`
- `2333 / 25,000 = 9.3%`

Interpretation:
- A reasonable summary estimate is that the country-core dataset likely represents about `10%–20%` of the full historical universe of U.S. country artists.
- This is consistent with the fact that many older, regional, short-lived, or poorly documented artists are absent from modern structured sources.

## Recommended reading of the dataset

Best interpretation:
- `2333` is a large and useful country-core universe.
- It is much broader than the official canon.
- It is still clearly smaller than the full historical universe of all country artists since the genre's emergence in the 1920s.

Short takeaway:
- Versus the narrow canon: far larger than `100%`.
- Versus the project's broad documented universe: about `48%`.
- Versus the full historical universe: roughly `10%–20%`.

## Sources

- Britannica country music article: early-20th-century origin and early-1920s recording takeoff.
- Country Music Hall of Fame Medallion Ceremony page: `158` members as of `October 19, 2025`.
- Internal project outputs:
  - `artist_universe_country_only.csv`
  - `artist_universe_country_plus_adjacent.csv`
