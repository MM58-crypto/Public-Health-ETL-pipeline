# Global Health Data Pipeline

**From public health statistics to data that can tell a clearer story.**

How has life expectancy changed across countries? Are longer lives also healthier lives? How do mortality outcomes differ across age groups and populations?

This project explores those questions by building a data pipeline around the World Health Organization's **Global Health Observatory (GHO)** data. The goal is to turn raw API responses into a reproducible analytical foundation for exploring global population health and mortality trends.

For readers interested in public health, the project offers a path toward clearer country comparisons and historical trends. For technical readers, it demonstrates the engineering behind those comparisons: API ingestion, raw data preservation, data quality checks, and dimensional modelling.

> **Status: analytical preparation.** Bronze ingestion, validated silver Parquet transformations, and a gold star schema with three shared dimensions and two fact tables are implemented. Full CSV exports and a five-country regional sample are available. Published findings and dashboards remain future work.

## What this project aims to explore

The implemented scope covers **population health, disease burden, and vaccination coverage**:

- Life expectancy at birth
- Healthy life expectancy at birth
- Adult mortality
- Tuberculosis and malaria incidence
- Reported measles cases
- DTP3, measles first-dose (MCV1), and measles second-dose (MCV2) vaccination coverage

Under-five mortality and life expectancy at age 60 remain proposed additions outside the implemented silver scope.

Together, these measures could support questions about changes over time, differences between populations, and the gap between total life expectancy and years lived in good health. These are intended analytical directions; published findings and dashboards will come later.

## The pipeline

The architecture follows three layers:

| Layer | Purpose | Status |
| --- | --- | --- |
| **Bronze** | Preserve source responses and ingestion context | Implemented |
| **Silver** | Standardize fields, resolve reference codes, and validate observations | Implemented |
| **Gold** | Organize compatible country-level metrics into shared dimensions and fact tables | Implemented |

Python and `requests` handle extraction, with JSON used for raw storage. Pandas performs silver and gold transformations and PyArrow writes typed Parquet outputs. DuckDB and dbt remain optional later choices, not current dependencies.

## What works today

The current `main.py` attempts to retrieve fifteen datasets:

| Dataset | GHO API path | Output file |
| --- | --- | --- |
| Dimension catalogue | `/api/Dimension` | `dimensions.json` |
| Indicator catalogue | `/api/Indicator` | `indicators.json` |
| Adult mortality observations | `/api/WHOSIS_000004` | `adult_mortality.json` |
| Country reference values | `/api/DIMENSION/COUNTRY/DimensionValues` | `country_vals.json` |
| Life expectancy at birth | `/api/WHOSIS_000001` | `life_expectancy_at_birth.json` |
| Healthy life expectancy at birth | `/api/WHOSIS_000002` | `hale_at_birth.json` |
| Sex reference values | `/api/DIMENSION/SEX/DimensionValues` | `sex_vals.json` |
| Region reference values | `/api/DIMENSION/REGION/DimensionValues` | `region_vals.json` |
| Income-group reference values | `/api/DIMENSION/WORLDBANKINCOMEGROUP/DimensionValues` | `income_group_vals.json` |
| Tuberculosis incidence | `/api/MDG_0000000020` | `tuberculosis_incidence.json` |
| Estimated malaria incidence | `/api/MALARIA_EST_INCIDENCE` | `malaria_incidence.json` |
| Reported measles cases | `/api/WHS3_62` | `measles_reported_cases.json` |
| DTP3 vaccination coverage | `/api/WHS4_100` | `dtp3_coverage.json` |
| Measles first-dose coverage | `/api/WHS8_110` | `measles_mcv1_coverage.json` |
| Measles second-dose coverage | `/api/MCV2` | `measles_mcv2_coverage.json` |

Responses are saved under `bronze_layer/` as complete parsed JSON objects, retaining the OData wrapper and source fields. No analytical cleaning is performed during extraction.

The shared `fetch_data()` function uses a 10-second connection timeout and a 60-second read timeout, checks HTTP status before decoding JSON, and requires an OData object containing a `value` list. These timeouts do not impose an overall run deadline. It creates the output directory automatically and reports timeout, HTTP, network, JSON, response-shape, and filesystem failures separately.

Each dataset returns a success/failure result. The script attempts all fifteen datasets, logs a summary, and exits with code `0` when all succeed or `1` when any fail. Importing `main.py` does not start ingestion.

The prototype still overwrites files on successful retrieval. Request and response-validation failures leave existing files untouched, but a write failure can leave a partial file. Retries, atomic file replacement, and ingestion history remain future improvements. Pagination and source-completeness verification are deferred; the retrieved data is sufficient for the project's current analytical scope.

## Why the data needs careful handling

A health observation is more than a country and a year. Records may also distinguish sex, age group, geographic level, or other population categories. Ignoring these distinctions can mix totals with subgroups or produce misleading comparisons.

Silver preserves geographic context, distinguishes numeric estimates from formatted display values, and retains uncertainty bounds where provided. Reference tables resolve source codes without mixing countries and aggregates. Analytical models must also account for differences in units and estimation releases.

## Silver schema

`silver.py` uses Pandas for transformations and PyArrow for Parquet storage. It reads stored bronze JSON and does not call the API. The retrieved observations are the accepted project scope; proving source completeness is not a prerequisite.

### Outputs

```text
silver_layer/
  health_observations.parquet
  disease_observations.parquet
  vaccination_coverage.parquet
  ref_indicators.parquet
  ref_countries.parquet
  ref_sexes.parquet
  ref_regions.parquet
  ref_income_groups.parquet
```

These are silver observations and reference tables, not a gold star schema. Observations include `indicator_name` as a convenience label from `ref_indicators`; the business key remains unchanged. Each run rebuilds the tables from the selected bronze inputs rather than appending observations from multiple snapshots. Bronze remains unchanged.

### Health observations

`adult_mortality.json`, `life_expectancy_at_birth.json`, and `hale_at_birth.json` are combined into one long-form table with this grain:

```text
indicator_code + location_type + location_code + year + sex_code
```

This grain applies to the audited indicators only: `TimeDimType` is `YEAR`, `Dim1Type` is `SEX`, and the other breakdown/source dimensions are null. Unexpected dimension types or newly populated additional dimensions must fail validation rather than be silently discarded.

Types below are enforced in the Parquet/Arrow schema. Numeric conversions use nullable Pandas dtypes before writing. Invalid conversions fail instead of silently becoming null or truncated integers.

| Column | Type | Nullable | Source / rule |
| --- | --- | --- | --- |
| `indicator_code` | string | No | `IndicatorCode`; join to `ref_indicators` |
| `indicator_name` | string | No | Resolve `indicator_code` through the validated `ref_indicators` table; sourced from bronze `indicators.json` |
| `location_type` | string | No | `SpatialDimType`; retain COUNTRY, REGION, WORLDBANKINCOMEGROUP, and GLOBAL separately |
| `location_code` | string | No | `SpatialDim`; never label every location as a country |
| `year` | int16 | No | `TimeDim`, after validating the YEAR time type |
| `sex_code` | string | No | `Dim1`, after validating `Dim1Type == SEX` |
| `estimate` | float64 | Yes | `NumericValue`; preserve missing estimates, never replace them with zero |
| `lower_bound` | float64 | Yes | `Low`; currently absent for adult mortality |
| `upper_bound` | float64 | Yes | `High`; do not assume a confidence level |
| `display_value` | string | Yes | `Value`, preserved without parsing it into the estimate |
| `period_start` | date32 | No | Local calendar date from `TimeDimensionBegin`, without shifting to UTC before extracting the date |
| `period_end` | date32 | No | Local calendar date from `TimeDimensionEnd`; inclusive source end date |
| `source_observation_id` | int64 | No | `Id`, retained for traceability; not a durable business key |
| `source_date` | string | Yes | Preserve the source ISO-8601 `Date` including its offset; its meaning is not confirmed |

Validate `TimeDimensionValue` and both period dates against `year`. Validate source parent-location fields against country references before omitting the redundant parent labels from the observations table. Observation outputs omit `comments` and `source_file`; source comments remain in the unchanged bronze JSON and are not required or validated by silver.

`source_date` is not an ingestion timestamp. Reference-table source paths do not identify immutable snapshots while bronze files can be overwritten. Historical ingestion lineage is outside this initial layout.

### Disease observations and vaccination coverage

Both new tables use the existing observation fields and types **except `sex_code`**, which is absent rather than fabricated. Their grain is:

```text
indicator_code + location_type + location_code + year
```

`disease_observations` combines TB incidence, estimated malaria incidence, and reported measles cases. `vaccination_coverage` combines DTP3, MCV1, and MCV2 coverage. Indicator codes distinguish the diseases, vaccine doses, and defined target populations; no extra disease/vaccine lookup tables are needed at this stage.

All six audited endpoints have `TimeDimType == YEAR` and null `Dim1Type`/`Dim1`, `Dim2Type`/`Dim2`, `Dim3Type`/`Dim3`, and data-source dimensions. Silver rejects newly populated breakdowns rather than silently dropping them. It applies the same key, reference, period, bounds, and row-preservation checks as the health table. Disease values must be nonnegative, reported case counts integral, and vaccination values between 0 and 100; missing values remain missing.

The tables preserve countries and source aggregates separately. Their measures are not interchangeable:

| Indicator | Unit / target population |
| --- | --- |
| `MDG_0000000020` | New and relapse TB cases per 100,000 population per year |
| `MALARIA_EST_INCIDENCE` | New malaria cases per 1,000 population **at risk** per year |
| `WHS3_62` | Reported measles case count, **not an incidence rate** |
| `WHS4_100` | DTP3 coverage among one-year-olds (%) |
| `WHS8_110` | MCV1 coverage among one-year-olds (%) |
| `MCV2` | MCV2 coverage by the locally recommended age (%) |

Do not sum rates or coverage percentages, or average country coverage into regional coverage without appropriate denominator weights. MCV1 and MCV2 in one year need not describe the same cohort; their difference is not automatically a cohort dropout rate. Cross-country associations are not evidence of vaccine effectiveness.

Definition sources: [TB](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/incidence-of-tuberculosis-%28per-100-000-population-per-year%EF%BC%89), [malaria](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/malaria-incidence-%28per-1-000-population-at-risk%29), [reported measles cases](https://immunizationdata.who.int/global/wiise-detail-page/measles-reported-cases-and-incidence), [DTP3 and MCV2](https://www.who.int/data/gho/indicator-metadata-registry/imr-details/7791), and [immunization coverage](https://www.who.int/data/gho/data/themes/topics/indicator-groups/indicator-group-details/GHO/immunization-coverage-estimates).


### Reference tables

All reference code/name columns are non-null strings. Keys must be unique within each table. Every table also includes a non-null string `source_file` identifying its bronze input.

| Table | Columns besides `source_file` | Input and scope |
| --- | --- | --- |
| `ref_indicators` | `indicator_code` (key), `indicator_name`, `unit`, `definition`, `definition_url` | Select the nine implemented indicators from `indicators.json`; enrich with explicit definitions, units, and WHO documentation URLs |
| `ref_countries` | `country_code` (key), `country_name`, `region_code` (nullable) | `country_vals.json`; retain all country-reference entries, including those without region parents |
| `ref_sexes` | `sex_code` (key), `sex_name` | `sex_vals.json`; retain all returned categories |
| `ref_regions` | `region_code` (key), `region_name` | `region_vals.json`; retain all returned groupings, not only the six used by current observations |
| `ref_income_groups` | `income_group_code` (key), `income_group_name` | `income_group_vals.json`; retain all returned categories |

The indicator catalogue does not supply units or full definitions. `silver.py` enriches the selected catalogue entries using an explicit definition mapping: `WHOSIS_000001` and `WHOSIS_000002` use `years`; `WHOSIS_000004` uses `per_1000`, the probability of dying between ages 15 and 60 expressed per 1,000. Metadata sources: [life expectancy](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/life-expectancy), [HALE](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/gho-ghe-hale-healthy-life-expectancy), and [adult mortality](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/adult-mortality-rate-%28probability-of-dying-between-15-and-60-years-per-1000-population%29). `dimensions.json` remains supporting bronze metadata and is not required by the silver transformation.

Location joins must use both type and code: COUNTRY uses `ref_countries`, REGION uses `ref_regions`, and WORLDBANKINCOMEGROUP uses `ref_income_groups`. Keep `GLOBAL` observations with their original location type/code; an unmatched reference join must not drop them.

Income-group references label existing aggregate observations. They do not assign countries to income groups. None of these new references supplies historical membership dates, so historical country classifications must not be inferred from them.

### Transformation and validation contract

1. Read each bronze `value` list, verify its expected indicator or reference dimension, and preserve every observation.
2. Normalize names and types; reject invalid conversions instead of silently coercing them to null.
3. Validate the supported observation grain and require unique reference keys. Duplicate business keys are errors, not rows to automatically deduplicate.
4. Require all applicable reference joins to resolve with many-to-one cardinality. Keep country and aggregate observations distinct; never sum both-sex estimates with male/female estimates.
5. Check available bounds against their estimates and validate period/year consistency. Preserve legitimate null values and each indicator's available years.
6. Reconcile transformed observation counts with the input files, not an assumed WHO source total. Report validation failures and do not publish a successful silver run when checks fail.
7. Write the validated Parquet tables. Reprocessing unchanged inputs and the same definition mapping must produce equivalent analytical rows.

Required inputs must contain nonempty collections with the expected fields, including fields whose values may legitimately be null. All transformations, reference checks, and Arrow conversions finish before output publication. Output rows are sorted by their keys for reproducibility.

All eight Parquet files are written to a temporary staging directory before any existing file is replaced. Validation or staging-write failures leave previous outputs untouched. Replacements are atomic **per file**, not a transaction across all eight files: an interruption during replacement can leave a mixed set. Rerun the full transformation to rebuild it, and do not treat an unsuccessful run as a valid new silver dataset.

For analysis across the three original health indicators, use matching geography/year/sex keys; the audited shared period is 2000-2021. Do not remove the additional 2022-2023 LE/HALE rows from silver. Joining annual disease or vaccination observations to sex-specific health rows repeats the annual measure across sexes; filter to a compatible health population before comparison. Matching keys or source dates alone does not establish methodological comparability between estimation releases.

## Gold analytical questions and schema

`gold.py` reads validated silver Parquet files offline and builds two fact tables sharing country and year dimensions; health facts also reference a sex dimension. Country and sex codes are natural keys, and year is its own key. No surrogate IDs or historical dimension versions are introduced. Separate fact grains prevent annual disease measures from being repeated across sex-specific health rows.

### Questions supported

| Analytical question | Measures and comparison rule |
| --- | --- |
| How have population health outcomes changed within and across countries? | Life expectancy, HALE, and adult mortality; compare the same indicator, sex, and selected years. Use 2000–2021 when all three are required in the current snapshot. |
| How large is the gap between life expectancy and HALE, and how has it changed? | `life_expectancy_years - healthy_life_expectancy_years` at the same country/year/sex key. This is a difference between summary estimates, not a count of people with illness. |
| How do female and male outcomes differ? | Match country/year and subtract the selected male estimate from the female estimate. Keep both-sex estimates separate; do not average male/female estimates to reconstruct them. |
| How have TB and malaria incidence changed, and which countries have higher values in a selected year? | Compare each indicator separately. TB is per 100,000 population per year; malaria is per 1,000 population at risk per year. Neither is a case count. |
| Which countries have lower or declining vaccination coverage? | Compare the same vaccine/dose and selected year. Absolute coverage change is measured in percentage points, not percent change. Only call it year-on-year when the years are consecutive. |
| How do reported measles cases and vaccination coverage vary together? | Use country/year rows with non-null case and coverage estimates. Counts depend on population size, surveillance, and reporting; this is descriptive, not evidence of causality or vaccine effectiveness. |

Country comparisons must state their selected year and number of countries with non-null estimates. Do not silently compare each country's latest available year. For changes over time, use matched countries and report exclusions. No coverage threshold, country ranking, causal model, or regional average is imposed by the transformation.

### Geography and time scope

- Include only silver observations with `location_type == COUNTRY`, including all country-reference entries with observations; no additional geopolitical filter is applied.
- Keep all observed years and the union of keys across each fact table's indicators. Do not create a full country/year grid or restrict outputs to complete cases.
- Preserve true zeroes and missing estimates. A null gold measure can mean either no source observation or a source observation with a null estimate; inspect silver to distinguish them.
- Keep country and region labels in `dim_country` for filtering. Region membership comes from the current reference snapshot, not historical membership. Region is a country attribute, not a regional measure.
- Leave WHO regional, global, and income-group aggregates in silver. Gold does not recompute them. Country income-group assignment and population denominators are not available.
- Joining the two fact tables requires filtering health to `sex_code == "SEX_BTSX"` first for country-level descriptive comparisons; otherwise annual measures repeat across sexes. This filter alone does not make different target populations or estimation methods comparable.

Observed availability in the current silver snapshot, counting countries with at least one non-null estimate:

| Indicator | Countries | Years |
| --- | ---: | --- |
| Life expectancy and HALE, each | 185 | 2000–2023 |
| Adult mortality | 185 | 2000–2021 |
| TB incidence | 197 | 2000–2024 |
| Malaria incidence | 110 | 2000–2024 |
| Reported measles cases | 214 | 1974–2025 |
| DTP3, MCV1, and MCV2 coverage, each | 195 | 2000–2025 |

These ranges do not imply complete annual coverage for every country. In particular, missing malaria estimates must not be interpreted as zero incidence. The latest source year is not a guarantee of final reporting.

### Output contract

The canonical outputs are five Parquet tables in `gold_layer/`. Dimensions hold descriptive attributes; facts hold only dimension keys and measures. A repeated country key across years or sexes is expected, not a duplicate observation.

| Table | Unique key / grain | Other columns |
| --- | --- | --- |
| `dim_country` | `country_code` | `country_name`, `region_code`, `region_name` |
| `dim_year` | `year` | None; one row per observed year across both facts |
| `dim_sex` | `sex_code` | `sex_name` |
| `fact_country_health` | `country_code + year + sex_code` | `life_expectancy_years`, `healthy_life_expectancy_years`, `adult_mortality_per_1000`, `life_expectancy_hale_gap_years` |
| `fact_country_disease_vaccination` | `country_code + year` | `tb_incidence_per_100000`, `malaria_incidence_per_1000_at_risk`, `measles_reported_cases`, `dtp3_coverage_percent`, `mcv1_coverage_percent`, `mcv2_coverage_percent` |

Dimension keys and fact keys are non-null; `year` is int16 and country/sex codes are strings. Country and sex labels are non-null strings. Region code/name are nullable strings so countries without a known region are retained. All measures are nullable float64, including reported measles counts, which retain silver's numeric representation.

Country and sex dimensions retain all entries from the corresponding silver references, including entries without current facts. The year dimension contains exactly the years observed in either fact. Fact-to-dimension relationships are many-to-one and are enforced by the transformation, not database DDL.

The LE–HALE gap is computed only when both estimates exist at exactly the same country/year/sex key; otherwise it is null. No uncertainty interval is derived for the difference. Rates and percentages remain non-additive: moving them into fact tables does not make summing them meaningful.

Gold measure names map to silver indicators as follows:

| Gold measure | Silver indicator |
| --- | --- |
| `life_expectancy_years` | `WHOSIS_000001` |
| `healthy_life_expectancy_years` | `WHOSIS_000002` |
| `adult_mortality_per_1000` | `WHOSIS_000004` |
| `tb_incidence_per_100000` | `MDG_0000000020` |
| `malaria_incidence_per_1000_at_risk` | `MALARIA_EST_INCIDENCE` |
| `measles_reported_cases` | `WHS3_62` |
| `dtp3_coverage_percent` | `WHS4_100` |
| `mcv1_coverage_percent` | `WHS8_110` |
| `mcv2_coverage_percent` | `MCV2` |

Gold is an estimate-focused analytical projection, not a replacement for silver. Indicator definitions and URLs remain in `silver_layer/ref_indicators.parquet`. Bounds, display values, source IDs, and source dates remain in the relevant silver observation table; recover them using the mapped indicator code, `location_type == COUNTRY`, country/year, and sex for health observations. Matching keys alone does not establish comparability between source estimation releases.

Duplicate dimension or observation keys and unresolved country/sex/region references fail rather than aggregate or silently drop rows. All transformations and typed Arrow conversions finish before publication. All five Parquet files are staged before replacement; replacement is atomic per file, not across the set. An unsuccessful run must not be treated as a new valid gold dataset. Rerun the full build after an interrupted publication.

### Verified gold build

The star-schema build produced **234 countries**, **52 years**, **4 sex categories**, **13,320 health facts**, and **10,720 disease/vaccination facts**. Joining the dimensions to the facts reconstructed every previous gold row, including labels and nulls. All nine source measures were also reconciled against silver, and dimension keys, fact grains, and foreign-key relationships were verified.

The five-country sample contains **360 health facts** and **260 disease/vaccination facts**. Full CSVs, sample membership and dimension relationships, and each country slice were checked against the canonical data. A joined sample query exercised the 2021 both-sex life-expectancy/HALE comparison.

### CSV viewing exports and regional sample

Run `python gold.py --export-dir gold_exports` to rebuild the star schema and publish 20 CSV files:

- Five full-table CSVs directly in `gold_exports/`, with the same names as the Parquet tables.
- Five sample-table CSVs in `gold_exports/samples/EMR/`, using the same schema and containing only **Egypt (`EGY`), Jordan (`JOR`), Morocco (`MAR`), Saudi Arabia (`SAU`), and Tunisia (`TUN`)**.
- Two filtered fact CSVs in each country's folder, for example `gold_exports/samples/EMR/SAU/fact_country_health.csv` and `fact_country_disease_vaccination.csv`.

The fixed selection is reproducible; it is not the entire Eastern Mediterranean region or a statistically representative sample. The sample's country dimension has five rows, and its year/sex dimensions contain only keys referenced by sample facts. Sample exports fail if any selected country is missing from `dim_country` or does not belong to EMR. The full canonical model is never restricted to this selection.

Country folders are viewing slices, not separate dimension tables or independent data models. Use the shared sample dimensions one level above to resolve their keys. Null CSV cells remain blank; numeric zeroes remain zeroes.

Without `--export-dir`, `gold.py` rebuilds only Parquet and leaves CSV snapshots unchanged. Export staging completes before any CSV replacement; replacement is atomic per file, not across all 20 files. Gold publication precedes CSV export, so an export failure can leave a fresh canonical model and older exports; the command exits with code `1`. Rerun with `--export-dir` after resolving the failure.

## Run the pipeline

Python 3.11 was used for verification. From the project root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
python silver.py
python gold.py --export-dir gold_exports
```

`main.py` requires internet access and refreshes all fifteen bronze datasets, overwriting their existing files. If bronze files are already available, skip ingestion. Both `silver.py` and `gold.py` run offline; gold requires a successful silver build, not the CSV viewing snapshots.

Silver requires the nine observation files plus `indicators.json`, `country_vals.json`, `sex_vals.json`, `region_vals.json`, and `income_group_vals.json`. It exits with code `0` only after all eight outputs have been written, or `1` on input, validation, conversion, or filesystem failure. Errors are logged to stderr.

Custom input/output directories are supported:

```bash
python silver.py --bronze-dir bronze_layer --silver-dir silver_layer
python gold.py --silver-dir silver_layer --gold-dir gold_layer --export-dir gold_exports
```

Reference-table input paths recorded in `source_file` are relative to the project root, including when a custom input directory is used. Importing `silver.py` does not run the pipeline.

Gold requires `health_observations`, `disease_observations`, `vaccination_coverage`, `ref_countries`, `ref_regions`, and `ref_sexes` Parquet files from the validated silver build. It exits with code `0` after writing all five canonical tables and any requested exports, or `1` on input, transformation, or filesystem failure. Importing `gold.py` does not run it. No additional dependencies are needed.

For example, select a common year and explicitly require the measures needed for a measles/coverage comparison:

```python
import pandas as pd

annual = pd.read_parquet("gold_layer/fact_country_disease_vaccination.parquet")
countries = pd.read_parquet("gold_layer/dim_country.parquet")
annual = annual.merge(countries, on="country_code", validate="many_to_one")
comparison = annual.loc[annual["year"].eq(2024)].dropna(
    subset=["measles_reported_cases", "mcv1_coverage_percent", "mcv2_coverage_percent"]
)
print(comparison[[
    "country_code", "country_name", "year",
    "measles_reported_cases", "mcv1_coverage_percent", "mcv2_coverage_percent",
]].to_string(index=False))
print(f"Countries included: {len(comparison)}")
```

Run ingestion, silver, and gold regression checks with:

```bash
python -B -m unittest discover -s tests -v
```

The checks use temporary directories; ingestion HTTP responses are simulated, and silver/gold tests exercise real Parquet output. They do not contact WHO or overwrite project datasets. Silver checks cover preservation of nulls and aggregates, local-calendar date handling, reference joins, duplicate keys, invalid dimensions and numbers, measure boundaries, repeatability, and failure-safe staging. Gold checks cover exact country/year/sex matching, dimension joins, null-only rows, zero-versus-missing values, nonoverlapping source years, aggregate exclusion, invalid dimension/observation keys, sample isolation, and failure-safe CSV staging. All 27 tests passed after the star-schema cutover.

### Verified endpoint profile and outputs

On 2026-10-18, the six new selected endpoints returned HTTP 200 and nonempty collections without continuation links. `SDGMALARIA` and `MALARIA005` returned empty collections; the implemented malaria source is `MALARIA_EST_INCIDENCE`, not an automatic fallback.

| New indicator | Rows | Source years |
| --- | ---: | --- |
| TB incidence | 5,177 | 2000–2024 |
| Estimated malaria incidence | 2,855 | 2000–2024 |
| Reported measles cases | 11,062 | 1974–2025 |
| DTP3 coverage | 5,337 | 2000–2025 |
| MCV1 coverage | 5,337 | 2000–2025 |
| MCV2 coverage | 5,337 | 2000–2025 |

These are observed source ranges, not a guarantee that every country has every year or that the latest year is final. The measles response contains 1,113 missing estimates; silver preserves them rather than treating them as zero cases. No duplicate annual business keys were found.

The verified build produced 41,160 health observations, 19,094 disease observations, 16,011 vaccination observations, 9 indicator references, 234 country references, 4 sex references, 43 region references, and 8 income-group references. Existing health observations and the four location/sex reference files were byte-identical after rebuilding. All 20 regression tests passed, and actual source values were reconciled against the new Parquet outputs.

All eight Parquet tables have matching CSV viewing snapshots in `silver_exports/`. They were refreshed and checked against their Parquet inputs; running `silver.py` does **not** refresh them automatically. To refresh explicitly:

```bash
python -c 'from pathlib import Path; import pandas as pd; output = Path("silver_exports"); output.mkdir(exist_ok=True); [pd.read_parquet(path).to_csv(output / (path.stem + ".csv"), index=False) for path in sorted(Path("silver_layer").glob("*.parquet"))]'
```

## Next milestones

- Use the gold fact tables and shared dimensions to answer the documented questions with explicitly selected years and matched comparison samples.
- Develop reports and visualizations without mixing rates, case counts, coverage percentages, or sex-specific populations.
- Improve bronze persistence and run history separately, without making completeness verification a prerequisite for analysis.
- Add regional or income-group analytical models only with explicit aggregation rules and suitable population weights or membership data.

## Data source

Source: **World Health Organization — Global Health Observatory**.

[GHO OData API documentation](https://www.who.int/data/gho/info/gho-odata-api)

This is an independent learning and portfolio project. Source definitions, units, and methodological notes will guide the interpretation of each indicator.
