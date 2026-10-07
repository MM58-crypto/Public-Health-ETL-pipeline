# Global Health Data Pipeline

**From public health statistics to data that can tell a clearer story.**

How has life expectancy changed across countries? Are longer lives also healthier lives? How do mortality outcomes differ across age groups and populations?

This project explores those questions by building a data pipeline around the World Health Organization's **Global Health Observatory (GHO)** data. The goal is to turn raw API responses into a reproducible analytical foundation for exploring global population health and mortality trends.

For readers interested in public health, the project offers a path toward clearer country comparisons and historical trends. For technical readers, it demonstrates the engineering behind those comparisons: API ingestion, raw data preservation, data quality checks, and dimensional modelling.

> **Status: early development.** The current implementation is a Python ingestion prototype focused on the bronze layer. Silver and gold transformations are planned and have not been implemented.

## What this project aims to explore

The initial focus is **population health and mortality outcomes**, with a proposed selection covering:

- Life expectancy at birth
- Healthy life expectancy at birth
- Adult mortality
- Under-five mortality
- Life expectancy at age 60

Adult mortality, life expectancy at birth, and healthy life expectancy at birth are included in the prototype and have been profiled. Under-five mortality and life expectancy at age 60 remain proposed additions outside the initial silver scope.

Together, these measures could support questions about changes over time, differences between populations, and the gap between total life expectancy and years lived in good health. These are intended analytical directions; published findings and dashboards will come later.

## The pipeline

The planned architecture follows three layers:

| Layer | Purpose | Status |
| --- | --- | --- |
| **Bronze** | Preserve source responses and ingestion context | Prototype in progress |
| **Silver** | Standardize fields, resolve reference codes, and validate observations | Planned |
| **Gold** | Organize health metrics into analytical fact and dimension tables | Planned |

Python and `requests` currently handle extraction, with JSON used for raw storage. Pandas and PyArrow are selected for the planned silver transformations and Parquet outputs. DuckDB and dbt remain optional later choices, not current dependencies.

## What works today

The current `main.py` attempts to retrieve nine datasets:

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

Responses are saved under `bronze_layer/` as complete parsed JSON objects, retaining the OData wrapper and source fields. No analytical cleaning is performed during extraction.

The shared `fetch_data()` function uses a 10-second connection timeout and a 60-second read timeout, checks HTTP status before decoding JSON, and requires an OData object containing a `value` list. These timeouts do not impose an overall run deadline. It creates the output directory automatically and reports timeout, HTTP, network, JSON, response-shape, and filesystem failures separately.

Each dataset returns a success/failure result. The script attempts all nine datasets, logs a summary, and exits with code `0` when all succeed or `1` when any fail. Importing `main.py` does not start ingestion.

The prototype still overwrites files on successful retrieval. Request and response-validation failures leave existing files untouched, but a write failure can leave a partial file. Retries, atomic file replacement, and ingestion history remain future improvements. Pagination and source-completeness verification are deferred; the retrieved data is sufficient for the project's current analytical scope.

## Why the data needs careful handling

A health observation is more than a country and a year. Records may also distinguish sex, age group, geographic level, or other population categories. Ignoring these distinctions can mix totals with subgroups or produce misleading comparisons.

The pipeline will preserve that context, distinguish numeric estimates from formatted display values, and retain uncertainty bounds where provided. Analytical models will also account for differences in units and reporting periods across indicators.

## Silver schema draft

**Design only: no silver transformation code or outputs exist yet.** The first implementation will use Pandas for transformations and PyArrow for Parquet storage. It will read stored bronze JSON, not call the API. The retrieved observations are the accepted project scope; proving source completeness is not a prerequisite.

### Proposed outputs

```text
silver_layer/
  health_observations.parquet
  ref_indicators.parquet
  ref_countries.parquet
  ref_sexes.parquet
  ref_regions.parquet
  ref_income_groups.parquet
```

These are normalized silver tables, not a gold star schema. Each run will rebuild them from the selected bronze inputs rather than append observations from multiple snapshots. Bronze remains unchanged.

### Health observations

Combine `adult_mortality.json`, `life_expectancy_at_birth.json`, and `hale_at_birth.json` into one long-form table. Proposed grain:

```text
indicator_code + location_type + location_code + year + sex_code
```

This grain applies to the audited indicators only: `TimeDimType` is `YEAR`, `Dim1Type` is `SEX`, and the other breakdown/source dimensions are null. Unexpected dimension types or newly populated additional dimensions must fail validation rather than be silently discarded.

Proposed types below are Parquet/Arrow types; nullable integer values should use a nullable Pandas dtype during transformation.

| Column | Type | Nullable | Source / rule |
| --- | --- | --- | --- |
| `indicator_code` | string | No | `IndicatorCode`; join to `ref_indicators` |
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
| `comments` | string | Yes | `Comments`; currently null |
| `source_file` | string | No | Project-relative bronze input path |

Validate `TimeDimensionValue` and both period dates against `year`. Validate source parent-location fields against country references before omitting the redundant parent labels from the observations table. Preserve populated comments. Retain the full original representation in bronze.

`source_date` is not an ingestion timestamp, and a file path does not identify an immutable snapshot while bronze files can be overwritten. Historical ingestion lineage is outside this initial layout.

### Reference tables

All reference code/name columns are non-null strings. Keys must be unique within each table. Every table also includes a non-null string `source_file` identifying its bronze input.

| Table | Columns besides `source_file` | Input and scope |
| --- | --- | --- |
| `ref_indicators` | `indicator_code` (key), `indicator_name`, `unit`, `definition`, `definition_url` | Select the three implemented indicators from `indicators.json`; enrich with explicit WHO-verified definitions, units, and documentation URLs |
| `ref_countries` | `country_code` (key), `country_name`, `region_code` (nullable) | `country_vals.json`; retain all country-reference entries, including those without region parents |
| `ref_sexes` | `sex_code` (key), `sex_name` | `sex_vals.json`; retain all returned categories |
| `ref_regions` | `region_code` (key), `region_name` | `region_vals.json`; retain all returned groupings, not only the six used by current observations |
| `ref_income_groups` | `income_group_code` (key), `income_group_name` | `income_group_vals.json`; retain all returned categories |

The indicator catalogue does not supply units or full definitions. The planned enrichment must explicitly associate `WHOSIS_000001` and `WHOSIS_000002` with years, and `WHOSIS_000004` with the probability of dying between ages 15 and 60 expressed per 1,000. Metadata sources: [life expectancy](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/life-expectancy), [HALE](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/gho-ghe-hale-healthy-life-expectancy), and [adult mortality](https://www.who.int/data/gho/data/indicators/indicator-details/GHO/adult-mortality-rate-%28probability-of-dying-between-15-and-60-years-per-1000-population%29). `dimensions.json` remains supporting bronze metadata; it does not need a separate analytical table in the initial silver layer.

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

For analysis across all three indicators, use matching geography/year/sex keys; the audited shared period is 2000-2021. Do not remove the additional 2022-2023 LE/HALE rows from silver. Matching keys or source dates alone does not establish methodological comparability between estimation releases.

## Run the current prototype

With Python installed, run these commands from the directory containing `main.py`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install requests
python main.py
```

An internet connection and an accessible GHO API are required. Review the output files after execution; the current success messages do not establish that every dataset is complete.

Run the ingestion regression checks with:

```bash
python -B -m unittest discover -s tests -v
```

These checks use temporary directories and simulated HTTP responses; they do not contact WHO or overwrite stored datasets.

## Next milestones

- Review the silver schema draft and implement Pandas/PyArrow transformations over stored bronze inputs.
- Validate silver types, reference joins, observation grain, and input-to-output row accounting.
- Improve bronze persistence and run history separately, without making completeness verification a prerequisite for silver.
- Build gold analytical models after the silver contract is stable.
- Document analytical examples as the pipeline matures.

## Data source

Source: **World Health Organization — Global Health Observatory**.

[GHO OData API documentation](https://www.who.int/data/gho/info/gho-odata-api)

This is an independent learning and portfolio project. Source definitions, units, and methodological notes will guide the interpretation of each indicator.
