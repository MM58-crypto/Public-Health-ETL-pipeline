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

Adult mortality is already included in the prototype. The remaining indicators will be confirmed through source exploration, including their codes, coverage, units, and available breakdowns.

Together, these measures could support questions about changes over time, differences between populations, and the gap between total life expectancy and years lived in good health. These are intended analytical directions; published findings and dashboards will come later.

## The pipeline

The planned architecture follows three layers:

| Layer | Purpose | Status |
| --- | --- | --- |
| **Bronze** | Preserve source responses and ingestion context | Prototype in progress |
| **Silver** | Standardize fields, resolve reference codes, and validate observations | Planned |
| **Gold** | Organize health metrics into analytical fact and dimension tables | Planned |

Python and `requests` currently handle extraction, with JSON used for raw storage. Parquet, DuckDB, and dbt are planned for subsequent storage and transformation work.

## What works today

The current `main.py` retrieves four datasets:

| Dataset | GHO API path | Output file |
| --- | --- | --- |
| Dimension catalogue | `/api/Dimension` | `dimensions.json` |
| Indicator catalogue | `/api/Indicator` | `indicators.json` |
| Adult mortality observations | `/api/WHOSIS_000004` | `adult_mortality.json` |
| Country reference values | `/api/DIMENSION/COUNTRY/DimensionValues` | `country_vals.json` |

Responses are saved under `bronze_layer/` as complete parsed JSON objects, retaining the OData wrapper and source fields. No analytical cleaning is performed during extraction.

The prototype currently overwrites these files on each run. Request validation, timeouts, retries, pagination, and ingestion history remain to be added before it can be treated as a reliable pipeline.

## Why the data needs careful handling

A health observation is more than a country and a year. Records may also distinguish sex, age group, geographic level, or other population categories. Ignoring these distinctions can mix totals with subgroups or produce misleading comparisons.

The pipeline will preserve that context, distinguish numeric estimates from formatted display values, and retain uncertainty bounds where provided. Analytical models will also account for differences in units and reporting periods across indicators.

## Run the current prototype

With Python installed, run these commands from the directory containing `main.py`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install requests
mkdir -p bronze_layer
python main.py
```

An internet connection and an accessible GHO API are required. Review the output files after execution; the current success messages do not establish that every dataset is complete.

## Next milestones

- Confirm the initial indicator selection and profile its coverage and dimensions.
- Add reliable requests and verified pagination.
- Preserve dated ingestion snapshots and useful run metadata.
- Introduce Parquet storage, then build and test silver and gold models.
- Document analytical examples as the pipeline matures.

## Data source

Source: **World Health Organization — Global Health Observatory**.

[GHO OData API documentation](https://www.who.int/data/gho/info/gho-odata-api)

This is an independent learning and portfolio project. Source definitions, units, and methodological notes will guide the interpretation of each indicator.
