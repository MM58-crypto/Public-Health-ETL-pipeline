# Global Health Data Pipeline

**From public health statistics to data that can tell a clearer story.**

How has life expectancy changed across countries? Are longer lives also healthier lives? How do health outcomes differ between populations?

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
<!--
These measures support six analytical questions:

- How have life expectancy, HALE, and adult mortality changed over time?
- How large is the gap between life expectancy and healthy life expectancy?
- How do female and male health outcomes differ?
- How have TB and malaria incidence changed within and across countries?
- Which countries have lower or declining vaccination coverage?
- How do reported measles cases and vaccination coverage vary together?
-->
## Architecture

The architecture follows three layers:

| Layer | Purpose | Script and storage |
| --- | --- | --- |
| **Bronze** | Preserve WHO source responses without analytical cleaning | [`main.py`](main.py) → JSON in `bronze_layer/` |
| **Silver** | Standardize observations, resolve reference codes, and validate data | [`silver.py`](silver.py) → Parquet in `silver_layer/` |
| **Gold** | Organize country-level measures into shared dimensions and fact tables | [`gold.py`](gold.py) → Parquet in `gold_layer/` |

Python and `requests` handle extraction; Pandas and PyArrow handle transformation and storage. Silver and gold are rebuilt from local inputs without contacting WHO.

### Silver: validated observations

Silver keeps three observation tables: `health_observations`, `disease_observations`, and `vaccination_coverage`. Health observations are identified by indicator, location type/code, year, and sex; disease and vaccination observations have no sex breakdown.

Reference tables describe indicators, countries, sexes, regions, and income groups. Silver retains source aggregates separately from countries, along with available uncertainty bounds and source identifiers. It remains the place to inspect detail that is not carried into gold.

### Gold: analytical star schema

Gold contains two fact tables sharing country and year dimensions; health facts also reference a sex dimension. Country/sex codes and year serve as the join keys. Descriptive labels live in dimensions rather than being repeated in facts.

| Table | One row represents | Contents |
| --- | --- | --- |
| `dim_country` | One country | Country name and current region |
| `dim_year` | One observed year | Year key shared by both facts |
| `dim_sex` | One sex category | Sex code and label |
| `fact_country_health` | Country × year × sex | Life expectancy, HALE, adult mortality, and the LE–HALE gap |
| `fact_country_disease_vaccination` | Country × year | TB/malaria incidence, reported measles cases, and DTP3/MCV1/MCV2 coverage |

A repeated country key across different years or sexes is expected—not a duplicate observation. Separate fact grains prevent annual disease and vaccination measures from being repeated across sex-specific health records.
<!--
## Methodology and interpretation

- **Preserve observations, not just complete cases.** Keep all available country/year combinations and distinguish missing estimates from true zeroes. Duplicate observation keys, invalid measures, and unresolved references are rejected rather than silently corrected.
- **Match the comparison population.** Compare the same indicator, year, and sex, and report missing-data exclusions. For a country-level join between the two facts, first select both-sex health observations (`SEX_BTSX`); do not combine those with male/female rows.
- **Respect units and denominators.** LE and HALE are in years; adult mortality is the probability of dying between ages 15 and 60 per 1,000. TB incidence is per 100,000 population per year; malaria incidence is per 1,000 population **at risk** per year. Measles reports are case counts, not incidence rates.
- **Interpret coverage carefully.** Vaccination coverage is a percentage; changes are percentage points. MCV1 and MCV2 can describe different cohorts, so their difference is not automatically a dropout rate. Do not sum rates or percentages, or construct regional estimates without suitable denominator weights.
- **Keep geography explicit.** Gold contains country observations only; WHO regional, global, and income-group aggregates remain in silver. Region labels describe current reference membership, not historical classifications. Country-level income-group assignments and population denominators are not available.
- **Treat derived results as descriptive.** The LE–HALE gap subtracts estimates at matching country/year/sex keys; no uncertainty interval is inferred. Associations between disease reports and vaccination coverage do not establish causality or vaccine effectiveness.

Years vary by indicator and country. The current snapshot's shared period for LE, HALE, and adult mortality is **2000–2021**; retain later observations for questions that do not require all three. Never substitute each country's latest year for a common comparison year. Source completeness and final reporting are not guaranteed, and matching keys alone does not establish comparability between estimation releases.

Indicator definitions, units, and WHO metadata links are available in [`silver_exports/ref_indicators.csv`](silver_exports/ref_indicators.csv). For source estimates, bounds, and identifiers, consult the silver observation tables.
-->
<!--
## Browse the outputs

- **`gold_layer/`**: the five canonical Parquet tables listed above.
- **`gold_exports/`**: matching full-table CSVs for spreadsheet viewing.
- **`gold_exports/samples/EMR/`**: shared dimensions and facts for Egypt (`EGY`), Jordan (`JOR`), Morocco (`MAR`), Saudi Arabia (`SAU`), and Tunisia (`TUN`). Each country subfolder contains its two filtered fact CSVs.
- **`silver_exports/`**: viewing snapshots of the detailed silver tables; running `silver.py` does not refresh these CSVs.

The fixed five-country selection is for inspection, not a representative sample or the whole Eastern Mediterranean region. Country folders are viewing slices, not separate data models. The full gold dataset remains unrestricted.

Gold CSVs refresh only when `--export-dir` is supplied. Missing values appear as blank cells, not zeroes.
-->
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

Custom input/output directories are supported:

```bash
python silver.py --bronze-dir bronze_layer --silver-dir silver_layer
python gold.py --silver-dir silver_layer --gold-dir gold_layer --export-dir gold_exports
```

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
]].head(10).to_string(index=False))
print(f"Countries included: {len(comparison)}")
```

Run ingestion, silver, and gold regression checks with:

```bash
python -B -m unittest discover -s tests -v
```

Tests run in temporary directories without contacting WHO or overwriting project datasets.

If a build or export fails, resolve the reported error and rerun that step before using its outputs; files from an incomplete run may not represent one consistent snapshot.

For implementation detail, see the [silver code walkthrough](SILVER_CODE_WALKTHROUGH.md) and the linked pipeline scripts above.

## Next milestones

- Use the gold fact tables and shared dimensions to answer the documented questions with explicitly selected years and matched comparison samples.
- Develop reports and visualizations without mixing rates, case counts, coverage percentages, or sex-specific populations.
- Improve source persistence and run history.
- Add regional or income-group analytical models only with explicit aggregation rules and suitable population weights or membership data.

## Data source

Source: **World Health Organization — Global Health Observatory**.

[GHO OData API documentation](https://www.who.int/data/gho/info/gho-odata-api)

This is an independent learning and portfolio project. Source definitions, units, and methodological notes will guide the interpretation of each indicator.
