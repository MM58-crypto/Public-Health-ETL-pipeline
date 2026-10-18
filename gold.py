"""Build a country-level gold star schema from validated silver Parquet tables."""

import argparse
import logging
from pathlib import Path
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from silver import arrow_table, members, require, unique


logger = logging.getLogger(__name__)
HEALTH_METRICS = {
    "WHOSIS_000001": "life_expectancy_years",
    "WHOSIS_000002": "healthy_life_expectancy_years",
    "WHOSIS_000004": "adult_mortality_per_1000",
}
DISEASE_METRICS = {
    "MDG_0000000020": "tb_incidence_per_100000",
    "MALARIA_EST_INCIDENCE": "malaria_incidence_per_1000_at_risk",
    "WHS3_62": "measles_reported_cases",
}
VACCINATION_METRICS = {
    "WHS4_100": "dtp3_coverage_percent",
    "WHS8_110": "mcv1_coverage_percent",
    "MCV2": "mcv2_coverage_percent",
}
HEALTH_GRAIN = ["country_code", "year", "sex_code"]
ANNUAL_GRAIN = ["country_code", "year"]
TABLE_KEYS = {
    "dim_country": ["country_code"],
    "dim_year": ["year"],
    "dim_sex": ["sex_code"],
    "fact_country_health": HEALTH_GRAIN,
    "fact_country_disease_vaccination": ANNUAL_GRAIN,
}
SAMPLE_REGION = "EMR"
SAMPLE_COUNTRIES = ("EGY", "JOR", "MAR", "SAU", "TUN")


def read_countries(silver_dir):
    countries = pd.read_parquet(silver_dir / "ref_countries.parquet", columns=[
        "country_code", "country_name", "region_code",
    ])
    regions = pd.read_parquet(silver_dir / "ref_regions.parquet", columns=["region_code", "region_name"])
    unique(countries, ["country_code"], "country references")
    unique(regions, ["region_code"], "region references")
    members(countries["region_code"], regions["region_code"], "country region")
    return countries.merge(regions, on="region_code", how="left", validate="many_to_one")


def read_observations(silver_dir, name, metrics, grain):
    columns = ["indicator_code", "location_type", "location_code", "year", "estimate"]
    if "sex_code" in grain:
        columns.append("sex_code")
    frame = pd.read_parquet(silver_dir / f"{name}.parquet", columns=columns)
    require(frame["indicator_code"].isin(metrics).all(), f"{name}: unsupported indicator")
    return frame.loc[frame["location_type"].eq("COUNTRY")].rename(columns={"location_code": "country_code"})


def country_metrics(observations, metrics, grain, countries):
    require(observations[grain + ["indicator_code"]].notna().all().all(), "Null country observation key")
    unique(observations, grain + ["indicator_code"], "country observations")
    members(observations["country_code"], countries["country_code"], "observation country")
    # pivot, not pivot_table: never aggregate duplicates or drop all-null observations.
    frame = observations.pivot(index=grain, columns="indicator_code", values="estimate")
    frame = frame.reindex(columns=list(metrics)).rename(columns=metrics).reset_index()
    frame.columns.name = None
    return frame


def fact_schema(metrics, with_sex=False):
    fields = [
        pa.field("country_code", pa.string(), nullable=False),
        pa.field("year", pa.int16(), nullable=False),
    ]
    if with_sex:
        fields.append(pa.field("sex_code", pa.string(), nullable=False))
    fields.extend(pa.field(name, pa.float64()) for name in metrics)
    return pa.schema(fields)


def build_gold(silver_dir=Path("silver_layer"), gold_dir=Path("gold_layer")):
    """Preserve country key unions and nulls; stage all five tables before publication."""
    silver_dir, gold_dir = Path(silver_dir), Path(gold_dir)
    countries = read_countries(silver_dir)
    sexes = pd.read_parquet(silver_dir / "ref_sexes.parquet", columns=["sex_code", "sex_name"])
    unique(sexes, ["sex_code"], "sex references")

    health = country_metrics(
        read_observations(silver_dir, "health_observations", HEALTH_METRICS, HEALTH_GRAIN),
        HEALTH_METRICS, HEALTH_GRAIN, countries,
    )
    members(health["sex_code"], sexes["sex_code"], "health sex")
    health["life_expectancy_hale_gap_years"] = health["life_expectancy_years"] - health["healthy_life_expectancy_years"]

    annual_metrics = DISEASE_METRICS | VACCINATION_METRICS
    annual = country_metrics(pd.concat([
        read_observations(silver_dir, "disease_observations", DISEASE_METRICS, ANNUAL_GRAIN),
        read_observations(silver_dir, "vaccination_coverage", VACCINATION_METRICS, ANNUAL_GRAIN),
    ], ignore_index=True), annual_metrics, ANNUAL_GRAIN, countries)

    years = pd.concat([health[["year"]], annual[["year"]]], ignore_index=True).drop_duplicates()
    tables = {}
    for name, frame, schema in [
        ("dim_country", countries, pa.schema([
            pa.field("country_code", pa.string(), nullable=False),
            pa.field("country_name", pa.string(), nullable=False),
            pa.field("region_code", pa.string()),
            pa.field("region_name", pa.string()),
        ])),
        ("dim_year", years, pa.schema([pa.field("year", pa.int16(), nullable=False)])),
        ("dim_sex", sexes, pa.schema([
            pa.field("sex_code", pa.string(), nullable=False),
            pa.field("sex_name", pa.string(), nullable=False),
        ])),
        ("fact_country_health", health,
         fact_schema([*HEALTH_METRICS.values(), "life_expectancy_hale_gap_years"], with_sex=True)),
        ("fact_country_disease_vaccination", annual, fact_schema(annual_metrics.values())),
    ]:
        unique(frame, TABLE_KEYS[name], name)
        if name.startswith("fact_"):
            members(frame["year"], years["year"], f"{name} year")
        tables[name] = arrow_table(
            frame[schema.names].sort_values(TABLE_KEYS[name]).reset_index(drop=True), schema, name,
        )

    gold_dir.mkdir(parents=True, exist_ok=True)
    # Match silver publication: atomic per file, not a transaction across the set.
    with tempfile.TemporaryDirectory(prefix=".gold-", dir=gold_dir) as staging:
        for name, table in tables.items():
            pq.write_table(table, Path(staging) / f"{name}.parquet", compression="snappy")
        for name in tables:
            (Path(staging) / f"{name}.parquet").replace(gold_dir / f"{name}.parquet")
    counts = {name: table.num_rows for name, table in tables.items()}
    for name, count in counts.items():
        logger.info("%s: wrote %d rows", name, count)
    return counts


def export_gold(gold_dir=Path("gold_layer"), export_dir=Path("gold_exports")):
    """Export the full star and a fixed EMR sample without restricting canonical data."""
    gold_dir, export_dir = Path(gold_dir), Path(export_dir)
    tables = {name: pd.read_parquet(gold_dir / f"{name}.parquet") for name in TABLE_KEYS}
    countries = tables["dim_country"]
    selected = countries.loc[countries["country_code"].isin(SAMPLE_COUNTRIES)]
    require(set(selected["country_code"]) == set(SAMPLE_COUNTRIES), "EMR sample: missing selected countries")
    require(selected["region_code"].eq(SAMPLE_REGION).fillna(False).all(),
            "EMR sample: selected countries must belong to EMR")

    sample = {"dim_country": selected}
    for name in ("fact_country_health", "fact_country_disease_vaccination"):
        sample[name] = tables[name].loc[tables[name]["country_code"].isin(SAMPLE_COUNTRIES)]
    sample_years = pd.concat([
        sample["fact_country_health"]["year"], sample["fact_country_disease_vaccination"]["year"],
    ]).unique()
    sample["dim_year"] = tables["dim_year"].loc[tables["dim_year"]["year"].isin(sample_years)]
    sample["dim_sex"] = tables["dim_sex"].loc[
        tables["dim_sex"]["sex_code"].isin(sample["fact_country_health"]["sex_code"])
    ]

    sample_dir = Path("samples") / SAMPLE_REGION
    exports = {Path(f"{name}.csv"): frame for name, frame in tables.items()}
    exports.update({sample_dir / f"{name}.csv": sample[name] for name in TABLE_KEYS})
    for country in SAMPLE_COUNTRIES:
        for name in ("fact_country_health", "fact_country_disease_vaccination"):
            exports[sample_dir / country / f"{name}.csv"] = sample[name].loc[
                sample[name]["country_code"].eq(country)
            ]

    export_dir.mkdir(parents=True, exist_ok=True)
    # Stage all CSVs first; publication is atomic per file, not across the export set.
    with tempfile.TemporaryDirectory(prefix=".gold-exports-", dir=export_dir) as staging:
        for relative, frame in exports.items():
            path = Path(staging) / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(path, index=False)
        for relative in exports:
            destination = export_dir / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            (Path(staging) / relative).replace(destination)
    logger.info("Exported %d CSV files to %s", len(exports), export_dir)
    return {path.as_posix(): len(frame) for path, frame in exports.items()}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-dir", type=Path, default=Path("silver_layer"))
    parser.add_argument("--gold-dir", type=Path, default=Path("gold_layer"))
    parser.add_argument("--export-dir", type=Path, help="Also export full CSVs and the fixed five-country EMR sample")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        counts = build_gold(args.silver_dir, args.gold_dir)
        if args.export_dir is not None:
            export_gold(args.gold_dir, args.export_dir)
    except (OSError, ValueError, TypeError, KeyError, OverflowError, pa.ArrowException) as error:
        logger.error("Gold transformation failed: %s", error)
        return 1
    logger.info("Gold transformation succeeded: %d tables.", len(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
