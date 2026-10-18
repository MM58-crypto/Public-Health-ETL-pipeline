"""Build country-level gold analytical marts from validated silver Parquet tables."""

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
    return frame.merge(countries, on="country_code", how="left", validate="many_to_one")


def mart_schema(metrics, with_sex=False):
    fields = [
        pa.field("country_code", pa.string(), nullable=False),
        pa.field("country_name", pa.string(), nullable=False),
        pa.field("region_code", pa.string()),
        pa.field("region_name", pa.string()),
        pa.field("year", pa.int16(), nullable=False),
    ]
    if with_sex:
        fields.extend([
            pa.field("sex_code", pa.string(), nullable=False),
            pa.field("sex_name", pa.string(), nullable=False),
        ])
    fields.extend(pa.field(name, pa.float64()) for name in metrics)
    return pa.schema(fields)


def build_gold(silver_dir=Path("silver_layer"), gold_dir=Path("gold_layer")):
    """Preserve country key unions and nulls; stage both marts before publication."""
    silver_dir, gold_dir = Path(silver_dir), Path(gold_dir)
    countries = read_countries(silver_dir)
    sexes = pd.read_parquet(silver_dir / "ref_sexes.parquet", columns=["sex_code", "sex_name"])
    unique(sexes, ["sex_code"], "sex references")

    health = country_metrics(
        read_observations(silver_dir, "health_observations", HEALTH_METRICS, HEALTH_GRAIN),
        HEALTH_METRICS, HEALTH_GRAIN, countries,
    )
    members(health["sex_code"], sexes["sex_code"], "health sex")
    health = health.merge(sexes, on="sex_code", how="left", validate="many_to_one")
    health["life_expectancy_hale_gap_years"] = health["life_expectancy_years"] - health["healthy_life_expectancy_years"]

    annual_metrics = DISEASE_METRICS | VACCINATION_METRICS
    annual = country_metrics(pd.concat([
        read_observations(silver_dir, "disease_observations", DISEASE_METRICS, ANNUAL_GRAIN),
        read_observations(silver_dir, "vaccination_coverage", VACCINATION_METRICS, ANNUAL_GRAIN),
    ], ignore_index=True), annual_metrics, ANNUAL_GRAIN, countries)

    tables = {}
    for name, frame, grain, schema in [
        ("country_health", health, HEALTH_GRAIN,
         mart_schema([*HEALTH_METRICS.values(), "life_expectancy_hale_gap_years"], with_sex=True)),
        ("country_disease_vaccination", annual, ANNUAL_GRAIN, mart_schema(annual_metrics.values())),
    ]:
        tables[name] = arrow_table(frame[schema.names].sort_values(grain).reset_index(drop=True), schema, name)

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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-dir", type=Path, default=Path("silver_layer"))
    parser.add_argument("--gold-dir", type=Path, default=Path("gold_layer"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        counts = build_gold(args.silver_dir, args.gold_dir)
    except (OSError, ValueError, TypeError, KeyError, OverflowError, pa.ArrowException) as error:
        logger.error("Gold transformation failed: %s", error)
        return 1
    logger.info("Gold transformation succeeded: %d tables.", len(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
