"""Build typed silver Parquet tables from stored WHO bronze responses."""

import argparse
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent
METADATA_BASE = "https://www.who.int/data/gho/data/indicators/indicator-details/GHO/"
INDICATORS = {
    "WHOSIS_000004": {
        "filename": "adult_mortality.json",
        "unit": "per_1000",
        "definition": "Probability that a person aged 15 dies before age 60 under the reporting year's mortality conditions, expressed per 1,000.",
        "definition_url": METADATA_BASE + "adult-mortality-rate-%28probability-of-dying-between-15-and-60-years-per-1000-population%29",
    },
    "WHOSIS_000001": {
        "filename": "life_expectancy_at_birth.json",
        "unit": "years",
        "definition": "Expected years of life from birth under the prevailing age- and sex-specific mortality conditions.",
        "definition_url": METADATA_BASE + "life-expectancy",
    },
    "WHOSIS_000002": {
        "filename": "hale_at_birth.json",
        "unit": "years",
        "definition": "Expected equivalent years in full health from birth under the prevailing age- and sex-specific mortality and morbidity conditions.",
        "definition_url": METADATA_BASE + "gho-ghe-hale-healthy-life-expectancy",
    },
}
GRAIN = ["indicator_code", "location_type", "location_code", "year", "sex_code"]
UNUSED_DIMENSIONS = ["Dim2Type", "Dim2", "Dim3Type", "Dim3", "DataSourceDimType", "DataSourceDim"]
OBSERVATION_COLUMNS = [
    "Id", "IndicatorCode", "SpatialDimType", "SpatialDim", "TimeDimType", "TimeDim",
    "Dim1Type", "Dim1", *UNUSED_DIMENSIONS, "NumericValue", "Low", "High", "Value",
    "Date", "TimeDimensionValue", "TimeDimensionBegin", "TimeDimensionEnd",
    "ParentLocationCode", "ParentLocation",
]
REFERENCE_COLUMNS = ["Code", "Title", "Dimension", "ParentDimension", "ParentCode", "ParentTitle"]
OBSERVATION_SCHEMA = pa.schema([
    pa.field("indicator_code", pa.string(), nullable=False),
    pa.field("indicator_name", pa.string(), nullable=False),
    pa.field("location_type", pa.string(), nullable=False),
    pa.field("location_code", pa.string(), nullable=False),
    pa.field("year", pa.int16(), nullable=False),
    pa.field("sex_code", pa.string(), nullable=False),
    pa.field("estimate", pa.float64()),
    pa.field("lower_bound", pa.float64()),
    pa.field("upper_bound", pa.float64()),
    pa.field("display_value", pa.string()),
    pa.field("period_start", pa.date32(), nullable=False),
    pa.field("period_end", pa.date32(), nullable=False),
    pa.field("source_observation_id", pa.int64(), nullable=False),
    pa.field("source_date", pa.string()),
])


class ValidationError(ValueError):
    """A bronze input violates the supported silver data contract."""


def require(condition, message):
    if not condition:
        raise ValidationError(message)


def reject_nonfinite_json(value):
    raise ValidationError(f"Non-finite JSON number: {value}")


def read_collection(path, columns):
    with path.open(encoding="utf-8") as source:
        payload = json.load(source, parse_constant=reject_nonfinite_json)
    require(isinstance(payload, dict) and isinstance(payload.get("value"), list),
            f"{path}: expected an OData object with a value list")
    rows = payload["value"]
    require(bool(rows), f"{path}: required collection is empty")
    required = set(columns)
    for index, row in enumerate(rows):
        require(isinstance(row, dict), f"{path}: row {index} is not an object")
        missing = required - row.keys()
        require(not missing, f"{path}: row {index} is missing fields {sorted(missing)}")
    return pd.DataFrame.from_records(rows, columns=columns)


def source_path(path):
    return Path(os.path.relpath(path.resolve(), PROJECT_ROOT)).as_posix()


def strings(frame, columns, label, nullable=False):
    for column in columns:
        values = frame[column]
        require(nullable or values.notna().all(), f"{label}: {column} contains nulls")
        require(values.dropna().map(lambda value: isinstance(value, str)).all(),
                f"{label}: {column} must contain strings")
        frame[column] = values.astype("string")
        if not nullable:
            require(frame[column].str.strip().ne("").all(), f"{label}: {column} contains empty strings")


def unique(frame, columns, label):
    require(not frame.duplicated(columns).any(), f"{label}: duplicate key {columns}")


def members(values, allowed, label):
    unknown = values[values.notna() & ~values.isin(allowed)].unique().tolist()
    require(not unknown, f"{label}: unknown reference codes {unknown[:5]}")


def numbers(values, label, dtype, nullable=False):
    require(nullable or values.notna().all(), f"{label}: null numeric value")
    require(not values.map(lambda value: isinstance(value, bool)).any(), f"{label}: boolean is not a number")
    try:
        converted = pd.to_numeric(values, errors="raise")
        require(not (values.notna() & converted.isna()).any(), f"{label}: invalid numeric value")
        require(not converted.isin([float("inf"), float("-inf")]).any(), f"{label}: non-finite number")
        return converted.astype(dtype)
    except (ValueError, TypeError, OverflowError) as error:
        raise ValidationError(f"{label}: invalid {dtype} conversion: {error}") from error


def period_dates(values, label):
    # Extract local calendar dates, not UTC dates: +01:00 January 1 is still January 1.
    parsed = {}
    for value in values.unique():
        try:
            require(isinstance(value, str) and "T" in value, f"{label}: expected ISO timestamp")
            parsed[value] = datetime.fromisoformat(value).date()
        except (ValueError, TypeError) as error:
            raise ValidationError(f"{label}: invalid period timestamp {value!r}") from error
    return values.map(parsed)


def read_reference(bronze_dir, filename, dimension):
    frame = read_collection(bronze_dir / filename, REFERENCE_COLUMNS)
    strings(frame, ["Code", "Title", "Dimension"], filename)
    strings(frame, ["ParentDimension", "ParentCode", "ParentTitle"], filename, nullable=True)
    require(frame["Dimension"].eq(dimension).all(), f"{filename}: expected dimension {dimension}")
    unique(frame, ["Code"], filename)
    frame["source_file"] = source_path(bronze_dir / filename)
    return frame


def build_references(bronze_dir):
    references = {}
    for table, filename, dimension, prefix in [
        ("ref_sexes", "sex_vals.json", "SEX", "sex"),
        ("ref_regions", "region_vals.json", "REGION", "region"),
        ("ref_income_groups", "income_group_vals.json", "WORLDBANKINCOMEGROUP", "income_group"),
    ]:
        frame = read_reference(bronze_dir, filename, dimension)
        require(frame[["ParentDimension", "ParentCode", "ParentTitle"]].isna().all().all(),
                f"{filename}: unexpected parent hierarchy")
        references[table] = frame.rename(columns={"Code": f"{prefix}_code", "Title": f"{prefix}_name"})[
            [f"{prefix}_code", f"{prefix}_name", "source_file"]
        ]

    countries = read_reference(bronze_dir, "country_vals.json", "COUNTRY")
    has_parent = countries["ParentDimension"].notna()
    require(countries.loc[has_parent, "ParentDimension"].eq("REGION").all(),
            "country_vals.json: unsupported parent dimension")
    require(countries.loc[~has_parent, ["ParentCode", "ParentTitle"]].isna().all().all(),
            "country_vals.json: parent values without a parent dimension")
    require(countries.loc[has_parent, ["ParentCode", "ParentTitle"]].notna().all().all(),
            "country_vals.json: incomplete region parent")
    regions = references["ref_regions"].set_index("region_code")["region_name"]
    members(countries["ParentCode"], regions.index, "country parent region")
    require(countries.loc[has_parent, "ParentTitle"].eq(countries.loc[has_parent, "ParentCode"].map(regions)).all(),
            "country_vals.json: parent region label mismatch")
    references["ref_countries"] = countries.rename(columns={
        "Code": "country_code", "Title": "country_name", "ParentCode": "region_code",
    })[["country_code", "country_name", "region_code", "source_file"]]

    indicators = read_collection(bronze_dir / "indicators.json", ["IndicatorCode", "IndicatorName", "Language"])
    strings(indicators, ["IndicatorCode", "IndicatorName", "Language"], "indicators.json")
    selected = indicators.loc[indicators["IndicatorCode"].isin(INDICATORS)].copy()
    unique(selected, ["IndicatorCode"], "selected indicators")
    require(set(selected["IndicatorCode"]) == set(INDICATORS), "indicators.json: missing selected indicator")
    selected = selected.rename(columns={"IndicatorCode": "indicator_code", "IndicatorName": "indicator_name"})
    for field in ["unit", "definition", "definition_url"]:
        selected[field] = selected["indicator_code"].map({code: metadata[field] for code, metadata in INDICATORS.items()})
    selected["source_file"] = source_path(bronze_dir / "indicators.json")
    references["ref_indicators"] = selected[["indicator_code", "indicator_name", "unit", "definition", "definition_url", "source_file"]]
    return references


def build_observations(bronze_dir, references):
    frames = []
    input_count = 0
    for code, metadata in INDICATORS.items():
        filename = metadata["filename"]
        frame = read_collection(bronze_dir / filename, OBSERVATION_COLUMNS)
        strings(frame, ["IndicatorCode", "SpatialDimType", "SpatialDim", "TimeDimType", "Dim1Type", "Dim1", "TimeDimensionValue", "TimeDimensionBegin", "TimeDimensionEnd"], filename)
        strings(frame, ["Value", "Date", "ParentLocationCode", "ParentLocation"], filename, nullable=True)
        require(frame["IndicatorCode"].eq(code).all(), f"{filename}: expected indicator {code}")
        require(frame["TimeDimType"].eq("YEAR").all(), f"{filename}: unsupported time dimension")
        require(frame["Dim1Type"].eq("SEX").all(), f"{filename}: unsupported first dimension")
        require(frame[UNUSED_DIMENSIONS].isna().all().all(), f"{filename}: unexpected additional dimensions")
        frame["TimeDim"] = numbers(frame["TimeDim"], f"{filename}: TimeDim", "Int16")
        frame["Id"] = numbers(frame["Id"], f"{filename}: Id", "Int64")
        for column in ["NumericValue", "Low", "High"]:
            frame[column] = numbers(frame[column], f"{filename}: {column}", "Float64", nullable=True)
        frame["period_start"] = period_dates(frame["TimeDimensionBegin"], filename)
        frame["period_end"] = period_dates(frame["TimeDimensionEnd"], filename)
        require(frame["TimeDimensionValue"].eq(frame["TimeDim"].astype("string")).all(),
                f"{filename}: descriptive year mismatch")
        for column, month, day in [("period_start", 1, 1), ("period_end", 12, 31)]:
            dates = pd.to_datetime(frame[column], errors="raise")
            require((dates.dt.year.eq(frame["TimeDim"]) & dates.dt.month.eq(month) & dates.dt.day.eq(day)).all(),
                    f"{filename}: period does not match the observation year")
        for lower, upper in [("Low", "NumericValue"), ("NumericValue", "High"), ("Low", "High")]:
            present = frame[lower].notna() & frame[upper].notna()
            require(frame.loc[present, lower].le(frame.loc[present, upper]).all(),
                    f"{filename}: invalid uncertainty bounds")
        input_count += len(frame)
        frames.append(frame)

    observations = pd.concat(frames, ignore_index=True).rename(columns={
        "IndicatorCode": "indicator_code", "SpatialDimType": "location_type", "SpatialDim": "location_code",
        "TimeDim": "year", "Dim1": "sex_code", "NumericValue": "estimate", "Low": "lower_bound", "High": "upper_bound",
        "Value": "display_value", "Id": "source_observation_id", "Date": "source_date",
    })
    unique(observations, GRAIN, "health observations")
    members(observations["indicator_code"], references["ref_indicators"]["indicator_code"], "indicator")
    indicator_names = references["ref_indicators"].set_index("indicator_code")["indicator_name"]
    observations["indicator_name"] = observations["indicator_code"].map(indicator_names)
    members(observations["sex_code"], references["ref_sexes"]["sex_code"], "sex")
    members(observations["location_type"], ["COUNTRY", "REGION", "WORLDBANKINCOMEGROUP", "GLOBAL"], "location type")
    for location_type, table, key in [
        ("COUNTRY", "ref_countries", "country_code"),
        ("REGION", "ref_regions", "region_code"),
        ("WORLDBANKINCOMEGROUP", "ref_income_groups", "income_group_code"),
    ]:
        applicable = observations.loc[observations["location_type"].eq(location_type), ["location_code"]]
        joined = applicable.merge(references[table][[key]], left_on="location_code", right_on=key,
                                  how="left", validate="many_to_one", indicator=True)
        require(joined["_merge"].eq("both").all(), f"{location_type}: unresolved location reference")
    global_rows = observations["location_type"].eq("GLOBAL")
    require(observations.loc[global_rows, "location_code"].eq("GLOBAL").all(), "Unexpected GLOBAL location code")

    is_country = observations["location_type"].eq("COUNTRY")
    countries = references["ref_countries"].set_index("country_code")
    regions = references["ref_regions"].set_index("region_code")["region_name"]
    country_rows = observations.loc[is_country]
    expected_regions = country_rows["location_code"].map(countries["region_code"])
    expected_labels = expected_regions.map(regions)
    for column, expected in [("ParentLocationCode", expected_regions), ("ParentLocation", expected_labels)]:
        actual = country_rows[column]
        matches = actual.eq(expected).fillna(False) | (actual.isna() & expected.isna())
        require(matches.all(), f"Country observations: {column} mismatch with references")
    require(observations.loc[~is_country, ["ParentLocationCode", "ParentLocation"]].isna().all().all(),
            "Aggregate observations: unexpected parent location")
    require(len(observations) == input_count, "Observation row count changed during transformation")
    return observations[OBSERVATION_SCHEMA.names].sort_values(GRAIN).reset_index(drop=True)


def arrow_table(frame, schema, label):
    for field in schema:
        require(field.nullable or frame[field.name].notna().all(), f"{label}: {field.name} cannot be null")
    return pa.Table.from_pandas(frame, schema=schema, preserve_index=False, safe=True).replace_schema_metadata(None)


def build_silver(bronze_dir=Path("bronze_layer"), silver_dir=Path("silver_layer")):
    """Validate all inputs before writing; return the number of rows in each output."""
    bronze_dir, silver_dir = Path(bronze_dir), Path(silver_dir)
    references = build_references(bronze_dir)
    observations = build_observations(bronze_dir, references)
    tables = {"health_observations": arrow_table(observations, OBSERVATION_SCHEMA, "health observations")}
    for name, frame in references.items():
        schema = pa.schema([
            pa.field(column, pa.string(), nullable=(name == "ref_countries" and column == "region_code"))
            for column in frame.columns
        ])
        frame = frame.sort_values(frame.columns[0]).reset_index(drop=True)
        tables[name] = arrow_table(frame, schema, name)

    silver_dir.mkdir(parents=True, exist_ok=True)
    # Stage every table before replacing outputs. Replacements are atomic per file, not across the set.
    with tempfile.TemporaryDirectory(prefix=".silver-", dir=silver_dir) as staging:
        for name, table in tables.items():
            pq.write_table(table, Path(staging) / f"{name}.parquet", compression="snappy")
        for name in tables:
            (Path(staging) / f"{name}.parquet").replace(silver_dir / f"{name}.parquet")
    counts = {name: table.num_rows for name, table in tables.items()}
    for name, count in counts.items():
        logger.info("%s: wrote %d rows", name, count)
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-dir", type=Path, default=Path("bronze_layer"))
    parser.add_argument("--silver-dir", type=Path, default=Path("silver_layer"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        counts = build_silver(args.bronze_dir, args.silver_dir)
    except (OSError, ValueError, TypeError, OverflowError, pa.ArrowException) as error:
        logger.error("Silver transformation failed: %s", error)
        return 1
    logger.info("Silver transformation succeeded: %d tables.", len(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
