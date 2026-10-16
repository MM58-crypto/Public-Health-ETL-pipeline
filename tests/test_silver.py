from copy import deepcopy
from datetime import date
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import silver


def reference(code, title, dimension, region=None):
    return {
        "Code": code, "Title": title, "Dimension": dimension,
        "ParentDimension": "REGION" if region else None,
        "ParentCode": region, "ParentTitle": "Eastern Mediterranean" if region else None,
    }


def observation(code, identifier=1, location_type="COUNTRY", location="SAU", year=2021, sex="SEX_BTSX", estimate=100.5):
    return {
        "Id": identifier, "IndicatorCode": code,
        "SpatialDimType": location_type, "SpatialDim": location,
        "TimeDimType": "YEAR", "TimeDim": year, "Dim1Type": "SEX", "Dim1": sex,
        "Dim2Type": None, "Dim2": None, "Dim3Type": None, "Dim3": None,
        "DataSourceDimType": None, "DataSourceDim": None,
        "NumericValue": estimate, "Low": None, "High": None,
        "Value": "source display", "Date": "2026-10-01T16:11:13.08+02:00",
        "TimeDimensionValue": str(year),
        "TimeDimensionBegin": f"{year}-01-01T00:00:00+14:00",
        "TimeDimensionEnd": f"{year}-12-31T23:59:59-12:00",
        "ParentLocationCode": "EMR" if location_type == "COUNTRY" else None,
        "ParentLocation": "Eastern Mediterranean" if location_type == "COUNTRY" else None,
    }


class SilverTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.bronze = Path(directory.name) / "bronze"
        self.output = Path(directory.name) / "silver"
        self.bronze.mkdir()
        self.rows = {
            "sex_vals": [reference(code, label, "SEX") for code, label in [
                ("SEX_BTSX", "Both sexes"), ("SEX_MLE", "Male"),
                ("SEX_FMLE", "Female"), ("SEX_NOA", "Not applicable"),
            ]],
            "region_vals": [reference("EMR", "Eastern Mediterranean", "REGION"), reference("AFR", "Africa", "REGION")],
            "income_group_vals": [reference("WB_HI", "High-income", "WORLDBANKINCOMEGROUP")],
            "country_vals": [reference("SAU", "Saudi Arabia", "COUNTRY", "EMR"), reference("ZZZ", "No parent example", "COUNTRY")],
            "indicators": [
                {"IndicatorCode": "WHOSIS_000004", "IndicatorName": "Adult mortality", "Language": "EN"},
                {"IndicatorCode": "WHOSIS_000001", "IndicatorName": "Life expectancy at birth", "Language": "EN"},
                {"IndicatorCode": "WHOSIS_000002", "IndicatorName": "Healthy life expectancy at birth", "Language": "EN"},
            ],
            "adult_mortality": [
                observation("WHOSIS_000004"),
                observation("WHOSIS_000004", 2, "GLOBAL", "GLOBAL"),
                observation("WHOSIS_000004", 3, "REGION", "EMR"),
                observation("WHOSIS_000004", 4, "WORLDBANKINCOMEGROUP", "WB_HI"),
                observation("WHOSIS_000004", 5, sex="SEX_FMLE", estimate=None),
            ],
            "life_expectancy_at_birth": [observation("WHOSIS_000001", year=2023, estimate=75.2)],
            "hale_at_birth": [observation("WHOSIS_000002", year=2023, estimate=65.1)],
        }
        self.rows["life_expectancy_at_birth"][0].update(Low=72.0, High=78.0, Comments="Bronze-only annotation")
        self.save_inputs()

    def save_inputs(self):
        for name, rows in self.rows.items():
            (self.bronze / f"{name}.json").write_text(json.dumps({"value": rows}), encoding="utf-8")

    def output_rows(self, table="health_observations"):
        return pq.read_table(self.output / f"{table}.parquet").to_pylist()

    def test_preserves_grain_nulls_aggregates_and_local_calendar_dates(self):
        silver.build_silver(self.bronze, self.output)
        table = pq.read_table(self.output / "health_observations.parquet")
        rows = table.to_pylist()
        self.assertEqual(len(rows), 7)
        self.assertEqual({row["location_type"] for row in rows}, {"COUNTRY", "REGION", "GLOBAL", "WORLDBANKINCOMEGROUP"})
        self.assertEqual({row["year"] for row in rows}, {2021, 2023})
        self.assertEqual(sum(row["estimate"] is None for row in rows), 1)
        self.assertEqual(sum(row["lower_bound"] is None for row in rows), 6)
        life = next(row for row in rows if row["indicator_code"] == "WHOSIS_000001")
        self.assertEqual(life["period_start"], date(2023, 1, 1))
        self.assertEqual(life["period_end"], date(2023, 12, 31))
        self.assertEqual(life["estimate"], 75.2)
        self.assertEqual(life["display_value"], "source display")
        self.assertNotIn("comments", table.column_names)
        self.assertNotIn("source_file", table.column_names)
        self.assertEqual(life["source_date"], "2026-10-01T16:11:13.08+02:00")
        self.assertEqual(table.schema.field("year").type, pa.int16())
        self.assertEqual(table.schema.field("period_start").type, pa.date32())
        self.assertEqual(table.schema.field("source_observation_id").type, pa.int64())
        self.assertFalse(table.schema.field("indicator_code").nullable)
        self.assertTrue(table.schema.field("estimate").nullable)
        self.assertIn("SEX_NOA", {row["sex_code"] for row in self.output_rows("ref_sexes")})
        countries = {row["country_code"]: row for row in self.output_rows("ref_countries")}
        self.assertIsNone(countries["ZZZ"]["region_code"])
        units = {row["indicator_code"]: row["unit"] for row in self.output_rows("ref_indicators")}
        self.assertEqual(units, {"WHOSIS_000004": "per_1000", "WHOSIS_000001": "years", "WHOSIS_000002": "years"})

    def test_rerun_is_equivalent_and_bronze_is_unchanged(self):
        before = {path.name: path.read_bytes() for path in self.bronze.iterdir()}
        silver.build_silver(self.bronze, self.output)
        first = {path.name: pq.read_table(path) for path in self.output.iterdir()}
        silver.build_silver(self.bronze, self.output)
        for name, table in first.items():
            self.assertTrue(table.equals(pq.read_table(self.output / name)))
        self.assertEqual(before, {path.name: path.read_bytes() for path in self.bronze.iterdir()})

    def test_duplicate_business_key_fails_without_replacing_existing_outputs(self):
        silver.build_silver(self.bronze, self.output)
        before = {path.name: path.read_bytes() for path in self.output.iterdir()}
        duplicate = deepcopy(self.rows["adult_mortality"][0])
        duplicate["Id"] = 99999
        self.rows["adult_mortality"].append(duplicate)
        self.save_inputs()
        with self.assertLogs("silver", level="ERROR"):
            self.assertEqual(silver.main(["--bronze-dir", str(self.bronze), "--silver-dir", str(self.output)]), 1)
        self.assertEqual(before, {path.name: path.read_bytes() for path in self.output.iterdir()})

    def test_unresolved_reference_and_parent_mismatch_fail(self):
        original = deepcopy(self.rows["adult_mortality"][0])
        for field, value in [("Dim1", "UNKNOWN"), ("SpatialDim", "UNKNOWN"), ("ParentLocation", "Wrong region")]:
            with self.subTest(field=field):
                self.rows["adult_mortality"][0] = {**original, field: value}
                self.save_inputs()
                with self.assertRaises(silver.ValidationError):
                    silver.build_silver(self.bronze, self.output)
                self.assertFalse(self.output.exists())

    def test_reference_duplicates_cannot_multiply_observations(self):
        self.rows["sex_vals"].append(deepcopy(self.rows["sex_vals"][0]))
        self.save_inputs()
        with self.assertRaises(silver.ValidationError):
            silver.build_silver(self.bronze, self.output)
        self.assertFalse(self.output.exists())

    def test_unexpected_dimensions_and_wrong_indicator_fail(self):
        original = deepcopy(self.rows["adult_mortality"][0])
        for update in [{"Dim1Type": "AGEGROUP"}, {"Dim2Type": "AGEGROUP", "Dim2": "AGE_15"},
                       {"DataSourceDim": "new source"}, {"IndicatorCode": "WHOSIS_000015"}]:
            with self.subTest(update=update):
                self.rows["adult_mortality"][0] = {**original, **update}
                self.save_inputs()
                with self.assertRaises(silver.ValidationError):
                    silver.build_silver(self.bronze, self.output)
                self.assertFalse(self.output.exists())

    def test_bad_numbers_are_not_coerced_to_null_or_truncated(self):
        original = deepcopy(self.rows["adult_mortality"][0])
        for field, value in [("NumericValue", ""), ("NumericValue", True), ("NumericValue", "inf"),
                             ("TimeDim", 2021.5), ("Id", 2**63)]:
            with self.subTest(field=field, value=value):
                self.rows["adult_mortality"][0] = {**original, field: value}
                self.save_inputs()
                with self.assertRaises(silver.ValidationError):
                    silver.build_silver(self.bronze, self.output)
                self.assertFalse(self.output.exists())

    def test_bad_periods_and_bounds_fail(self):
        original = deepcopy(self.rows["adult_mortality"][0])
        for update in [{"TimeDimensionBegin": "2021-02-30T00:00:00+01:00"},
                       {"TimeDimensionEnd": "2022-12-31T00:00:00+01:00"},
                       {"TimeDimensionValue": "2020"}, {"Low": 101.0}, {"High": 99.0},
                       {"NumericValue": None, "Low": 10.0, "High": 5.0}]:
            with self.subTest(update=update):
                self.rows["adult_mortality"][0] = {**original, **update}
                self.save_inputs()
                with self.assertRaises((silver.ValidationError, ValueError)):
                    silver.build_silver(self.bronze, self.output)
                self.assertFalse(self.output.exists())

    def test_missing_nullable_source_field_is_not_assumed_to_be_null(self):
        del self.rows["adult_mortality"][0]["Dim2Type"]
        self.save_inputs()
        with self.assertRaises(silver.ValidationError):
            silver.build_silver(self.bronze, self.output)
        self.assertFalse(self.output.exists())

    def test_staging_write_failure_leaves_previous_outputs_intact(self):
        silver.build_silver(self.bronze, self.output)
        before = {path.name: path.read_bytes() for path in self.output.iterdir()}
        write_table = pq.write_table
        writes = 0

        def fail_second_write(*args, **kwargs):
            nonlocal writes
            writes += 1
            if writes == 2:
                raise OSError("simulated full disk")
            return write_table(*args, **kwargs)

        with patch("silver.pq.write_table", side_effect=fail_second_write):
            with self.assertRaises(OSError):
                silver.build_silver(self.bronze, self.output)
        self.assertEqual(before, {path.name: path.read_bytes() for path in self.output.iterdir()})


if __name__ == "__main__":
    unittest.main()
