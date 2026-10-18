from pathlib import Path
import tempfile
import unittest

import pandas as pd

import gold


class GoldTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.silver = Path(directory.name) / "silver"
        self.output = Path(directory.name) / "gold"
        self.silver.mkdir()
        pd.DataFrame([
            {"country_code": "AAA", "country_name": "Country A", "region_code": "R"},
            {"country_code": "BBB", "country_name": "Country B", "region_code": None},
        ]).to_parquet(self.silver / "ref_countries.parquet", index=False)
        pd.DataFrame([{"region_code": "R", "region_name": "Region"}]).to_parquet(
            self.silver / "ref_regions.parquet", index=False)
        pd.DataFrame([
            {"sex_code": "SEX_BTSX", "sex_name": "Both sexes"},
            {"sex_code": "SEX_FMLE", "sex_name": "Female"},
        ]).to_parquet(self.silver / "ref_sexes.parquet", index=False)
        self.health = pd.DataFrame([
            self.observation("WHOSIS_000001", 80.0, sex_code="SEX_BTSX"),
            self.observation("WHOSIS_000002", 70.0, sex_code="SEX_BTSX"),
            self.observation("WHOSIS_000001", 85.0, sex_code="SEX_FMLE"),
            self.observation("WHOSIS_000002", 72.0, year=2022, sex_code="SEX_FMLE"),
            self.observation("WHOSIS_000004", None, country="BBB", sex_code="SEX_BTSX"),
            self.observation("WHOSIS_000001", 77.0, location_type="REGION", sex_code="SEX_BTSX"),
        ])
        self.health.to_parquet(self.silver / "health_observations.parquet", index=False)
        pd.DataFrame([
            self.observation("WHS3_62", 0.0),
            self.observation("MDG_0000000020", 125.0),
            self.observation("MALARIA_EST_INCIDENCE", 2.0),
            self.observation("WHS3_62", None, year=2022),
            self.observation("WHS3_62", 400.0, location_type="GLOBAL"),
        ]).to_parquet(self.silver / "disease_observations.parquet", index=False)
        pd.DataFrame([
            self.observation("WHS8_110", 95.0),
            self.observation("MCV2", 90.0),
            self.observation("WHS4_100", 0.0, country="BBB", year=2023),
        ]).to_parquet(self.silver / "vaccination_coverage.parquet", index=False)

    @staticmethod
    def observation(code, estimate, country="AAA", year=2021, location_type="COUNTRY", **extra):
        return dict(indicator_code=code, estimate=estimate, location_code=country,
                    location_type=location_type, year=year, **extra)

    def test_matching_keys_only_and_null_only_health_rows_survive(self):
        gold.build_gold(self.silver, self.output)
        frame = pd.read_parquet(self.output / "country_health.parquet").set_index(gold.HEALTH_GRAIN)
        self.assertEqual(set(frame.index), {
            ("AAA", 2021, "SEX_BTSX"), ("AAA", 2021, "SEX_FMLE"),
            ("AAA", 2022, "SEX_FMLE"), ("BBB", 2021, "SEX_BTSX"),
        })
        self.assertEqual(frame.loc[("AAA", 2021, "SEX_BTSX"), "life_expectancy_hale_gap_years"], 10.0)
        self.assertTrue(pd.isna(frame.loc[("AAA", 2021, "SEX_FMLE"), "life_expectancy_hale_gap_years"]))
        self.assertTrue(pd.isna(frame.loc[("AAA", 2022, "SEX_FMLE"), "life_expectancy_hale_gap_years"]))
        self.assertTrue(pd.isna(frame.loc[("BBB", 2021, "SEX_BTSX"), "adult_mortality_per_1000"]))
        self.assertTrue(pd.isna(frame.loc[("BBB", 2021, "SEX_BTSX"), "region_name"]))
        self.assertEqual(frame.loc[("AAA", 2021, "SEX_FMLE"), "sex_name"], "Female")

    def test_annual_union_preserves_zero_missing_and_nonoverlapping_years(self):
        gold.build_gold(self.silver, self.output)
        frame = pd.read_parquet(self.output / "country_disease_vaccination.parquet").set_index(gold.ANNUAL_GRAIN)
        self.assertEqual(set(frame.index), {("AAA", 2021), ("AAA", 2022), ("BBB", 2023)})
        paired = frame.loc[("AAA", 2021)]
        self.assertEqual(paired["measles_reported_cases"], 0.0)
        self.assertEqual(paired["tb_incidence_per_100000"], 125.0)
        self.assertEqual(paired["malaria_incidence_per_1000_at_risk"], 2.0)
        self.assertEqual(paired["mcv1_coverage_percent"], 95.0)
        self.assertTrue(pd.isna(frame.loc[("AAA", 2022), "measles_reported_cases"]))
        self.assertEqual(frame.loc[("BBB", 2023), "dtp3_coverage_percent"], 0.0)
        self.assertTrue(pd.isna(frame.loc[("BBB", 2023), "measles_reported_cases"]))

    def test_invalid_country_keys_fail_without_replacing_previous_outputs(self):
        gold.build_gold(self.silver, self.output)
        before = {path.name: path.read_bytes() for path in self.output.iterdir()}
        duplicate = pd.concat([self.health, self.health.iloc[[0]]], ignore_index=True)
        unknown = self.health.copy()
        unknown.loc[0, "location_code"] = "UNKNOWN"
        for invalid, message in [(duplicate, "duplicate key"), (unknown, "unknown reference codes")]:
            with self.subTest(message=message):
                invalid.to_parquet(self.silver / "health_observations.parquet", index=False)
                with self.assertRaisesRegex(ValueError, message):
                    gold.build_gold(self.silver, self.output)
                self.assertEqual(before, {path.name: path.read_bytes() for path in self.output.iterdir()})


if __name__ == "__main__":
    unittest.main()
