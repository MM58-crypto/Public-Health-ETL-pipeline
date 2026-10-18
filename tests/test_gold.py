from pathlib import Path
import tempfile
import unittest

from unittest.mock import patch
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
        facts = pd.read_parquet(self.output / "fact_country_health.parquet")
        countries = pd.read_parquet(self.output / "dim_country.parquet")
        sexes = pd.read_parquet(self.output / "dim_sex.parquet")
        years = pd.read_parquet(self.output / "dim_year.parquet")
        self.assertEqual(set(years["year"]), {2021, 2022, 2023})
        self.assertNotIn("country_name", facts)
        self.assertNotIn("region_name", facts)
        self.assertNotIn("sex_name", facts)
        frame = facts.merge(countries, on="country_code", validate="many_to_one").merge(
            sexes, on="sex_code", validate="many_to_one",
        ).merge(years, on="year", validate="many_to_one").set_index(gold.HEALTH_GRAIN)
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
        frame = pd.read_parquet(self.output / "fact_country_disease_vaccination.parquet").set_index(gold.ANNUAL_GRAIN)
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

    def test_invalid_dimensions_cannot_multiply_or_orphan_facts(self):
        gold.build_gold(self.silver, self.output)
        before = {path.name: path.read_bytes() for path in self.output.iterdir()}
        countries = pd.read_parquet(self.silver / "ref_countries.parquet")
        sexes = pd.read_parquet(self.silver / "ref_sexes.parquet")
        bad_region = countries.copy()
        bad_region.loc[0, "region_code"] = "UNKNOWN"
        for name, invalid, message in [
            ("ref_countries", pd.concat([countries, countries.iloc[[0]]]), "duplicate key"),
            ("ref_countries", bad_region, "unknown reference codes"),
            ("ref_sexes", sexes.loc[sexes.sex_code.ne("SEX_FMLE")], "unknown reference codes"),
        ]:
            with self.subTest(name=name, message=message):
                invalid.to_parquet(self.silver / f"{name}.parquet", index=False)
                with self.assertRaisesRegex(ValueError, message):
                    gold.build_gold(self.silver, self.output)
                self.assertEqual(before, {path.name: path.read_bytes() for path in self.output.iterdir()})
                countries.to_parquet(self.silver / "ref_countries.parquet", index=False)
                sexes.to_parquet(self.silver / "ref_sexes.parquet", index=False)

    def build_sample(self):
        countries = pd.read_parquet(self.silver / "ref_countries.parquet")
        pd.concat([countries, pd.DataFrame([
            {"country_code": code, "country_name": code, "region_code": "EMR"}
            for code in gold.SAMPLE_COUNTRIES
        ])], ignore_index=True).to_parquet(self.silver / "ref_countries.parquet", index=False)
        regions = pd.read_parquet(self.silver / "ref_regions.parquet")
        pd.concat([regions, pd.DataFrame([
            {"region_code": "EMR", "region_name": "Eastern Mediterranean"},
        ])], ignore_index=True).to_parquet(self.silver / "ref_regions.parquet", index=False)
        for name in ("health_observations", "disease_observations", "vaccination_coverage"):
            source = pd.read_parquet(self.silver / f"{name}.parquet")
            selected = source.loc[source.location_code.eq("AAA") & source.location_type.eq("COUNTRY")]
            pd.concat([source, *[
                selected.assign(location_code=country) for country in gold.SAMPLE_COUNTRIES
            ]], ignore_index=True).to_parquet(self.silver / f"{name}.parquet", index=False)
        gold.build_gold(self.silver, self.output)
        return self.output.parent / "exports"

    def test_sample_filters_countries_and_dimensions_without_restricting_full_data(self):
        exports = self.build_sample()
        gold.export_gold(self.output, exports)
        sample = exports / "samples" / "EMR"
        full = pd.read_csv(exports / "fact_country_disease_vaccination.csv")
        self.assertEqual(set(full.country_code), {"AAA", "BBB", *gold.SAMPLE_COUNTRIES})
        countries = pd.read_csv(sample / "dim_country.csv")
        self.assertEqual(set(countries.country_code), set(gold.SAMPLE_COUNTRIES))
        self.assertEqual(set(countries.region_code), {"EMR"})
        self.assertEqual(set(pd.read_csv(sample / "dim_year.csv").year), {2021, 2022})
        for name, grain in [
            ("fact_country_health", gold.HEALTH_GRAIN),
            ("fact_country_disease_vaccination", gold.ANNUAL_GRAIN),
        ]:
            regional = pd.read_csv(sample / f"{name}.csv")
            self.assertEqual(set(regional.country_code), set(gold.SAMPLE_COUNTRIES))
            self.assertFalse(regional.duplicated(grain).any())
            for country in gold.SAMPLE_COUNTRIES:
                local = pd.read_csv(sample / country / f"{name}.csv")
                self.assertEqual(set(local.country_code), {country})
                self.assertEqual(
                    set(local[grain].itertuples(index=False, name=None)),
                    set(regional.loc[regional.country_code.eq(country), grain].itertuples(index=False, name=None)),
                )
        self.assertEqual(set(pd.read_parquet(self.output / "dim_country.parquet").country_code),
                         {"AAA", "BBB", *gold.SAMPLE_COUNTRIES})

    def test_invalid_sample_leaves_previous_exports_untouched(self):
        exports = self.build_sample()
        gold.export_gold(self.output, exports)
        before = {path.relative_to(exports): path.read_bytes() for path in exports.rglob("*.csv")}
        countries = pd.read_parquet(self.output / "dim_country.parquet")
        wrong_region = countries.copy()
        wrong_region.loc[wrong_region.country_code.eq("SAU"), "region_code"] = "R"
        for invalid, message in [
            (countries.loc[countries.country_code.ne("SAU")], "missing selected countries"),
            (wrong_region, "must belong to EMR"),
        ]:
            with self.subTest(message=message):
                invalid.to_parquet(self.output / "dim_country.parquet", index=False)
                with self.assertRaisesRegex(ValueError, message):
                    gold.export_gold(self.output, exports)
                self.assertEqual(before, {
                    path.relative_to(exports): path.read_bytes() for path in exports.rglob("*.csv")
                })

    def test_csv_staging_failure_preserves_previous_exports(self):
        exports = self.build_sample()
        gold.export_gold(self.output, exports)
        before = {path.relative_to(exports): path.read_bytes() for path in exports.rglob("*.csv")}
        with patch.object(pd.DataFrame, "to_csv", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                gold.export_gold(self.output, exports)
        self.assertEqual(before, {
            path.relative_to(exports): path.read_bytes() for path in exports.rglob("*.csv")
        })



if __name__ == "__main__":
    unittest.main()
