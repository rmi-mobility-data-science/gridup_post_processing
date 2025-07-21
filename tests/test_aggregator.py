import unittest
import duckdb
import pandas as pd
import tempfile
import os
from src.joiner import join
from src.aggregator import generate_results


class TestAggregator(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.con = duckdb.connect()

        # Create test input data
        load_df = pd.DataFrame(
            {
                "geoid": ["A", "B"],
                "charge_category": ["L1", "L2"],
                "scenario": ["base", "base"],
                "year": [2025, 2025],
                "ports": [2, 3],
                "0": [10, 20],
                "1": [15, 25],
                "2": [15, 25],
                "3": [15, 25],
                "4": [15, 25],
                "5": [15, 25],
                "6": [15, 25],
                "7": [15, 25],
                "8": [15, 25],
                "9": [15, 25],
                "10": [15, 25],
                "11": [15, 25],
                "12": [15, 25],
                "13": [15, 25],
                "14": [15, 25],
                "15": [15, 25],
                "16": [15, 25],
                "17": [15, 25],
                "18": [15, 25],
                "19": [15, 25],
                "20": [15, 25],
                "21": [15, 25],
                "22": [15, 25],
                "23": [15, 25],
            }
        )

        cross_df = pd.DataFrame(
            {
                "destination_bgrp": ["A", "B"],
                "charger_type": ["L1", "L2"],
                "place_name": ["City1", "City2"],
                "place_state": ["CA", "NY"],
                "weight": [0.8, 1.0],
            }
        )

        # Save to temporary files
        self.load_path = os.path.join(self.tempdir.name, "load.parquet")
        self.cross_path = os.path.join(self.tempdir.name, "cross.parquet")
        load_df.to_parquet(self.load_path, index=False)
        cross_df.to_parquet(self.cross_path, index=False)

        # Prepare joined table
        join(
            con=self.con,
            load_table_path=self.load_path,
            cross_table_path=self.cross_path,
            load_identity_col="geoid",
            cross_table_col="destination_bgrp",
            output_table="joined_test",
        )

    def tearDown(self):
        self.con.close()
        self.tempdir.cleanup()

    def test_generate_results_structure(self):
        result = generate_results(
            con=self.con, input_table="joined_test", aggregation_col="place_name"
        ).df()

        self.assertGreaterEqual(len(result), 1)
        self.assertIn("ports", result.columns)
        self.assertIn("place_state", result.columns)
