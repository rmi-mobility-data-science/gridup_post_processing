import unittest
import duckdb
import pandas as pd
import tempfile
import os
from src.joiner import join


class TestJoiner(unittest.TestCase):
    def setUp(self):
        # Create temporary directory for parquet files
        self.tempdir = tempfile.TemporaryDirectory()
        self.con = duckdb.connect()

        # Create dummy data
        load_df = pd.DataFrame(
            {
                "geoid": ["A", "B"],
                "charge_category": ["L1", "L2"],
                "scenario": ["base", "base"],
                "year": [2025, 2025],
                "ports": [2, 3],
                "0": [10, 20],
                "1": [15, 25],
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

        # Save to temp parquet files
        self.load_path = os.path.join(self.tempdir.name, "load.parquet")
        self.cross_path = os.path.join(self.tempdir.name, "cross.parquet")
        load_df.to_parquet(self.load_path, index=False)
        cross_df.to_parquet(self.cross_path, index=False)

    def tearDown(self):
        self.con.close()
        self.tempdir.cleanup()

    def test_join_outputs_expected_columns(self):
        join(
            con=self.con,
            load_table_path=self.load_path,
            cross_table_path=self.cross_path,
            load_identity_col="geoid",
            cross_table_col="destination_bgrp",
            output_table="joined_test",
        )
        df = self.con.sql("SELECT * FROM joined_test").df()
        self.assertEqual(len(df), 2)
        self.assertIn("place_name", df.columns)
