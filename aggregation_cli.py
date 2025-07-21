import os
import duckdb
import argparse
from src.joiner import join
from src.aggregator import generate_results


def main():
    # Configuration
    parser = argparse.ArgumentParser(description="Run aggregation pipeline.")
    parser.add_argument("--inputs_path", required=True, help="Path to input directory")
    parser.add_argument(
        "--outputs_path", required=True, help="Path to output directory"
    )
    parser.add_argument(
        "--cross_table_path", required=True, help="Path to cross table parquet file"
    )
    parser.add_argument(
        "--aggregation_col", required=True, help="Either place_name or utility_name"
    )
    args = parser.parse_args()

    inputs_path = args.inputs_path
    outputs_path = args.outputs_path
    load_curves_files = os.path.join(
        inputs_path, "block_group_load_curves", "evolved_*/*.parquet"
    )
    cross_table_path = args.cross_table_path
    aggregation_col = args.aggregation_col

    # Column names
    LOAD_IDENTITY_COL = "geoid"
    CROSS_TABLE_COL = "destination_bgrp"

    # ---------------------------------------------
    # Set up DuckDB connection
    # ----------------------------------------------
    sc_con = duckdb.connect()

    # ---------------------------------------------
    # Join & final aggregation
    # ---------------------------------------------
    # Join is done separately for interior and boundary results
    join(
        con=sc_con,
        load_table_path=load_curves_files,
        cross_table_path=cross_table_path,
        load_identity_col=LOAD_IDENTITY_COL,
        cross_table_col=CROSS_TABLE_COL,
        output_table="joined_table",
    )

    # Now combine both results to get full results
    full_results = generate_results(
        con=sc_con,
        input_table="joined_table",
        aggregation_col=aggregation_col,
    )

    # ---------------------------------------------
    # Save results
    # ---------------------------------------------
    full_results.to_parquet(os.path.join(outputs_path, "full_results.parquet"))
    sc_con.close()
    print(f"Aggregation complete. Results saved to {outputs_path}.")


if __name__ == "__main__":
    main()
