import os
import pandas as pd
import duckdb
import argparse


# ---------------------------------------------
# Function Definitions
# ---------------------------------------------
def join(
    con: duckdb.DuckDBPyConnection,
    load_table_path: str,
    cross_table_path: str,
    load_identity_col: str,
    cross_table_col: str,
    output_table: str = "joined_table",
) -> pd.DataFrame:
    """Join aggregation boundary table with load curves table for each boundary

    Args:
        con: duckdb.DuckDBPyConnection,
        load_table_path (str): Path to the load curves table
        cross_table_path (str): Path to the table with
        crosswalk records
        cross_table_col: Column name that links block groups
        with places
        load_identity_col: Column name for load identity
        output_table (str): Name of the output table to store results

    Returns:
        pd.DataFrame: DataFrame containing the joined results
    """
    query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
            SELECT 
                load_table.*,
                cross_table.* 
            FROM read_parquet('{load_table_path}') AS load_table
            INNER JOIN (
                SELECT * 
                FROM read_parquet('{cross_table_path}')
            ) AS cross_table
            ON load_table.{load_identity_col} = cross_table.{cross_table_col}
                AND load_table.charge_category = cross_table.charger_type
            """
    return con.execute(query)


def generate_results(
    con: duckdb.DuckDBPyConnection, input_table: str, aggregation_col: str
):
    """Generate results by scenario, name and year

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Table containing joined data
        aggregation_col (str): Column name for aggregation geographies

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with aggregated results
    """
    agg_cols = [str(i) for i in range(24)]
    agg_str = ", ".join([f'sum(weight*"{col}") as "{col}"' for col in agg_cols])

    state_col = "place_state," if aggregation_col == "place_name" else ""
    state_group = ", place_state" if state_col else ""

    query = f"""
        SELECT
            {aggregation_col},
            {state_col}
            charge_category,
            scenario,
            year,
            {agg_str},
            CEILING(SUM(ports*weight)) AS ports,
        FROM {input_table}
        GROUP BY {aggregation_col}{state_group}, charge_category, scenario, year
        ORDER BY {aggregation_col}{state_group}, charge_category, scenario, year 
        """
    return con.sql(query)


# ---------------------------------------------
# Configuration
# ---------------------------------------------
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


if __name__ == "__main__":
    main()
