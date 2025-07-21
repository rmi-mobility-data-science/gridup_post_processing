import duckdb
import pandas as pd


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
