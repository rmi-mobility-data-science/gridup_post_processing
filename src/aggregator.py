import duckdb


def generate_results(
    con: duckdb.DuckDBPyConnection, input_table: str, aggregation_col: str
) -> duckdb.DuckDBPyRelation:
    """
    Generate aggregated results by scenario, place/utility, and year.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Table name containing the joined data
        aggregation_col (str): Aggregation group column (e.g., 'place_name' or 'utility_name')

    Returns:
        duckdb.DuckDBPyRelation: Aggregated results as a DuckDB relation
    """
    # Columns for 24-hour profile
    agg_cols = [str(i) for i in range(24)]
    agg_exprs = ", ".join([f'sum(weight * "{col}") AS "{col}"' for col in agg_cols])

    # Conditionally include place_state if aggregating by place_name
    state_col = "place_state," if aggregation_col == "place_name" else ""
    state_group = ", place_state" if state_col else ""

    query = f"""
        SELECT
            {aggregation_col},
            {state_col}
            charge_category,
            scenario,
            year,
            {agg_exprs},
            CEILING(SUM(ports * weight)) AS ports
        FROM {input_table}
        GROUP BY {aggregation_col}{state_group}, charge_category, scenario, year
        ORDER BY {aggregation_col}{state_group}, charge_category, scenario, year
    """
    return con.sql(query)
