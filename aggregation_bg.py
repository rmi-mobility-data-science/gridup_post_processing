import os
import pandas as pd
import duckdb
from dask import delayed, compute


# ---------------------------------------------
# Function Definitions
# ---------------------------------------------
def extract_state_cross_table(
    con: duckdb.DuckDBPyConnection,
    cross_table_path: str,
    state: str,
    state_cross_table_path: str,
):
    """Extract state-specific crosswalk records

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        cross_table_path (str): Path to the table with
        crosswalk records
        state (str): State abbreviation
        state_cross_table_path (str): Path to save the
        state-specific cross table
    """
    query = f"""
        COPY (
            SELECT *
            FROM read_parquet('{cross_table_path}') WHERE place_state = '{state}'
            ) 
        TO '{state_cross_table_path}' (FORMAT 'parquet');
        """
    return con.execute(query)


def extract_relevant_bgs(
    con: duckdb.DuckDBPyConnection,
    load_curves_files: str,
    state_cross_table_path: str,
    load_identity_col: str,
    cross_table_col: str,
    relevant_load_bgs_path: str,
):
    """Extract cells from load curves table that are
       part of aggregation boundaries convex hulls.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        load_curves_files (str): Path to load curve parquet files
        state_cross_table_path (str): Path to the table with
        relevant crosswalk records
        load_identity_col (str): Column name (in load_curves_table)
        with BG IDs
        cross_table_col: Column name that links block groups
        with places
        relevant_load_bgs_path (str): Path to save relevant
        block groups
    """
    query = f"""
        COPY (
            SELECT * 
            FROM read_parquet('{load_curves_files}')
            WHERE {load_identity_col} in 
            (SELECT {cross_table_col}
            FROM read_parquet('{state_cross_table_path}'))
            ) 
        TO '{relevant_load_bgs_path}' (FORMAT 'parquet');
        """
    return con.execute(query)


def parallel_join(
    load_table_path: str,
    state_cross_table_path: str,
    load_identity_col: str,
    cross_table_col: str,
) -> pd.DataFrame:
    """Join aggregation boundary table with load curves table for each boundary

    Args:
        load_table_path (str): Path to the load curves table
        state_cross_table_path (str): Path to the table with
        relevant crosswalk records
        cross_table_col: Column name that links block groups
        with places
        load_identity_col: Column name for load identity

    Returns:
        pd.DataFrame: DataFrame containing the joined results
    """

    def process_group(group_val):
        # Create a new DuckDB connection inside each Dask process
        con = duckdb.connect()

        # Load both tables
        con.execute(
            f"CREATE TEMP TABLE load_table AS SELECT * FROM '{load_table_path}'"
        )
        con.execute(
            f"CREATE TEMP TABLE cross_table AS SELECT * FROM '{state_cross_table_path}'"
        )

        # Filter and join
        query = f"""
        SELECT 
            load_table.*,
            cross_table.* 
        FROM load_table
        INNER JOIN (
            SELECT * FROM cross_table WHERE place_name = ?
        ) AS cross_table
        ON load_table.{load_identity_col} = cross_table.{cross_table_col}
            AND load_table.charge_category = cross_table.charger_type
        """
        result = con.execute(query, [group_val]).fetchdf()
        con.close()
        return result

    # This part is outside parallelism — create a temp connection to get unique groups
    con = duckdb.connect()
    con.execute(
        f"""
         CREATE TEMP TABLE cross_table AS 
         SELECT * FROM read_parquet('{state_cross_table_path}')
        """
    )
    unique_agg_boundary_names = (
        con.sql("SELECT DISTINCT place_name FROM cross_table")
        .df()["place_name"]
        .tolist()
    )
    con.close()

    # Run in parallel
    tasks = [delayed(process_group)(g) for g in unique_agg_boundary_names]
    results = compute(*tasks, scheduler="processes", num_workers=4)

    # Combine all results
    return pd.concat(results, ignore_index=True)


def generate_results(con: duckdb.DuckDBPyConnection, input_table: str):
    """Generate results by scenario, name and year

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Table containing joined data

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with aggregated results
    """
    agg_cols = [str(i) for i in range(24)]
    agg_str = ", ".join([f'sum(weight*"{col}") as "{col}"' for col in agg_cols])

    query = f"""
        SELECT
            place_name,
            charge_category,
            scenario,
            year,
            {agg_str},
            CEILING(SUM(ports*weight)) AS ports,
        FROM {input_table}
        GROUP BY place_name, charge_category, scenario, year
        ORDER BY place_name, charge_category, scenario, year 
        """
    return con.sql(query)


# ---------------------------------------------
# Configuration
# ---------------------------------------------
INPUTS_PATH = os.path.join("data", "inputs")
OUTPUTS_PATH = os.path.join("data", "outputs", "census_places")
BOUNDARIES_DIR = os.path.join(INPUTS_PATH, "aggregation_boundaries")
BOUNDARIES_PATH = os.path.join(BOUNDARIES_DIR, "filtered_census_places.parquet")
LOAD_CURVES_FILES = os.path.join(
    INPUTS_PATH, "block_group_load_curves", "evolved_*/*.parquet"
)
CROSS_TABLE_PATH = os.path.join(
    INPUTS_PATH, "crosswalk_files", "blockgroup_place_crosswalk_all_vehicles.parquet"
)
STATE_CROSS_TABLE_PATH = os.path.join(OUTPUTS_PATH, "state_cross_table.parquet")
RELEVANT_LOAD_BGS_PATH = os.path.join(OUTPUTS_PATH, "relevant_load_bgs.parquet")

# Column names
LOAD_IDENTITY_COL = "geoid"
CROSS_TABLE_COL = "destination_bgrp"


# ---------------------------------------------
# Load and prepare boundary data
# ---------------------------------------------
boundary_df = pd.read_parquet(BOUNDARIES_PATH)
if "geometry" in boundary_df.columns:
    boundary_df = boundary_df.drop(columns=["geometry"])
states = boundary_df["STATE"].unique().tolist()

for state in states:
    print(f"Processing state: {state}")
    state_outputs_path = os.path.join(OUTPUTS_PATH, state)
    if not os.path.isdir(state_outputs_path):
        os.makedirs(state_outputs_path, exist_ok=True)
    state_boundary_df = boundary_df[boundary_df["STATE"] == state].copy()

    # ---------------------------------------------
    # Set up DuckDB connection
    # ----------------------------------------------
    sc_con = duckdb.connect()
    sc_con.register("aggregation_boundary_input", state_boundary_df)

    # ---------------------------------------------
    # Preprocessing pipeline
    # ---------------------------------------------
    extract_state_cross_table(
        con=sc_con,
        cross_table_path=CROSS_TABLE_PATH,
        state=state,
        state_cross_table_path=STATE_CROSS_TABLE_PATH,
    )

    # ---------------------------------------------
    # Load curves setup
    # ---------------------------------------------
    # Relevant load cells are within convex hulls of aggregation boundaries
    relevant_load_bgs = extract_relevant_bgs(
        con=sc_con,
        load_curves_files=LOAD_CURVES_FILES,
        state_cross_table_path=STATE_CROSS_TABLE_PATH,
        load_identity_col=LOAD_IDENTITY_COL,
        cross_table_col=CROSS_TABLE_COL,
        relevant_load_bgs_path=RELEVANT_LOAD_BGS_PATH,
    )

    # ---------------------------------------------
    # Parallel join & final aggregation
    # ---------------------------------------------
    # Join is done separately for interior and boundary results
    joined_df = parallel_join(
        load_table_path=RELEVANT_LOAD_BGS_PATH,
        state_cross_table_path=STATE_CROSS_TABLE_PATH,
        load_identity_col=LOAD_IDENTITY_COL,
        cross_table_col=CROSS_TABLE_COL,
    )

    # Now combine both results to get full results
    full_results = generate_results(
        con=sc_con,
        input_table="joined_df",
    )

    # ---------------------------------------------
    # Save results and remove temporary tables
    # ---------------------------------------------
    os.remove(os.path.join(OUTPUTS_PATH, "relevant_load_bgs.parquet"))
    os.remove(os.path.join(OUTPUTS_PATH, "state_cross_table.parquet"))
    full_results.to_parquet(os.path.join(state_outputs_path, "full_results.parquet"))
    sc_con.close()
