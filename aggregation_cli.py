import os
import pandas as pd
import duckdb
from dask import delayed, compute
import geopandas as gpd
from shapely import wkt
import argparse


# ---------------------------------------------
# Function Definitions
# ---------------------------------------------
def create_convex_hull(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    wkt_col: str,
    geom_col: str,
    output_table: str,
):
    """Create convex hull geometries for aggregation
       boundariesfrom WKT geometries

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Boundary table
        wkt_col (str): Column with wkt geometries
        geom_col (str): Column with geometries
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with
        convex hull geometries
    """
    query = f"""
    CREATE OR REPLACE TABLE {output_table} AS
    SELECT *,
           ST_ConvexHull(ST_GeomFromText({wkt_col})) AS {geom_col}
    FROM {input_table};
    """
    con.execute(query)


def convert_geom_to_wkt(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    geom_col: str,
    wkt_col: str,
    output_table: str,
):
    """Convert the geometry column to WKT

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Input table
        geom_col (str): Column with geometries
        wkt_col (str): Column with wkt geometries
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with
        WKT geometries
    """
    query = f"""
    CREATE OR REPLACE TABLE {output_table} AS
    SELECT
        name,
        ST_AsText({geom_col}) AS {wkt_col}
    FROM {input_table};
    """
    con.execute(query)


def convert_polygon_to_h3(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    wkt_col: str,
    h3_col: str,
    output_table: str,
):
    """Convert aggregation polygon geometries to
       H3 indexes at level 8

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Input table
        wkt_col (str): Column with wkt geometries
        h3_col (str): Column with h3 indexes
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with
        h3 indexes
    """
    query = f"""
    CREATE OR REPLACE TABLE {output_table} AS
    SELECT
        *,
        {wkt_col},
        UNNEST(h3_polygon_wkt_to_cells_string({wkt_col}, 8)) AS {h3_col}
    FROM {input_table}
    """
    con.execute(query)


def extract_relevant_cells(
    con: duckdb.DuckDBPyConnection,
    load_curves_files: str,
    convex_hull_table: str,
    load_identity_col: str,
    h3_col: str,
    output_table: str,
    output_path: str,
):
    """Extract cells from load curves table that are
       part of aggregation boundaries convex hulls.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        load_curves_files: Path to load curve parquet files
        convex_hull_table: Table containing H3 cells
        of convex hulls
        load_identity_col: Column name (in load_curves_table)
        with H3 cell IDs
        h3_col: Column name (in convex_hull_table)
        with H3 cell IDs
        output_table (str): Name of the table to be created in DuckDB
        output_path (str): Path to save the output parquet file

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation
        with relevant load cells
    """
    query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT * 
        FROM '{load_curves_files}'
        WHERE {load_identity_col} in (select {h3_col} from {convex_hull_table})
        """
    con.execute(query)
    con.execute(f"COPY {output_table} TO '{output_path}' (FORMAT 'parquet')")


def extract_unique_load_cells(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    cell_col: str,
    output_table: str,
):
    """Extract unique load cells from the query result

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table: Input table with load cells
        cell_col: Column name for load cells
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation
        with unique cells
    """
    query = f"""
    CREATE OR REPLACE TABLE {output_table} AS
    SELECT DISTINCT {cell_col}
    FROM {input_table}
    """
    return con.sql(query)


def convert_h3_to_wkt(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    h3_col: str,
    h3_wkt: str,
    output_table: str,
):
    """Convert H3 indexes to WKT

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table: Input table with H3 indexes
        h3_col: H3 column name
        h3_wkt: Name for the transformed WKT column
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation
        with H3 indexes converted to WKT
    """
    query = f"""
    CREATE OR REPLACE TABLE {output_table} AS
    SELECT
    *,
    h3_cell_to_boundary_wkt({h3_col}) AS {h3_wkt}
    FROM {input_table}
    """
    return con.sql(query)


def reproject_polygons(
    input_polygons: pd.DataFrame, wkt_col: str, original_crs: int, target_crs: int
) -> gpd.GeoDataFrame:
    """Convert from WGS84 to US National Atlas Equal Area

    Args:
        input_polygons (str): Input dataframe with polygon geometries
        wkt_col (str): Column with wkt geometries
        original_crs (int): Original coordinate reference system (CRS) EPSG code
        target_crs (int): Target coordinate reference system (CRS) EPSG code

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with reprojected geometries
        and calculated areas
    """
    input_polygons["geometry"] = input_polygons[wkt_col].apply(wkt.loads)
    output_polygons = gpd.GeoDataFrame(
        input_polygons, geometry="geometry", crs=original_crs
    )
    output_polygons = output_polygons.to_crs(epsg=target_crs)
    return output_polygons


def find_intersection_area(
    boundaries_gdf: gpd.GeoDataFrame, load_cells_gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Find intersection areas between aggregation boundaries
       and load cells

    Args:
        boundaries_gdf: Geodataframe of aggregation boundaries
        load_cells_gdf: Geodataframe of load cells

    Returns:
        pd.DataFrame: DataFrame with intersection areas
        and the geometries removed
    """
    intersection = gpd.overlay(boundaries_gdf, load_cells_gdf, how="intersection")
    intersection["intersection_area"] = intersection.area
    intersection = intersection.drop(columns=["geometry"])
    return intersection


def distribute_weights(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    load_identity_col: str,
    output_path: str,
):
    """Distribute weights focusing on cells that are
       part of more than one aggregation boundary

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): duckdb table with intersection areas
        load_identity_col (str): Column name for load identity
        output_table (str): Name of the table to be created in DuckDB
        output_path (str): Path to save the output parquet file

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with weights calculated
    """
    query = f"""
        COPY (WITH with_ratios AS (
            SELECT *,
                intersection_area / SUM(intersection_area) OVER (PARTITION BY {load_identity_col}) AS ratio
            FROM {input_table}
        )
        -- Aggregate over polygons that are part of a boundary multi-polygon
        SELECT
            {load_identity_col},
            NAME,
            SUM(ratio) AS ratio_sum
        FROM with_ratios
        GROUP BY {load_identity_col}, NAME
        ORDER BY {load_identity_col}, NAME
        ) TO '{output_path}' (FORMAT 'parquet')
    """
    con.execute(query)


def generate_fine_hex_table(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    hex_col: str,
    fine_hex_level: int,
    output_table: str,
):
    """Convert fine level hex cells

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Input table with coarser hex cells
        hex_col (str): Coarser H3 column name
        fine_hex_level (str): Level of finer cells to convert to
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation
        with finer hex cells
    """
    query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        SELECT
            *,
            UNNEST(h3_cell_to_children({hex_col}, {fine_hex_level})) AS fine_hex_id
        FROM {input_table}
        """
    con.execute(query)


def subset_intersection_table(
    con: duckdb.DuckDBPyConnection,
    input_table_path: str,
    subset_col: str,
    subset_value: float,
    subset_type: str,
    output_path: str,
):
    """Subset intersection table based on a specific column value

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table_path (str): Path to input table with intersection areas
        subset_col (str): Column name to filter by
        subset_value (float): Value to filter the column by
        subset_type (str): Type of subset operation ('interior', 'boundary')
        output_path (str): Path to save the output parquet file

    """
    if subset_type == "interior":
        condition = f"{subset_col} >= {subset_value}"
    elif subset_type == "boundary":
        condition = f"{subset_col} < {subset_value}"
    query = f"""
        COPY (
            SELECT * FROM read_parquet('{input_table_path}') WHERE {condition}
        ) TO '{output_path}' (FORMAT 'parquet')
    """
    con.execute(query)


def parallel_join(
    load_table_path: str, intersection_table_path: str, load_identity_col: str
) -> pd.DataFrame:
    """Join aggregation boundary table with load curves table for each boundary

    Args:
        load_table_path (str): Path to the load curves table
        intersection_table_path (str): Path to the intersection table
        load_identity_col: Column name for load identity

    Returns:
        pd.DataFrame: DataFrame containing the joined results
    """

    def process_group(group_val):
        # Create a new DuckDB connection inside each Dask process
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        # Load both tables
        con.execute(
            f"CREATE TEMP TABLE load_table AS SELECT * FROM '{load_table_path}'"
        )
        con.execute(
            f"CREATE TEMP TABLE intersection_table AS SELECT * FROM '{intersection_table_path}'"
        )

        # Filter and join
        query = f"""
        SELECT 
            load_table.*,
            intersection_table.* 
        FROM load_table
        INNER JOIN (
            SELECT * FROM intersection_table WHERE NAME = ?
        ) AS intersection_table
        ON load_table.{load_identity_col} = intersection_table.{load_identity_col}
        """
        result = con.execute(query, [group_val]).fetchdf()
        con.close()
        return result

    # This part is outside parallelism — create a temp connection to get unique groups
    con = duckdb.connect()
    con.execute(
        f"CREATE TEMP TABLE intersection_table AS SELECT * FROM '{intersection_table_path}'"
    )
    unique_agg_boundary_names = (
        con.sql("SELECT DISTINCT NAME FROM intersection_table").df()["NAME"].tolist()
    )
    con.close()

    # Run in parallel
    tasks = [delayed(process_group)(g) for g in unique_agg_boundary_names]
    results = compute(*tasks, scheduler="processes", num_workers=4)

    # Combine all results
    return pd.concat(results, ignore_index=True)


def generate_interior_results(
    con: duckdb.DuckDBPyConnection, input_table: str, output_table: str
):
    """Apply weight ratios and aggregate to generate results
       by scenario, name, year, hour and charge category for
       internal hex cells

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Table containing joined data
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with detailed results.
        Detailed because charge category is also included
    """
    agg_cols = [str(i) for i in range(24)]
    weighted_sums = ",\n    ".join(
        [f'SUM("{col}" * ratio_sum) AS "{col}"' for col in agg_cols]
    )
    query = f"""
    CREATE OR REPLACE TABLE {output_table} AS
    SELECT
        NAME,
        charge_category,
        scenario,
        year,
        {weighted_sums},
        CEILING(SUM(ports * ratio_sum)) AS ports
    FROM {input_table}
    GROUP BY NAME, charge_category, scenario, year
    ORDER BY NAME, charge_category, scenario, year 
    """
    return con.execute(query)


def generate_boundary_results(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    cross_table_path: str,
    output_table: str,
):
    """Generate results by scenario, name and year

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Table containing joined data
        cross_table_path (str): Path to the cross table
        output_table (str): Name of the table to be created in DuckDB

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with aggregated results
    """
    agg_cols = [str(i) for i in range(24)]
    weighted_sums = ",\n    ".join(
        [f'SUM("{col}" * orig_to_fine_frac) AS "{col}"' for col in agg_cols]
    )

    query = f"""
        CREATE OR REPLACE TABLE {output_table} AS
        WITH boundary_joint AS (SELECT
            *,
        FROM {input_table}
        INNER JOIN read_parquet('{cross_table_path}') AS cross_table
        USING (geoid, charge_category, fine_hex_id))
        SELECT
            NAME,
            charge_category,
            scenario,
            year,
            {weighted_sums},
            CEILING(SUM(ports * orig_to_fine_frac)) AS ports
        FROM boundary_joint
        GROUP BY NAME, charge_category, scenario, year
        ORDER BY NAME, charge_category, scenario, year 
        """
    return con.sql(query)


def generate_full_results(
    con: duckdb.DuckDBPyConnection, interior_table: str, boundary_table: str
):
    """Generate results by scenario, name and year

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        interior_table (str): Table containing interior results
        boundary_table (str): Table containing boundary results

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with aggregated results
    """
    agg_cols = [str(i) for i in range(24)]
    hourly_sums = ",\n    ".join([f'SUM("{col}") AS "{col}"' for col in agg_cols])

    query = f"""
        WITH full_table AS (
        SELECT *,
        FROM {interior_table}
        UNION ALL
        SELECT *
        FROM {boundary_table}
        )
        SELECT
            NAME,
            charge_category,
            scenario,
            year,
            {hourly_sums},
            SUM(ports) AS ports
        FROM full_table
        GROUP BY NAME, charge_category, scenario, year
        ORDER BY NAME, charge_category, scenario, year 
        """
    return con.sql(query)


# ---------------------------------------------
# Configuration
# ---------------------------------------------
def main():
    # Configuration
    parser = argparse.ArgumentParser(description="Run aggregation pipeline by state.")
    parser.add_argument(
        "--state", required=True, help="Two-letter state code (e.g., NY, CA)"
    )
    parser.add_argument("--inputs_path", required=True, help="Path to input directory")
    parser.add_argument(
        "--outputs_path", required=True, help="Path to output directory"
    )
    parser.add_argument(
        "--boundaries_path",
        required=True,
        help="Path to polygon boundaries parquet file",
    )
    parser.add_argument(
        "--cross_table_path", required=True, help="Path to cross table parquet file"
    )
    args = parser.parse_args()

    state = args.state.upper()
    inputs_path = args.inputs_path
    outputs_path = args.outputs_path
    boundaries_path = args.boundaries_path
    load_curves_files = os.path.join(inputs_path, "evolved_*/*.parquet")
    cross_table_path = args.cross_table_path

    # Column names
    BOUNDARY_WKT_COL = "boundary_wkt"
    BOUNDARY_GEOM_COL = "boundary_geom"
    H3_COL = "h3_index"
    H3_WKT = "h3_wkt"
    LOAD_IDENTITY_COL = "geoid"
    SUBSET_COL = "ratio_sum"
    SUBSET_VALUE = 0.999

    ORIGINAL_CRS = 4326  # WGS84
    FINE_HEX_LEVEL = 9

    # ---------------------------------------------
    # Set up DuckDB connection
    # ----------------------------------------------
    sc_con = duckdb.connect()
    sc_con.execute("INSTALL spatial; LOAD spatial;")
    sc_con.execute("INSTALL h3 FROM community; LOAD h3;")

    # ---------------------------------------------
    # Load and prepare boundary data
    # ---------------------------------------------
    boundary_df = pd.read_parquet(boundaries_path)
    state_boundary_df = boundary_df[boundary_df["STATE"] == state].copy()
    state_outputs_path = os.path.join(outputs_path, state)
    if not os.path.isdir(state_outputs_path):
        os.makedirs(state_outputs_path, exist_ok=True)

    sc_con.register("aggregation_boundary_input", state_boundary_df)

    # ---------------------------------------------
    # Preprocessing pipeline
    # ---------------------------------------------
    create_convex_hull(
        con=sc_con,
        input_table="aggregation_boundary_input",
        wkt_col=BOUNDARY_WKT_COL,
        geom_col=BOUNDARY_GEOM_COL,
        output_table="convex_table",
    )

    convert_geom_to_wkt(
        con=sc_con,
        input_table="convex_table",
        geom_col=BOUNDARY_GEOM_COL,
        wkt_col=BOUNDARY_WKT_COL,
        output_table="wkt_table",
    )

    convert_polygon_to_h3(
        con=sc_con,
        input_table="wkt_table",
        wkt_col=BOUNDARY_WKT_COL,
        h3_col=H3_COL,
        output_table="convex_hull_h3",
    )

    # ---------------------------------------------
    # Load curves setup
    # ---------------------------------------------
    # Relevant load cells are within convex hulls of aggregation boundaries
    extract_relevant_cells(
        con=sc_con,
        load_curves_files=load_curves_files,
        convex_hull_table="convex_hull_h3",
        load_identity_col=LOAD_IDENTITY_COL,
        h3_col=H3_COL,
        output_table="relevant_load_cells",
        output_path=os.path.join(outputs_path, "relevant_load_cells.parquet"),
    )

    # ---------------------------------------------
    # Prepare intersection table
    # ---------------------------------------------
    if state not in ["HI", "AK"]:
        target_crs = 5070  # Albers Equal Area Projection
    elif state == "HI":
        target_crs = 3563  # Hawaii Albers Equal Area Conic
    elif state == "AK":
        target_crs = 3338  # Alaska Albers Equal Area Conic
    boundary_transformed = reproject_polygons(
        input_polygons=state_boundary_df,
        wkt_col=BOUNDARY_WKT_COL,
        original_crs=ORIGINAL_CRS,
        target_crs=target_crs,
    )

    extract_unique_load_cells(
        con=sc_con,
        input_table="relevant_load_cells",
        cell_col=LOAD_IDENTITY_COL,
        output_table="unique_load_cells",
    )
    convert_h3_to_wkt(
        con=sc_con,
        input_table="unique_load_cells",
        h3_col=LOAD_IDENTITY_COL,
        h3_wkt=H3_WKT,
        output_table="unique_load_cells",
    )
    unique_load_cells_df = sc_con.sql("SELECT * FROM unique_load_cells").fetchdf()
    unique_load_cells_transformed = reproject_polygons(
        input_polygons=unique_load_cells_df,
        wkt_col=H3_WKT,
        original_crs=ORIGINAL_CRS,
        target_crs=target_crs,
    )

    intersection_df = find_intersection_area(
        boundaries_gdf=boundary_transformed,
        load_cells_gdf=unique_load_cells_transformed,
    )
    sc_con.register("intersection_table", intersection_df)
    distribute_weights(
        con=sc_con,
        input_table="intersection_table",
        load_identity_col=LOAD_IDENTITY_COL,
        output_path=os.path.join(outputs_path, "intersection_table.parquet"),
    )

    # ---------------------------------------------
    # Subset intersection tables
    # ---------------------------------------------
    # Extract interior and boundary intersection tables
    subset_intersection_table(
        con=sc_con,
        input_table_path=os.path.join(outputs_path, "intersection_table.parquet"),
        subset_col=SUBSET_COL,
        subset_value=SUBSET_VALUE,
        subset_type="interior",
        output_path=os.path.join(outputs_path, "intersection_table_interior.parquet"),
    )
    subset_intersection_table(
        con=sc_con,
        input_table_path=os.path.join(outputs_path, "intersection_table.parquet"),
        subset_col=SUBSET_COL,
        subset_value=SUBSET_VALUE,
        subset_type="boundary",
        output_path=os.path.join(outputs_path, "intersection_table_boundary.parquet"),
    )

    # ---------------------------------------------
    # Parallel joins
    # ---------------------------------------------
    interior_joined_df = parallel_join(
        load_table_path=os.path.join(outputs_path, "relevant_load_cells.parquet"),
        intersection_table_path=os.path.join(
            outputs_path, "intersection_table_interior.parquet"
        ),
        load_identity_col=LOAD_IDENTITY_COL,
    )
    sc_con.register("interior_joined_table", interior_joined_df)

    boundary_joined_df = parallel_join(
        load_table_path=os.path.join(outputs_path, "relevant_load_cells.parquet"),
        intersection_table_path=os.path.join(
            outputs_path, "intersection_table_boundary.parquet"
        ),
        load_identity_col=LOAD_IDENTITY_COL,
    )
    sc_con.register("boundary_joined_table", boundary_joined_df)

    # ---------------------------------------------
    # Save results and remove temporary tables
    # ---------------------------------------------
    # First generate results for interior cells
    generate_interior_results(
        con=sc_con,
        input_table="interior_joined_table",
        output_table="full_interior_results",
    )

    # Generate results for boundary cells
    generate_fine_hex_table(
        con=sc_con,
        input_table="boundary_joined_table",
        hex_col=LOAD_IDENTITY_COL,
        fine_hex_level=FINE_HEX_LEVEL,
        output_table="fine_hex_table",
    )
    generate_boundary_results(
        con=sc_con,
        input_table="fine_hex_table",
        cross_table_path=cross_table_path,
        output_table="full_boundary_results",
    )

    # Now combine both results to get full results
    full_results = generate_full_results(
        con=sc_con,
        interior_table="full_interior_results",
        boundary_table="full_boundary_results",
    )

    # ---------------------------------------------
    # Save results and remove temporary tables
    # ---------------------------------------------
    os.remove(os.path.join(outputs_path, "intersection_table.parquet"))
    os.remove(os.path.join(outputs_path, "intersection_table_boundary.parquet"))
    os.remove(os.path.join(outputs_path, "intersection_table_interior.parquet"))
    os.remove(os.path.join(outputs_path, "relevant_load_cells.parquet"))
    full_results.to_parquet(os.path.join(state_outputs_path, "full_results.parquet"))
    sc_con.close()


if __name__ == "__main__":
    main()
